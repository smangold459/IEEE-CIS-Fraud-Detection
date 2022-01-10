import pandas as pd

def standardize_col_names(df):
    df.columns = (df.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('(', '')
                .str.replace(')', '')
                .str.replace('/','')
                .str.replace('\\',''))
    return df

def null_counts(df):
    null_df = pd.DataFrame(df.isnull().sum(), columns=['null_count'])
    null_df['null_fraction'] = null_df['null_count'] / df.shape[0]
    null_df = null_df.sort_values('null_count',ascending=False)
    return null_df

def prepare_data(final_model=False):
    
    if final_model == False:
        
        transaction = pd.read_csv('../data/raw/train_transaction.csv')
        identity = pd.read_csv('../data/raw/train_identity.csv')
        
        data = transaction.join(identity.pivot('TransactionID', 'variable', 'value'))
        
        data = standardize_col_names(data)
        
        data.set_index('transactionid', inplace=True)
        
        null_report = null_counts(data)
        to_drop = null_report.null_fraction.where((null_report.null_fraction==1)).dropna().index
        data.drop(to_drop, axis=1)
        
        data = data.loc[:,~data.columns.duplicated()]
    
        return data
        
    else:
        
        transaction_train = pd.read_csv('../data/raw/train_transaction.csv')
        identity_train = pd.read_csv('../data/raw/train_identity.csv')
                
        train = transaction_train.join(identity_train.pivot('TransactionID', 'variable', 'value'))
        
        train = standardize_col_names(train)
        
        train.set_index('transactionid', inplace=True)
        
        null_report = null_counts(train)
        to_drop = null_report.null_fraction.where((null_report.null_fraction >=1)).dropna().index
        train.drop(to_drop, axis=1)
        
        train = train.loc[:,~train.columns.duplicated()]
        
        transaction_test = pd.read_csv('../data/raw/test_transaction.csv')
        identity_test = pd.read_csv('..data/raw/test_identity.csv')
        
        test = transaction_test.join(identity_test.pivot('TransactionID', 'variable', 'values'))
        
        test = standardize_col_names(test)
        
        test.set_index('transactionid', inplace=True)
        
        null_report = null_counts(test)
        to_drop = null_report.null_fraction.where((null_report.null_fraction==1)).dropna().index
        test.drtop(to_drop, axis=1)
        
        test = test.loc[:,~test.columns.duplicated()]
    
        return train, test