ATTACH DATABASE ice
ENGINE = DataLakeCatalog('https://iceberg-catalog.aws-us-west-2.dev.altinity.cloud')
SETTINGS catalog_type = 'rest', auth_header = 'Authorization: Bearer etxkehqze7esafs9qw07lcrww5nd0iqo', warehouse = 's3://aws-st-2-fs5vug37-iceberg'
