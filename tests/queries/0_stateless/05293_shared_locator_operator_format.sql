SELECT position(formatQuerySingleLine('BACKUP TABLE t TO S3(''url'', use_environment_credentials = 1)'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('BACKUP TABLE t TO S3(''url'') SETTINGS base_backup = S3(''base'', use_environment_credentials = 1)'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('BACKUP FROM SNAPSHOT S3(''snap'', use_environment_credentials = 1) TO S3(''url'')'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('SNAPSHOT ALL TO S3(''url'', use_environment_credentials = 1)'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('ALTER TABLE t UNLOCK SNAPSHOT ''snap'' FROM S3(''url'', use_environment_credentials = 1)'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('SYSTEM UNLOCK SNAPSHOT ''snap'' FROM S3(''url'', use_environment_credentials = 1)'), 'equals(use_environment_credentials, 1)') > 0;
SELECT position(formatQuerySingleLine('CREATE DATABASE db_05293 ENGINE = Backup('''', S3(''url'', use_environment_credentials = 1))'), 'use_environment_credentials = 1') > 0;
SELECT position(formatQuerySingleLine('CREATE DATABASE db_05293 ENGINE = Backup('''', S3(''url'', use_environment_credentials = 1))'), 'equals(use_environment_credentials, 1)') = 0;
