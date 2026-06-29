SELECT formatQuery('BACKUP FROM SNAPSHOT S3(\'http://localhost:9000/bucket/snapshot/\') TO S3(\'http://localhost:9000/bucket/backup/\')');
SELECT formatQuery('BACKUP FROM SNAPSHOT S3(\'http://localhost:9000/bucket/snapshot/\') TO S3(\'http://localhost:9000/bucket/backup/\') SETTINGS id = \'abc\'');
SELECT formatQuery('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')');
