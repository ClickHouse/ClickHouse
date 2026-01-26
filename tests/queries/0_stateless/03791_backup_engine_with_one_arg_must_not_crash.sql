-- Bug https://github.com/ClickHouse/ClickHouse/issues/93551
CREATE DATABASE d1 ENGINE = Backup(-5036955478727481395::Int64); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
