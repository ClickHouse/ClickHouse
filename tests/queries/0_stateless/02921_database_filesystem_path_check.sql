create database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('/etc'); -- { serverError BAD_ARGUMENTS }
create database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('../../../../../../../../etc'); -- { serverError BAD_ARGUMENTS }
-- A path inside user_files that does not exist. A definition the user supplies now is rejected whether the
-- statement is CREATE or ATTACH; only the replay of a definition already stored on this server may load.
attach database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('no_such_dir_in_user_files'); -- { serverError BAD_ARGUMENTS }
