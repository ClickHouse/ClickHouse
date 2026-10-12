create database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('/etc'); -- { serverError BAD_ARGUMENTS }
create database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('../../../../../../../../etc'); -- { serverError BAD_ARGUMENTS }
-- A path inside user_files that does not exist. A definition the user supplies now is rejected whether the
-- statement is CREATE or ATTACH; only the replay of a definition already stored on this server may load.
attach database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('no_such_dir_in_user_files'); -- { serverError BAD_ARGUMENTS }
-- A wrapper that runs its children as internal queries must not turn a user-supplied definition into a
-- metadata replay. The sibling has an empty pipeline and cannot fail, so the assertion is order-independent.
attach database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('no_such_dir_in_user_files')
parallel with drop table if exists no_such_table_02921; -- { serverError BAD_ARGUMENTS }
-- Same wrapper, but now the name already has a stored definition, which a detach leaves behind. What the
-- server stored is that definition, not the one below, so the path still has to exist.
create database {CLICKHOUSE_DATABASE_1:Identifier};
detach database {CLICKHOUSE_DATABASE_1:Identifier};
attach database {CLICKHOUSE_DATABASE_1:Identifier} ENGINE=Filesystem('no_such_dir_in_user_files')
parallel with drop table if exists no_such_table_02921; -- { serverError BAD_ARGUMENTS }
-- Re-attach before dropping: a detached database keeps its metadata file, which every later restart replays.
attach database {CLICKHOUSE_DATABASE_1:Identifier};
drop database {CLICKHOUSE_DATABASE_1:Identifier};
