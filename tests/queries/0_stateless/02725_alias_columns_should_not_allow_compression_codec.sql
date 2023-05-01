drop table if exists alias_column_should_not_allow_compression;
create table if not exists alias_column_should_not_allow_compression ( user_id UUID, user_id_hashed ALIAS (cityHash64(user_id))) engine=MergeTree() order by tuple();
alter table alias_column_should_not_allow_compression modify column user_id codec(LZ4HC(1));
alter table alias_column_should_not_allow_compression modify column user_id_hashed codec(LZ4HC(1)); -- { serverError BAD_ARGUMENTS }
