-- Tags: shard


select * from remote('127.0.0.1', sys); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select * from remote('127.0.0.1', system); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select * from remote('127.0.0.1', system.o); -- { serverError UNKNOWN_TABLE }
select * from remote('127.0.0.1', system.one, default); -- { serverError UNKNOWN_IDENTIFIER }
-- A bare identifier user name is accepted only when a string-literal password follows it,
-- so this still fails because `key1` is not a password.
select * from remote('127.0.0.1', system.one, default, key1); -- { serverError BAD_ARGUMENTS }
select * from remote('127.0.0.1', system.one, 'default', '', key1); -- { serverError UNKNOWN_IDENTIFIER }
-- With a literal password, the identifier is the user name and `key1` is the sharding key.
select * from remote('127.0.0.1', system.one, default, '', key1); -- { serverError UNKNOWN_IDENTIFIER }
select * from remote('127.0.0.1', system.one, 'default', pwd, key1); -- { serverError BAD_ARGUMENTS }
select * from remote('127.0.0.1', system.one, 'default', '', key1, key2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select * from remote('127.0.0.1', system, one, 'default', '', key1, key2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
