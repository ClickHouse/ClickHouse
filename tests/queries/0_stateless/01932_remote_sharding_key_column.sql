-- Tags: shard

-- regression test for the following query:
--
--     select * from remote('127.1', system.one, dummy)
--
-- that produce the following error before:
--
--     Unknown column: dummy, there are only columns .
--
-- NOTE: that wrapping column into any function works before.
select * from remote('127.1', system.one, dummy) format Null;
select * from remote('127.1', system.one, identity(dummy)) format Null;
select * from remote('127.1', view(select * from system.one), identity(dummy)) format Null;
select * from remote('127.{1,2}', view(select * from system.one), identity(dummy)) format Null;
select * from remote('127.1', view(select * from system.one), dummy) format Null;
select * from remote('127.{1,2}', view(select * from system.one), dummy) format Null;
