-- Tags: zookeeper

set allow_unrestricted_reads_from_keeper = 'true';

drop table if exists test_zkinsert;

create table test_zkinsert (
	name String,
	path String,
	value String
) ENGINE Memory;

-- test recursive create and big transaction
insert into test_zkinsert (name, path, value) values ('c', '/1-insert-testc/c/c/c/c/c/c', 11), ('e', '/1-insert-testc/c/c/d', 10), ('c', '/1-insert-testc/c/c/c/c/c/c/c', 10), ('c', '/1-insert-testc/c/c/c/c/c/c', 9), ('f', '/1-insert-testc/c/c/d', 11), ('g', '/1-insert-testc/c/c/d', 12), ('g', '/1-insert-testc/c/c/e', 13), ('g', '/1-insert-testc/c/c/f', 14), ('g', '/1-insert-testc/c/c/kk', 14);
-- insert same value, suppose to have no side effects
insert into system.zookeeper (name, path, value) SELECT name, '/' || currentDatabase() || path, value from test_zkinsert;

SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name) WHERE path LIKE '/' || currentDatabase() || '/1-insert-test%';

SELECT '-------------------------';

-- test inserting into root path
insert into test_zkinsert (name, path, value) values ('testc', '/2-insert-testx', 'x');
insert into test_zkinsert (name, path, value) values ('testz', '/2-insert-testx', 'y');
insert into test_zkinsert (name, path, value) values ('testc', '/2-insert-testz//c/cd/dd//', 'y');
insert into test_zkinsert (name, path) values ('testc', '/2-insert-testz//c/cd/');
insert into test_zkinsert (name, value, path) values ('testb', 'z', '/2-insert-testx');

insert into system.zookeeper (name, path, value) SELECT name, '/' || currentDatabase() || path, value from test_zkinsert;

SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name) WHERE path LIKE '/' || currentDatabase() || '/2-insert-test%';

-- test exceptions 
insert into system.zookeeper (name, value) values ('abc', 'y'); -- { serverError 36 }
insert into system.zookeeper (path, value) values ('a/b/c', 'y'); -- { serverError 36 }
insert into system.zookeeper (name, version) values ('abc', 111); -- { serverError 44 }
insert into system.zookeeper (name, versionxyz) values ('abc', 111); -- { serverError 16 }
insert into system.zookeeper (name, path, value) values ('a/b/c', '/', 'y'); -- { serverError 36 }
insert into system.zookeeper (name, path, value) values ('/', '/a/b/c', 'z'); -- { serverError 36 }
insert into system.zookeeper (name, path, value) values ('', '/', 'y'); -- { serverError 36 }
insert into system.zookeeper (name, path, value) values ('abc', '/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc/abc', 'y'); -- { serverError 36 }

drop table if exists test_zkinsert;
