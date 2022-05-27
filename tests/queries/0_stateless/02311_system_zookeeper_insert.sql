-- Tags: zookeeper

set allow_unrestricted_reads_from_keeper = 'true';

-- test recursive create and big transaction
insert into system.zookeeper (name, path, value) values ('c', '/1-insert-testc/c/c/c/c/c/c', 11), ('e', '/1-insert-testc/c/c/d', 10), ('c', '/1-insert-testc/c/c/c/c/c/c/c', 10), ('c', '/1-insert-testc/c/c/c/c/c/c', 9), ('f', '/1-insert-testc/c/c/d', 11), ('g', '/1-insert-testc/c/c/d', 12), ('g', '/1-insert-testc/c/c/e', 13), ('g', '/1-insert-testc/c/c/f', 14), ('g', '/1-insert-testc/c/c/kk', 14);
-- insert same value, suppose to have no side effects
insert into system.zookeeper (name, path, value) values ('c', '/1-insert-testc/c/c/c/c/c/c', 11), ('e', '/1-insert-testc/c/c/d', 10), ('c', '/1-insert-testc/c/c/c/c/c/c/c', 10), ('c', '/1-insert-testc/c/c/c/c/c/c', 9), ('f', '/1-insert-testc/c/c/d', 11), ('g', '/1-insert-testc/c/c/d', 12), ('g', '/1-insert-testc/c/c/e', 13), ('g', '/1-insert-testc/c/c/f', 14), ('g', '/1-insert-testc/c/c/kk', 15);

SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name) WHERE path LIKE '/1-insert-test%';

SELECT '-------------------------';

-- test inserting into root path
insert into system.zookeeper (name, path, value) values ('testc', '/2-insert-testx', 'x');
insert into system.zookeeper (name, path, value) values ('testz', '/2-insert-testx', 'y');
insert into system.zookeeper (name, path, value) values ('testc', '2-insert-testz//c/cd/dd//', 'y');
insert into system.zookeeper (name, value, path) values ('testz', 'z', '/2-insert-testx');

SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name) WHERE path LIKE '/2-insert-test%';

-- test exceptions 
insert into system.zookeeper (name, path, value) values ('a/b/c', '/', 'y'); -- { serverError 36 }
insert into system.zookeeper (name, path, value) values ('/', '/a/b/c', 'z'); -- { serverError 36 }

set allow_unrestricted_reads_from_keeper = 'false';
