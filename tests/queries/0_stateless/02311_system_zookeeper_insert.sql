-- Tags: zookeeper

set allow_unrestricted_reads_from_keeper = 'true';

-- test recursive create and big transaction
insert into system.zookeeper (name, path, value) values ('c', '/testc/c/c/c/c/c/c', 11), ('e', '/testc/c/c/d', 10), ('c', '/testc/c/c/c/c/c/c/c', 10), ('c', '/testc/c/c/c/c/c/c', 9), ('f', '/testc/c/c/d', 11), ('g', '/testc/c/c/d', 12), ('g', '/testc/c/c/e', 13), ('g', '/testc/c/c/f', 14), ('g', '/testc/c/c/kk', 14);
-- insert same value, suppose to have no side effects
insert into system.zookeeper (name, path, value) values ('c', '/testc/c/c/c/c/c/c', 11), ('e', '/testc/c/c/d', 10), ('c', '/testc/c/c/c/c/c/c/c', 10), ('c', '/testc/c/c/c/c/c/c', 9), ('f', '/testc/c/c/d', 11), ('g', '/testc/c/c/d', 12), ('g', '/testc/c/c/e', 13), ('g', '/testc/c/c/f', 14), ('g', '/testc/c/c/kk', 15);
--allow_unrestricted_reads_from_keeper=1 
SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name) WHERE path LIKE '/test%';

SELECT '-------------------------';

-- test inserting into root path
insert into system.zookeeper (name, path, value) values ('testc', '/', 'x');
insert into system.zookeeper (name, path, value) values ('testz', '/', 'y');
insert into system.zookeeper (name, path, value) values ('testc', 'testz//c/cd/dd//', 'y');
insert into system.zookeeper (name, value, path) values ('testz', 'z', '/');

SELECT * FROM (SELECT path, name, value FROM system.zookeeper ORDER BY path, name);

-- test exceptions 
insert into system.zookeeper (name, path, value) values ('a/b/c', '/', 'y'); -- { serverError 36 }
insert into system.zookeeper (name, path, value) values ('/', '/a/b/c', 'z'); -- { serverError 36 }

set allow_unrestricted_reads_from_keeper = 'false';
