-- Tags: no-fasttest

DROP USER IF EXISTS test_user_01075, test_user_01075_x, test_user_01075_x@localhost, test_user_01075_x@'192.168.23.15';

CREATE USER test_user_01075;
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST ANY;
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST NONE;
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST LOCAL;
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST IP '192.168.23.15';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST IP '2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 ADD HOST IP '127.0.0.1';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 DROP HOST IP '2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 DROP HOST NAME 'localhost';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST LIKE '@.somesite.com';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST REGEXP '.*\.anothersite\.com';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST REGEXP '.*\.anothersite\.com', '.*\.anothersite\.org';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST REGEXP '.*\.anothersite2\.com', REGEXP '.*\.anothersite2\.org';
SHOW CREATE USER test_user_01075;

ALTER USER test_user_01075 HOST REGEXP '.*\.anothersite3\.com' HOST REGEXP '.*\.anothersite3\.org';
SHOW CREATE USER test_user_01075;

DROP USER test_user_01075;

CREATE USER test_user_01075_x@localhost;
SHOW CREATE USER test_user_01075_x@localhost;

ALTER USER test_user_01075_x@localhost RENAME TO test_user_01075_x@'%';
SHOW CREATE USER test_user_01075_x;

ALTER USER test_user_01075_x RENAME TO test_user_01075_x@'192.168.23.15';
SHOW CREATE USER 'test_user_01075_x@192.168.23.15';

DROP USER 'test_user_01075_x@192.168.23.15';
