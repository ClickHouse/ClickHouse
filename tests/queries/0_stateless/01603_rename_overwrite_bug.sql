DROP database IF EXISTS test_1603_rename_bug_ordinary;
create database test_1603_rename_bug_ordinary engine=Ordinary;
create table test_1603_rename_bug_ordinary.foo engine=Memory as select * from numbers(100);
create table test_1603_rename_bug_ordinary.bar engine=Log as select * from numbers(200);
detach table test_1603_rename_bug_ordinary.foo;
rename table test_1603_rename_bug_ordinary.bar to test_1603_rename_bug_ordinary.foo; -- { serverError 57 }
attach table test_1603_rename_bug_ordinary.foo;
SELECT count() from test_1603_rename_bug_ordinary.foo;
SELECT count() from test_1603_rename_bug_ordinary.bar;
DROP DATABASE test_1603_rename_bug_ordinary;

-- was not broken, adding just in case.
DROP database IF EXISTS test_1603_rename_bug_atomic;
create database test_1603_rename_bug_atomic engine=Atomic;
create table test_1603_rename_bug_atomic.foo engine=Memory as select * from numbers(100);
create table test_1603_rename_bug_atomic.bar engine=Log as select * from numbers(200);
detach table test_1603_rename_bug_atomic.foo;
rename table test_1603_rename_bug_atomic.bar to test_1603_rename_bug_atomic.foo; -- { serverError 57 }
attach table test_1603_rename_bug_atomic.foo;
SELECT count() from test_1603_rename_bug_atomic.foo;
SELECT count() from test_1603_rename_bug_atomic.bar;
DROP DATABASE test_1603_rename_bug_atomic;
