-- Tags: no-parallel

SELECT 'database atomic tests';

DROP DATABASE IF EXISTS test1601_detach_permanently_atomic;
CREATE DATABASE test1601_detach_permanently_atomic Engine=Atomic;

create table test1601_detach_permanently_atomic.test_name_reuse (number UInt64) engine=MergeTree order by tuple();

INSERT INTO test1601_detach_permanently_atomic.test_name_reuse SELECT * FROM numbers(100);

DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY;

SELECT 'can not create table with same name as detached permanently';
create table test1601_detach_permanently_atomic.test_name_reuse (number UInt64) engine=MergeTree order by tuple(); -- { serverError 57 }

SELECT 'can not detach twice';
DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY; -- { serverError 60 }
DETACH table test1601_detach_permanently_atomic.test_name_reuse; -- { serverError 60 }

SELECT 'can not drop detached';
drop table test1601_detach_permanently_atomic.test_name_reuse; -- { serverError 60 }

create table test1601_detach_permanently_atomic.test_name_rename_attempt (number UInt64) engine=MergeTree order by tuple();

SELECT 'can not replace with the other table';
RENAME TABLE test1601_detach_permanently_atomic.test_name_rename_attempt TO test1601_detach_permanently_atomic.test_name_reuse; -- { serverError 57 }
EXCHANGE TABLES test1601_detach_permanently_atomic.test_name_rename_attempt AND test1601_detach_permanently_atomic.test_name_reuse; -- { serverError 60 }

SELECT 'can still show the create statement';
SHOW CREATE TABLE test1601_detach_permanently_atomic.test_name_reuse FORMAT Vertical;

SELECT 'can not attach with bad uuid';
ATTACH TABLE test1601_detach_permanently_atomic.test_name_reuse UUID '00000000-0000-0000-0000-000000000001'　(`number` UInt64　)　ENGINE = MergeTree　ORDER BY tuple()　SETTINGS index_granularity = 8192 ;  -- { serverError 57 }

SELECT 'can attach with short syntax';
ATTACH TABLE test1601_detach_permanently_atomic.test_name_reuse;

SELECT count() FROM test1601_detach_permanently_atomic.test_name_reuse;

DETACH table test1601_detach_permanently_atomic.test_name_reuse;

SELECT 'can not detach permanently the table which is already detached (temporary)';
DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY; -- { serverError 60 }

DETACH DATABASE test1601_detach_permanently_atomic;
ATTACH DATABASE test1601_detach_permanently_atomic;

SELECT count() FROM test1601_detach_permanently_atomic.test_name_reuse;

SELECT 'After database reattachement the table is back (it was detached temporary)';
SELECT 'And we can detach it permanently';
DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY;

DETACH DATABASE test1601_detach_permanently_atomic;
ATTACH DATABASE test1601_detach_permanently_atomic;

SELECT 'After database reattachement the table is still absent (it was detached permamently)';
SELECT 'And we can not detach it permanently';
DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY; -- { serverError 60 }

SELECT 'But we can attach it back';
ATTACH TABLE test1601_detach_permanently_atomic.test_name_reuse;

SELECT 'And detach permanently again to check how database drop will behave';
DETACH table test1601_detach_permanently_atomic.test_name_reuse PERMANENTLY;

SELECT 'DROP database';
DROP DATABASE test1601_detach_permanently_atomic SYNC;

SELECT '-----------------------';
SELECT 'database ordinary tests';

DROP DATABASE IF EXISTS test1601_detach_permanently_ordinary;
set allow_deprecated_database_ordinary=1;
CREATE DATABASE test1601_detach_permanently_ordinary Engine=Ordinary;

create table test1601_detach_permanently_ordinary.test_name_reuse (number UInt64) engine=MergeTree order by tuple();

INSERT INTO test1601_detach_permanently_ordinary.test_name_reuse SELECT * FROM numbers(100);

DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY;

SELECT 'can not create table with same name as detached permanently';
create table test1601_detach_permanently_ordinary.test_name_reuse (number UInt64) engine=MergeTree order by tuple(); -- { serverError 57 }

SELECT 'can not detach twice';
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY; -- { serverError 60 }
DETACH table test1601_detach_permanently_ordinary.test_name_reuse; -- { serverError 60 }

SELECT 'can not drop detached';
drop table test1601_detach_permanently_ordinary.test_name_reuse; -- { serverError 60 }

create table test1601_detach_permanently_ordinary.test_name_rename_attempt (number UInt64) engine=MergeTree order by tuple();

SELECT 'can not replace with the other table';
RENAME TABLE test1601_detach_permanently_ordinary.test_name_rename_attempt TO test1601_detach_permanently_ordinary.test_name_reuse; -- { serverError 57 }

SELECT 'can still show the create statement';
SHOW CREATE TABLE test1601_detach_permanently_ordinary.test_name_reuse FORMAT Vertical;

SELECT 'can attach with full syntax';
ATTACH TABLE test1601_detach_permanently_ordinary.test_name_reuse (`number` UInt64　)　ENGINE = MergeTree　ORDER BY tuple()　SETTINGS index_granularity = 8192;
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY;

SELECT 'can attach with short syntax';
ATTACH TABLE test1601_detach_permanently_ordinary.test_name_reuse;

DETACH table test1601_detach_permanently_ordinary.test_name_reuse;

SELECT 'can not detach permanently the table which is already detached (temporary)';
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY; -- { serverError 60 }

DETACH DATABASE test1601_detach_permanently_ordinary;
ATTACH DATABASE test1601_detach_permanently_ordinary;

SELECT 'After database reattachement the table is back (it was detached temporary)';
SELECT 'And we can detach it permanently';
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY;

DETACH DATABASE test1601_detach_permanently_ordinary;
ATTACH DATABASE test1601_detach_permanently_ordinary;

SELECT 'After database reattachement the table is still absent (it was detached permamently)';
SELECT 'And we can not detach it permanently';
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY; -- { serverError 60 }

SELECT 'But we can attach it back';
ATTACH TABLE test1601_detach_permanently_ordinary.test_name_reuse;

SELECT 'And detach permanently again to check how database drop will behave';
DETACH table test1601_detach_permanently_ordinary.test_name_reuse PERMANENTLY;

SELECT 'DROP database - Directory not empty error, but database detached';
DROP DATABASE test1601_detach_permanently_ordinary; -- { serverError 219 }

ATTACH DATABASE test1601_detach_permanently_ordinary;

ATTACH TABLE test1601_detach_permanently_ordinary.test_name_reuse;
DROP TABLE test1601_detach_permanently_ordinary.test_name_reuse;

SELECT 'DROP database - now success';
DROP DATABASE test1601_detach_permanently_ordinary;


SELECT '-----------------------';
SELECT 'database lazy tests';

DROP DATABASE IF EXISTS test1601_detach_permanently_lazy;
CREATE DATABASE test1601_detach_permanently_lazy Engine=Lazy(10);

create table test1601_detach_permanently_lazy.test_name_reuse (number UInt64) engine=Log;

INSERT INTO test1601_detach_permanently_lazy.test_name_reuse SELECT * FROM numbers(100);

DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY;

SELECT 'can not create table with same name as detached permanently';
create table test1601_detach_permanently_lazy.test_name_reuse (number UInt64) engine=Log; -- { serverError 57 }

SELECT 'can not detach twice';
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY; -- { serverError 60 }
DETACH table test1601_detach_permanently_lazy.test_name_reuse; -- { serverError 60 }

SELECT 'can not drop detached';
drop table test1601_detach_permanently_lazy.test_name_reuse; -- { serverError 60 }

create table test1601_detach_permanently_lazy.test_name_rename_attempt (number UInt64) engine=Log;

SELECT 'can not replace with the other table';
RENAME TABLE test1601_detach_permanently_lazy.test_name_rename_attempt TO test1601_detach_permanently_lazy.test_name_reuse; -- { serverError 57 }

SELECT 'can still show the create statement';
SHOW CREATE TABLE test1601_detach_permanently_lazy.test_name_reuse FORMAT Vertical;

SELECT 'can attach with full syntax';
ATTACH TABLE test1601_detach_permanently_lazy.test_name_reuse (`number` UInt64　)　ENGINE = Log;
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY;

SELECT 'can attach with short syntax';
ATTACH TABLE test1601_detach_permanently_lazy.test_name_reuse;

DETACH table test1601_detach_permanently_lazy.test_name_reuse;

SELECT 'can not detach permanently the table which is already detached (temporary)';
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY; -- { serverError 60 }

DETACH DATABASE test1601_detach_permanently_lazy;
ATTACH DATABASE test1601_detach_permanently_lazy;

SELECT 'After database reattachement the table is back (it was detached temporary)';
SELECT 'And we can detach it permanently';
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY;

DETACH DATABASE test1601_detach_permanently_lazy;
ATTACH DATABASE test1601_detach_permanently_lazy;

SELECT 'After database reattachement the table is still absent (it was detached permamently)';
SELECT 'And we can not detach it permanently';
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY; -- { serverError 60 }

SELECT 'But we can attach it back';
ATTACH TABLE test1601_detach_permanently_lazy.test_name_reuse;

SELECT 'And detach permanently again to check how database drop will behave';
DETACH table test1601_detach_permanently_lazy.test_name_reuse PERMANENTLY;

SELECT 'DROP database - Directory not empty error, but database deteched';
DROP DATABASE test1601_detach_permanently_lazy; -- { serverError 219 }

ATTACH DATABASE test1601_detach_permanently_lazy;

ATTACH TABLE test1601_detach_permanently_lazy.test_name_reuse;
DROP TABLE test1601_detach_permanently_lazy.test_name_reuse;

SELECT 'DROP database - now success';
DROP DATABASE test1601_detach_permanently_lazy;
