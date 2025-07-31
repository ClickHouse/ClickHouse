DROP TABLE IF EXISTS nullable_test;
create table nullable_test(my_int_nullable Nullable(UInt32), my_text_lc_nullable LowCardinality(Nullable(String))) order by ();

insert into nullable_test values (NULL, NULL), (1, '1');

SELECT * from nullable_test ORDER BY ALL;

SYSTEM STOP MERGES nullable_test;

ALTER TABLE nullable_test MODIFY COLUMN my_int_nullable UInt64 SETTINGS mutations_sync = 0, alter_sync = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE nullable_test MODIFY COLUMN my_int_nullable UInt64 DEFAULT 42 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT * from nullable_test ORDER BY ALL;

SYSTEM START MERGES nullable_test;

SELECT * from nullable_test ORDER BY ALL;

OPTIMIZE TABLE nullable_test FINAL;
SELECT * from nullable_test ORDER BY ALL;

ALTER TABLE nullable_test MODIFY COLUMN my_text_lc_nullable String DEFAULT 'empty';
SELECT * from nullable_test ORDER BY ALL;

DROP TABLE IF EXISTS nullable_test;
