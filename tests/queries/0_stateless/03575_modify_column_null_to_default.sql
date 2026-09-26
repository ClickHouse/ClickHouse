DROP TABLE IF EXISTS nullable_test;

CREATE TABLE nullable_test(
    my_int_nullable Nullable(UInt32),
    my_int_nullable_with_default Nullable(UInt32) DEFAULT NULL,
    my_int_nullable_with_default2 Nullable(UInt32) DEFAULT 11,
    my_text_lc_nullable LowCardinality(Nullable(String)),
) ORDER BY tuple();

INSERT INTO nullable_test VALUES (NULL, NULL, NULL, NULL), (1, 1, 1, 'A');

-- { echoOn }

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

-- Previouly existing DEFAULT NULL does not allow to modify
ALTER TABLE nullable_test MODIFY COLUMN my_int_nullable_with_default UInt64 SETTINGS mutations_sync = 0, alter_sync = 0; -- { serverError CANNOT_CONVERT_TYPE }

SELECT * from nullable_test ORDER BY ALL;

ALTER TABLE nullable_test MODIFY COLUMN my_int_nullable_with_default UInt64 DEFAULT 43 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT * from nullable_test ORDER BY ALL;

-- But when we have DEFAULT which is non NULL we can keep it
ALTER TABLE nullable_test MODIFY COLUMN my_int_nullable_with_default2 UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT * from nullable_test ORDER BY ALL;

DROP TABLE IF EXISTS nullable_test;
