-- Tags: no-replicated-database

DROP TABLE IF EXISTS data_null;
DROP TABLE IF EXISTS set_null;
DROP TABLE IF EXISTS cannot_be_nullable;

SET data_type_default_nullable='false';

CREATE TABLE data_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT
) engine=Memory();


INSERT INTO data_null VALUES (NULL, 2, NULL, 4);

SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d) FROM data_null;

SHOW CREATE TABLE data_null;

CREATE TABLE data_null_error (
    a Nullable(INT) NULL,
    b INT NOT NULL,
    c Nullable(INT)
) engine=Memory();  --{serverError ILLEGAL_SYNTAX_FOR_DATA_TYPE}


CREATE TABLE data_null_error (
    a INT NULL,
    b Nullable(INT) NOT NULL,
    c Nullable(INT)
) engine=Memory();  --{serverError ILLEGAL_SYNTAX_FOR_DATA_TYPE}

SET data_type_default_nullable='true';

CREATE TABLE set_null (
    a INT NULL,
    b INT NOT NULL,
    c Nullable(INT),
    d INT,
    f DEFAULT 1
) engine=Memory();


INSERT INTO set_null VALUES (NULL, 2, NULL, NULL, NULL);

SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(f) FROM set_null;

SHOW CREATE TABLE set_null;
DETACH TABLE set_null;
ATTACH TABLE set_null;
SHOW CREATE TABLE set_null;

CREATE TABLE cannot_be_nullable (n Int8, a Array(UInt8)) ENGINE=Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE cannot_be_nullable (n Int8, a Array(UInt8) NOT NULL) ENGINE=Memory;
SHOW CREATE TABLE cannot_be_nullable;
DETACH TABLE cannot_be_nullable;
ATTACH TABLE cannot_be_nullable;
SHOW CREATE TABLE cannot_be_nullable;

DROP TABLE data_null;
DROP TABLE set_null;
DROP TABLE cannot_be_nullable;
