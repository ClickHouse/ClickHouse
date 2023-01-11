DROP TABLE IF EXISTS test;

CREATE TABLE test(a UInt8, b String EPHEMERAL) Engine=Memory();

DROP TABLE test;

CREATE TABLE test(a UInt8, b EPHEMERAL String) Engine=Memory(); -- { clientError SYNTAX_ERROR }
CREATE TABLE test(a UInt8, b EPHEMERAL 'a' String) Engine=Memory(); -- { clientError SYNTAX_ERROR }
CREATE TABLE test(a UInt8, b String EPHEMERAL test) Engine=Memory(); -- { clientError SYNTAX_ERROR }
CREATE TABLE test(a UInt8, b String EPHEMERAL 1+2) Engine=Memory(); -- { clientError SYNTAX_ERROR }
