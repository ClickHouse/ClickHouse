-- A full-definition ATTACH is CREATE-like user input, so it must not bypass the Row opt-in.

DROP TABLE IF EXISTS row_attach_gate;

SET allow_experimental_row_type = 0;
ATTACH TABLE row_attach_gate UUID '05062000-0000-0000-0000-000000000001' (r Row(a UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE row_attach_gate UUID '05062000-0000-0000-0000-000000000002' (a Array(Row(x UInt64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SET allow_experimental_row_type = 1;
CREATE TABLE row_attach_gate (r Row(a UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO row_attach_gate VALUES ((5));

-- Re-attaching a definition stored on this server keeps working without the opt-in.
DETACH TABLE row_attach_gate;
SET allow_experimental_row_type = 0;
ATTACH TABLE row_attach_gate;
SELECT r, toTypeName(r) FROM row_attach_gate;

DROP TABLE row_attach_gate;
