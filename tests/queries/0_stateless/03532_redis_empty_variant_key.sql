CREATE TABLE t0 (c0 Variant() PRIMARY KEY) ENGINE = Redis('<host>:<port>', 0, '<password>'); -- { serverError BAD_ARGUMENTS }
