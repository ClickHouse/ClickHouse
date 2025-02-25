SET allow_suspicious_codecs = 1;
CREATE TABLE t0 (c0 String CODEC(Gorilla(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t0 (c0 String CODEC(Delta(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
