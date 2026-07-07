SET allow_suspicious_codecs = 1;
CREATE TABLE 03364_gorilla (c0 String CODEC(Gorilla(1))) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE 03364_delta (c0 String CODEC(Delta(1))) ENGINE = MergeTree() ORDER BY tuple();
DETACH TABLE 03364_gorilla;
DETACH TABLE 03364_delta;
ATTACH TABLE 03364_gorilla;
ATTACH TABLE 03364_delta;
DROP TABLE 03364_gorilla;
DROP TABLE 03364_delta;
