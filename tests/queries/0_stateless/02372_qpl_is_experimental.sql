DROP TABLE IF EXISTS qpl_codec;

CREATE TABLE qpl_codec (id Int32 CODEC(DEFLATE_QPL)) ENGINE = MergeTree() ORDER BY id; -- { serverError 36 }

SET allow_experimental_codecs = 1;
CREATE TABLE qpl_codec (id Int32 CODEC(DEFLATE_QPL)) ENGINE = MergeTree() ORDER BY id;

DROP TABLE IF EXISTS qpl_codec;

