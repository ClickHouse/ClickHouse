-- A `.limit`, a `.skip` and a `.sort` are suffixes of a `find`, and they used to be looked for as
-- plain text over everything that follows the argument list of the `find` - including the argument
-- of a suffix that came before. A field is free to be named `limit`, or to be a dotted path that
-- ends in one, so `.sort({"profile.limit" : 1})` held the text `.limit` while asking for no limit
-- at all, and the query was refused because a `"` rather than a `(` follows it there. The search
-- now skips the arguments of the suffixes and the string literals, so only a suffix of the `find`
-- itself is one.
--
-- The queries below are, in order: a sort by a field whose dotted path ends in each of the three
-- suffix names; the same sort together with a real `.limit`; and a sort by a field named after a
-- suffix inside a string literal.
--
-- The comments have to stay out of the `mongo` dialect: there a comment is part of the query text.

SET dialect='clickhouse';

DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id Int32, `profile.limit` Int32, `profile.skip` Int32, `profile.sort` Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO docs VALUES (1, 30, 3, 300), (2, 20, 2, 200), (3, 10, 1, 100);

SET allow_experimental_mongo_dialect = 1;
SET dialect='mongo';
db.docs.find({}).sort({"profile.limit" : 1});
db.docs.find({}).sort({"profile.skip" : 1});
db.docs.find({}).sort({"profile.sort" : 1});
db.docs.find({}).sort({"profile.limit" : 1}).limit(2);
db.docs.find({}).sort({"profile.sort" : -1}).skip(1).limit(1);

SET dialect='clickhouse';

DROP TABLE docs;
