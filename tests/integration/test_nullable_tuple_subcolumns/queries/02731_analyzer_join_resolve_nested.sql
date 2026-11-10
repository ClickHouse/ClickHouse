-- Tuple-related queries from tests/queries/0_stateless/02731_analyzer_join_resolve_nested.sql.j2.

DROP TABLE IF EXISTS ttta;
DROP TABLE IF EXISTS tttb;

CREATE TABLE ttta
(
    x Int32,
    t Tuple(t Tuple(t Tuple(t Tuple(t UInt32, s String), s String), s String), s String)
) ENGINE = MergeTree ORDER BY x;

INSERT INTO ttta VALUES
    (1, ((((1, 's'), 's'), 's'), 's')),
    (2, ((((2, 's'), 's'), 's'), 's'));

CREATE TABLE tttb
(
    x Int32,
    t Tuple(t Tuple(t Tuple(t Tuple(t Int32, s String), s String), s String), s String)
) ENGINE = MergeTree ORDER BY x;

INSERT INTO tttb VALUES
    (2, ((((2, 's'), 's'), 's'), 's')),
    (3, ((((3, 's'), 's'), 's'), 's'));

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

SET join_use_nulls = 0;

SELECT t.*, t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.*, t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.t.*, t.t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.t.t.*, t.t.t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);

SET join_use_nulls = 1;

SELECT t.*, t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.*, t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.t.*, t.t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);
SELECT t.t.t.t.*, t.t.t.t.* APPLY toTypeName FROM ttta FULL JOIN tttb USING (t);

DROP TABLE IF EXISTS ttta;
DROP TABLE IF EXISTS tttb;
