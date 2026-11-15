-- A valid measure-tagged point (POINT M) must still parse: the dimension-tag strip added for
-- rejecting POINT M EMPTY must not reject a non-empty M point, which returns its 2D coordinates.
DROP TABLE IF EXISTS geo_point_m_valid;
SELECT readWKTPoint('POINT M (1 2)');
SELECT readWKTPoint('point m (3 4)');
SELECT readWKTPoint('  POINT   M   (5 6)  ');
SELECT readWKT('POINT M (7 8)');

-- Vectorized: a valid M-tagged row alongside plain points keeps its own coordinates.
DROP TABLE IF EXISTS geo_point_m_valid;
CREATE TABLE geo_point_m_valid (s String, id Int) engine=Memory();
INSERT INTO geo_point_m_valid VALUES ('POINT (11 22)', 1), ('POINT M (33 44)', 2), ('POINT (55 66)', 3);
SELECT readWKTPoint(s) FROM geo_point_m_valid ORDER BY id;
DROP TABLE geo_point_m_valid;
