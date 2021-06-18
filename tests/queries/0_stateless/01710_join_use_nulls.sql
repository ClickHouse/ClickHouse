DROP TABLE IF EXISTS X;
DROP TABLE IF EXISTS Y;

CREATE TABLE X (id Int) ENGINE=Memory;
CREATE TABLE Y (id Int) ENGINE=Memory;

-- Type mismatch of columns to JOIN by: plus(id, 1) Int64 at left, Y.id Int32 at right.
SELECT
    Y.id - 1
FROM X
RIGHT JOIN Y ON (X.id + 1) = Y.id
SETTINGS join_use_nulls=1; -- { serverError 53 }

DROP TABLE X;
DROP TABLE Y;
