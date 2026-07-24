-- A failed DETACH DICTIONARY ... PERMANENTLY (rejected because a dependent object exists) must leave
-- the dictionary fully usable. The dependency check that rejects the operation runs after the storage is
-- shut down, which for a dictionary deregisters it from the loader; without an atomic rollback the
-- dictionary stays attached but unusable (BAD_ARGUMENTS on use).

DROP DICTIONARY IF EXISTS d_atomic;
DROP TABLE IF EXISTS dep_atomic;
DROP TABLE IF EXISTS src_atomic;

CREATE TABLE src_atomic (id UInt64, val String) ENGINE = Memory;
INSERT INTO src_atomic VALUES (1, 'a');

CREATE DICTIONARY d_atomic (id UInt64, val String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src_atomic' DB currentDatabase()))
LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0);

-- A table whose column default reads the dictionary registers a dependency on it.
CREATE TABLE dep_atomic (id UInt64, v String DEFAULT dictGetString('d_atomic', 'val', id)) ENGINE = Memory;

SELECT dictGetString('d_atomic', 'val', 1);

-- Rejected because dep_atomic depends on the dictionary.
DETACH DICTIONARY d_atomic PERMANENTLY; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- Regression: the dictionary must still be usable after the rejected DETACH.
SELECT dictGetString('d_atomic', 'val', 1);

DROP TABLE dep_atomic;
DROP DICTIONARY d_atomic;
DROP TABLE src_atomic;
