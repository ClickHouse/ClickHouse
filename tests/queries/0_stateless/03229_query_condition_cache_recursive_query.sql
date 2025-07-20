-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that SYSTEM DROP QUERY CONDITION CACHE works

SET allow_experimental_analyzer = 1;

-- (it's silly to use what will be tested below but we have to assume other tests cluttered the query cache)
SYSTEM DROP QUERY CONDITION CACHE;

CREATE TABLE objects
(
    `id` String,
    `parent` String,
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO objects (`id`, `parent`) VALUES
  ('uuid1', 'uuid2'),
  ('uuid3', 'uuid4'),
  ('uuid4', 'uuid2'),
  ('uuid2', 'empty'),
  ('uuid5', 'uuid2'),
  ('uuid6', 'uuid4');

SELECT 'First run';
WITH RECURSIVE
        'empty' AS zero,
        _data AS (SELECT arrayJoin(['uuid3']) AS id),
        rec AS (
                SELECT id FROM objects WHERE id IN _data
                UNION ALL
                SELECT parent AS id FROM objects WHERE objects.id IN rec AND parent != zero GROUP BY parent
        )
SELECT * FROM rec GROUP BY id ORDER BY id;

SELECT 'Second run (copy-paste of the same query)';
WITH RECURSIVE
        'empty' AS zero,
        _data AS (SELECT arrayJoin(['uuid3']) AS id),
        rec AS (
                SELECT id FROM objects WHERE id IN _data
                UNION ALL
                SELECT parent AS id FROM objects WHERE objects.id IN rec AND parent != zero GROUP BY parent
        )
SELECT * FROM rec GROUP BY id ORDER BY id;

DROP TABLE objects;

