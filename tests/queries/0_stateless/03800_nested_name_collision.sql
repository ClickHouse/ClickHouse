-- Tags: no-parallel

DROP TABLE IF EXISTS nested_collision;

-- This previously crashed because "n" (String) conflicted with inferred Nested "n" from "n.a"
CREATE TABLE nested_collision
(
    n String,
    "n.a" Array(String)
)
ENGINE = Memory;

INSERT INTO nested_collision VALUES ('Hello', ['World']);

SELECT * FROM nested_collision ORDER BY n;

DROP TABLE nested_collision;