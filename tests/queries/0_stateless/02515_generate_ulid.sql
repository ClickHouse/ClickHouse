-- Tags: no-fasttest

SELECT generateULID(1) != generateULID(2), toTypeName(generateULID());
