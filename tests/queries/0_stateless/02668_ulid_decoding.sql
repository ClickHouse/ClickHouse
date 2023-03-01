-- Tags: no-fasttest

SELECT dateDiff('s', ULIDStringToDateTime(generateULID()), now()) = 0;
