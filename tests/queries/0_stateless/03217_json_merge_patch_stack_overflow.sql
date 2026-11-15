-- Tags: no-fasttest
-- Needs rapidjson library
SELECT JSONMergePatch(REPEAT('{"c":', 1000000)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 100000)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 10000)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 1000)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 100)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 10)); -- { serverError BAD_ARGUMENTS }
SELECT JSONMergePatch(REPEAT('{"c":', 1)); -- { serverError BAD_ARGUMENTS }
