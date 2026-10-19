SYSTEM DROP REPLICA 'r1' FROM ZKPATH ''; -- { clientError BAD_ARGUMENTS }
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/'; -- { clientError BAD_ARGUMENTS }
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '//'; -- { clientError BAD_ARGUMENTS }
SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'aux:/'; -- { clientError BAD_ARGUMENTS } -- 'aux:/' must not be misparsed as default keeper '/aux:'
SYSTEM DROP REPLICA 'r1' FROM ZKPATH 'aux://'; -- { clientError BAD_ARGUMENTS }
SYSTEM DROP DATABASE REPLICA 'r1' FROM ZKPATH ''; -- { clientError BAD_ARGUMENTS }
SYSTEM DROP DATABASE REPLICA 'r1' FROM ZKPATH 'aux:/'; -- { clientError BAD_ARGUMENTS }

-- NOTE: no comment lines may precede the first query in this file. Without a leading `-- Tags:`
-- line the client does not skip a leading comment block (`getTestTagsLength`), a comment block
-- glued to the first query becomes part of it, and the parse-time `clientError` hint on the
-- first query is then not honored (the hint search after a parse exception covers only the
-- first line of the query text).
