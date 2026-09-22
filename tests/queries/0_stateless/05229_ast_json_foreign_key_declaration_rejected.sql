-- `ForeignKeyDeclaration` is not part of the AST JSON dialect. The SQL parser drops the node before the
-- final AST, so no query can produce one, and there is no formatter for it.
SELECT formatQueryFromJSON('{"type":"ForeignKeyDeclaration"}'); -- { serverError BAD_ARGUMENTS }

-- Rejected in a nested position as well.
SELECT formatQueryFromJSON('{"type":"ExpressionList","children":[{"type":"ForeignKeyDeclaration"}]}'); -- { serverError BAD_ARGUMENTS }

-- A FOREIGN KEY clause in a table definition is still parsed and ignored, and its JSON round trip is unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE child (id Int32, pid Int32, PRIMARY KEY (id), FOREIGN KEY (pid) REFERENCES parent (pid)) ENGINE = MergeTree'));
