-- Tags: no-fasttest
-- Rendering documentation for every function must not crash the server, even if some function declares an
-- unknown documentation type. Reading these tables forces full documentation rendering for every function.
SELECT count() > 0 FROM system.functions;
SELECT count() > 0 FROM system.documentation;
-- Force materialization of the documentation columns specifically.
SELECT count() FROM system.functions WHERE length(arguments) >= 0 AND length(returned_value) >= 0 AND length(syntax) >= 0 FORMAT Null;
