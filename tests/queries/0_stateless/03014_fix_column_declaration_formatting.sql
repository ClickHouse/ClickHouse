WITH 'ALTER TABLE t_update_empty_nested ADD COLUMN `nested` negate(array(tuple(UInt8, UInt8)))' AS query
SELECT query = formatQuerySingleLine(query);
