CREATE OR REPLACE FUNCTION 03518_bad_sql_udf AS lambda(identity(x), x); -- { BAD_ARGUMENTS }
