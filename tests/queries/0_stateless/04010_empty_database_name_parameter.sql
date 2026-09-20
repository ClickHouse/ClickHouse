SET param_db='';
CREATE DATABASE {db:Identifier}; -- {serverError BAD_QUERY_PARAMETER}
DROP DATABASE {db:Identifier}; -- {serverError BAD_QUERY_PARAMETER}

SET param_table='';
DROP TABLE {db:Identifier}.{table:Identifier}; -- {serverError BAD_QUERY_PARAMETER}

SET param_db='some_db_name';
DROP TABLE {db:Identifier}.{table:Identifier}; -- {serverError BAD_QUERY_PARAMETER}

SET param_table='table';
DROP TABLE {db:Identifier}.{table:Identifier}; -- {serverError UNKNOWN_DATABASE}
