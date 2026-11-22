INSERT INTO TABLE FUNCTION file(currentDatabase() || '/query.data', 'RowBinary') SELECT INTERVAL 1 SECOND SETTINGS interval_output_format = 'kusto';
