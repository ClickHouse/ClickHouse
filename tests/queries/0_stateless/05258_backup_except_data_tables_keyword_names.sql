-- A table or database named like a BACKUP element keyword must stay quoted in an `EXCEPT DATA FROM TABLES` list,
-- otherwise the formatted query does not parse back.
-- https://github.com/ClickHouse/ClickHouse/issues/122426

SELECT formatQuery(q) AS f, formatQuery(f) = f
FROM values('q String',
    'BACKUP DATABASE db EXCEPT DATA FROM TABLES events, `view` TO Memory(\'b\')',
    'BACKUP DATABASE db EXCEPT DATA FROM TABLES view, events TO Memory(\'b\')',
    'BACKUP DATABASE db EXCEPT DATA FROM TABLES a, `dictionary`, `DataBase` TO Memory(\'b\')',
    'BACKUP DATABASE db EXCEPT DATA FROM TABLES a, `database`, TABLE db2.t TO Memory(\'b\')',
    'BACKUP ALL EXCEPT DATA FROM TABLES a, `view` TO Memory(\'b\')',
    'BACKUP ALL EXCEPT DATA FROM TABLES db.a, `view`.t TO Memory(\'b\')');
