from pathlib import Path


def generate_fixtures(path: Path) -> None:
    lines = [
        "CREATE TABLE e2e_scalar (id BIGINT, name STRING, score DOUBLE, enabled BOOLEAN);",
        "INSERT INTO e2e_scalar VALUES "
        "(-2,'negative',-2.5,false),(-1,'',0.0,true),(0,NULL,0.0,NULL),"
        "(1,'ASCII',1.25,true),(2,'中文',2.5,false),(3,'quote-s',3.75,true),"
        "(4,'tab\\tvalue',4.0,false),(5,'line\\nvalue',5.5,NULL);",
        "CREATE TABLE e2e_seq_10007 (id BIGINT, payload STRING, bucket BIGINT);",
    ]

    for start in range(0, 10007, 1000):
        end = min(start + 1000, 10007)
        values = ",".join(
            f"({value},'row-{value:05d}',{value % 17})"
            for value in range(start, end)
        )
        lines.append(f"INSERT INTO e2e_seq_10007 VALUES {values};")

    lines.extend(
        [
            "CREATE TABLE e2e_partitioned (id BIGINT, value STRING) PARTITIONED BY (p_date STRING);",
            "INSERT INTO e2e_partitioned PARTITION(p_date='2026-09-13') VALUES "
            "(0,'p1-0'),(1,'p1-1'),(2,'p1-2'),(3,'p1-3'),(4,'p1-4');",
            "INSERT INTO e2e_partitioned PARTITION(p_date='2026-09-14') VALUES "
            "(5,'p2-5'),(6,'p2-6'),(7,'p2-7'),(8,'p2-8'),(9,'p2-9'),(10,'p2-10'),(11,'p2-11');",
            "CREATE TABLE e2e_complex (id BIGINT, arr ARRAY<BIGINT>, attrs MAP<STRING,BIGINT>, obj STRUCT<x:BIGINT,y:STRING>);",
            "INSERT INTO e2e_complex VALUES "
            "(1,ARRAY(0),MAP(ARRAY('empty'),ARRAY(0)),NAMED_STRUCT('x',NULL,'y',NULL)),"
            "(2,ARRAY(1),MAP(ARRAY('a'),ARRAY(10)),NAMED_STRUCT('x',20,'y','two')),"
            "(3,ARRAY(1,NULL,3),MAP(ARRAY('z','a'),ARRAY(30,NULL)),NAMED_STRUCT('x',NULL,'y','三'));",
            "CREATE TABLE e2e_temporal_decimal (id BIGINT, amount32 DECIMAL(9,2), amount64 DECIMAL(18,4), amount128 DECIMAL(38,9), d DATE, dt DATETIME, ts TIMESTAMP);",
            "INSERT INTO e2e_temporal_decimal VALUES "
            "(1,0.00,0.0000,0.000000000,CAST('1970-01-01' AS DATE),CAST('1970-01-01 00:00:00' AS DATETIME),CAST('1970-01-01 00:00:00.000000001' AS TIMESTAMP)),"
            "(2,-12.34,123456789.1234,12345678901234567890123456789.123456789,CAST('2026-09-14' AS DATE),CAST('2026-09-14 12:34:56' AS DATETIME),CAST('2026-09-14 12:34:56.123456789' AS TIMESTAMP));",
            "CREATE TABLE e2e_empty (id BIGINT, value STRING);",
            "CREATE TABLE e2e_temporal_bounds (id BIGINT, d DATE, dt DATETIME);",
            "INSERT INTO e2e_temporal_bounds VALUES "
            "(0,CAST('1970-01-01' AS DATE),CAST('1970-01-01 00:00:00.000' AS DATETIME)),"
            "(1,CAST('1970-01-02' AS DATE),CAST('1970-01-01 00:00:00.001' AS DATETIME)),"
            "(2,CAST('2149-06-06' AS DATE),CAST('2106-02-07 06:28:15.999' AS DATETIME));",
            "CREATE TABLE e2e_temporal_nulls (id BIGINT, d DATE, dt DATETIME);",
            "INSERT INTO e2e_temporal_nulls VALUES (0,NULL,NULL),"
            "(1,CAST('1970-01-01' AS DATE),CAST('1970-01-01 00:00:00' AS DATETIME));",
            "CREATE TABLE e2e_date_before_epoch (v DATE);",
            "INSERT INTO e2e_date_before_epoch VALUES (CAST('1969-12-31' AS DATE));",
            "CREATE TABLE e2e_date_after_max (v DATE);",
            "INSERT INTO e2e_date_after_max VALUES (CAST('2149-06-07' AS DATE));",
            "CREATE TABLE e2e_datetime_before_epoch (v DATETIME);",
            "INSERT INTO e2e_datetime_before_epoch VALUES (CAST('1969-12-31 23:59:59.999' AS DATETIME));",
            "CREATE TABLE e2e_datetime_after_max (v DATETIME);",
            "INSERT INTO e2e_datetime_after_max VALUES (CAST('2106-02-07 06:28:16.000' AS DATETIME));",
            "CREATE TABLE e2e_temporal_nested (dates ARRAY<DATE>, attrs MAP<STRING,DATE>, obj STRUCT<dt:DATETIME>);",
            "INSERT INTO e2e_temporal_nested VALUES "
            "(ARRAY(CAST('1969-12-31' AS DATE)),"
            "MAP(ARRAY('d'),ARRAY(CAST('2149-06-07' AS DATE))),"
            "NAMED_STRUCT('dt',CAST('2106-02-07 06:28:16' AS DATETIME)));",
        ]
    )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
