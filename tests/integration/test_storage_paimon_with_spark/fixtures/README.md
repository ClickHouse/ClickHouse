# Checked-in Paimon fixtures {#checked-in-paimon-fixtures}

`paimon_flink_111/` — a table written by **Flink 1.19** with
`org.apache.paimon:paimon-flink-1.19:1.1.1` (the dominant real-world Paimon
writer), with a schema evolution (`ALTER TABLE ... ADD (extra BIGINT)`)
between two commits. Regenerate:

```bash
docker run --rm --user root -v "$PWD":/repo flink:1.19-scala_2.12-java11 bash -c '
cd /opt/flink
curl -sfLO https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.19/1.1.1/paimon-flink-1.19-1.1.1.jar
curl -sfLO https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
mv paimon-flink-1.19-1.1.1.jar flink-shaded-hadoop-2-uber-2.8.3-10.0.jar lib/
./bin/start-cluster.sh && sleep 5
cat > /tmp/gen.sql <<"SQL"
SET '"'"'table.dml-sync'"'"' = '"'"'true'"'"';
CREATE CATALOG p WITH ('"'"'type'"'"' = '"'"'paimon'"'"', '"'"'warehouse'"'"' = '"'"'file:/tmp/out'"'"');
USE CATALOG p;
CREATE TABLE compat_flink (id INT NOT NULL, val STRING, dt DATE) WITH ('"'"'file.format'"'"' = '"'"'parquet'"'"');
INSERT INTO compat_flink VALUES (1, '"'"'one'"'"', DATE '"'"'2024-01-01'"'"'), (2, '"'"'two'"'"', DATE '"'"'2024-01-02'"'"');
ALTER TABLE compat_flink ADD (extra BIGINT);
INSERT INTO compat_flink VALUES (3, '"'"'three'"'"', DATE '"'"'2024-01-03'"'"', CAST(30 AS BIGINT));
SQL
./bin/sql-client.sh -f /tmp/gen.sql
cp -r /tmp/out/default.db/compat_flink \
  /repo/tests/integration/test_storage_paimon_with_spark/fixtures/paimon_flink_111/
'
```

`paimon_spark_132/` — a table written by **Paimon 1.3.2** (`org.apache.paimon:paimon-spark-3.5:1.3.2`,
Spark 3.5.5), while the live test writer in the runner image is Paimon 1.1.1.
`test_reader_compat.py` uploads it to object storage and reads it, pinning
forward-compatibility with tables written by newer Paimon versions
(customers run newer Flink/Spark writers than our baked jar).

Contents: partitioned table `compat(id INT NOT NULL, val STRING, d DATE, part INT)`,
two snapshots with a schema evolution (`ADD COLUMN extra BIGINT`) in between.

Regenerate with a newer Paimon version (from the repo root; the `rm` clears the
baked 1.1.1 jars from the ephemeral container so `--packages` is the only
Paimon on the classpath) — see `tmp/gen_paimon_132_fixture.sh` history or:

```bash
docker run --rm -v "$PWD":/repo clickhouse/integration-tests-runner:latest bash -c '
rm -f /spark-3.5.5-bin-hadoop3/jars/*paimon*
OUT=/repo/tests/integration/test_storage_paimon_with_spark/fixtures/paimon_spark_132
rm -rf "$OUT"
/spark-3.5.5-bin-hadoop3/bin/spark-sql \
  --packages org.apache.paimon:paimon-spark-3.5:1.3.2 \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=file:$OUT \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.session.timeZone=UTC \
  -e "CREATE TABLE paimon.default.compat (id INT NOT NULL, val STRING, d DATE, part INT)
        PARTITIONED BY (part) TBLPROPERTIES ('"'"'file.format'"'"'='"'"'parquet'"'"');
      INSERT INTO paimon.default.compat VALUES
        (1, '"'"'one'"'"', DATE '"'"'2024-01-01'"'"', 0), (2, '"'"'two'"'"', DATE '"'"'2024-01-02'"'"', 1);
      ALTER TABLE paimon.default.compat ADD COLUMN extra BIGINT;
      INSERT INTO paimon.default.compat VALUES (3, '"'"'three'"'"', DATE '"'"'2024-01-03'"'"', 0, 30);"
'
```
