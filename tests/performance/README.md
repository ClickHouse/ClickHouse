## ClickHouse performance tests

This directory contains `.xml`-files with performance tests.

### How to write a performance test

First of all please check that existing tests don't cover your case. If there are no such tests then you can write your own test.

Test template:

``` xml
<test>
    <!-- Optional: Specify settings -->
    <settings>
        <max_threads>1</max_threads>
        <max_insert_threads>1</max_insert_threads>
    </settings>

    <!-- Optional: Variable substitutions, can be referenced to by curly brackets {} and used in any queries -->
    <substitutions>
        <substitution>
            <name>x</name>
            <values>
                <value>10</value>
                <value>50</value>
            </values>
        </substitution>
        <substitution>
            <name>y</name>
            <values>
                <value>5</value>
                <value>8</value>
            </values>
        </substitution>
    </substitutions>

    <!-- Optional: Table setup queries -->
    <create_query>CREATE TABLE tab1 [..]</create_query>
    <create_query>CREATE TABLE tab2 [..]</create_query>

    <!-- Optional: Table population queries -->
    <fill_query>INSERT INTO tab1 [...]</fill_query>
    <fill_query>INSERT INTO tab2 [...]</fill_query>

    <!-- Benchmark queries -->
    <query>SELECT [...] WHERE col BETWEEN {x} AND {y}</query>
    <query>SELECT [...]</query>
    <query>SELECT [...]</query>

    <!-- Optional: Table teardown queries -->
    <drop_query>DROP TABLE tab1</drop_query>
    <drop_query>DROP TABLE tab2</drop_query>
</test>
```

If your test takes more than 10 minutes, please, add tag `long` to have an opportunity to run all tests and skip long ones.

### Shell-script queries

In addition to SQL queries sent over the native protocol, a benchmark query can be a shell script, marked with `type="shell"`. This is useful for end-to-end measurements that the native protocol cannot express: HTTP latency, response compression, tool startup time, etc.

``` xml
<test>
    <!-- How fast clickhouse-local starts up. -->
    <query type="shell"><![CDATA[
        $CLICKHOUSE_LOCAL --query "SELECT 1" > /dev/null
    ]]></query>

    <!-- Reading ~1 MB of gzip-compressed data over HTTP. -->
    <query type="shell"><![CDATA[
        ${CLICKHOUSE_CURL} -H 'Accept-Encoding: gzip' \
            "${CLICKHOUSE_URL}?enable_http_compression=1" \
            --data-binary "SELECT number FROM numbers(500000) FORMAT TSV" \
            -o /dev/null
    ]]></query>
</test>
```

Each shell-script query is run with `bash -e -o pipefail` once per server (the reference build and the patched build), and timed by its wall-clock time, the same way a SQL query's time becomes the `client_time` metric in the report. Wrap the script in `<![CDATA[ ... ]]>` so that `<`, `>` and `&` do not need XML escaping. A non-zero exit code is treated as a failed query.

The script talks to the server using environment variables that mirror the stateless tests in `tests/queries/shell_config.sh`, prepared per-server so that the reference and the patched build (which listen on different ports) are measured each on their own:

* `CLICKHOUSE_BINARY` — path to the `clickhouse` binary,
* `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` — server endpoints,
* `CLICKHOUSE_CLIENT`, `CLICKHOUSE_LOCAL` — ready-to-run client and local commands,
* `CLICKHOUSE_CURL`, `CLICKHOUSE_URL` — `curl` invocation and the HTTP URL,
* `CLICKHOUSE_DATABASE` — the database name (`default`).

Notes:

* Parameter `{substitutions}` are **not** applied to shell scripts, because they use `${var}` and `{a,b}` brace expansion that would collide with the substitution syntax. Use shell loops or environment variables instead.
* The `<settings>` element is **not** applied to shell scripts; pass settings through the URL or client arguments inside the script.
* Profiler runs and server-side `ProfileEvents` are not collected for shell scripts (there is no single query to attribute them to); only the timing difference is reported.
* `CLICKHOUSE_CURL` is `curl -q -sS --fail --max-time 120`. Unlike `tests/queries/shell_config.sh`, it adds `--fail` so that an HTTP 4xx/5xx response makes `curl` exit non-zero and the query fails — a benchmark must never time a server error response as a fast successful sample.
* On timeout (`--max-query-seconds`, `--prewarm-max-query-seconds`) the whole process group of the script is killed, not just the immediate `bash`, so a script blocked inside a child such as `curl` or `$CLICKHOUSE_LOCAL` cannot keep running and pollute later measurements.

### How to run performance test

TODO

### How to validate single test

```
pip3 install clickhouse_driver scipy
../../tests/performance/scripts/perf.py --runs 1 insert_parallel.xml
```
