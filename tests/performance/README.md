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

### How to run performance test

TODO

### How to validate single test

```
pip3 install clickhouse_driver scipy
../../tests/performance/scripts/perf.py --runs 1 insert_parallel.xml
```
