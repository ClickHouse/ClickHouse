Star Schema
===========

Compile dbgen: https://github.com/vadimtk/ssb-dbgen

.. code-block:: bash

    git clone git@github.com:vadimtk/ssb-dbgen.git
    cd ssb-dbgen
    make

You will see some warnings. It's Ok.

Place ``dbgen`` and ``dists.dss`` to some place with at least 800 GB free space available.

Generate data:

.. code-block:: bash

    ./dbgen -s 1000 -T c
    ./dbgen -s 1000 -T l

Create tables in ClickHouse:

.. code-block:: sql

    CREATE TABLE lineorder (
            LO_ORDERKEY             UInt32,
            LO_LINENUMBER           UInt8,
            LO_CUSTKEY              UInt32,
            LO_PARTKEY              UInt32,
            LO_SUPPKEY              UInt32,
            LO_ORDERDATE            Date,
            LO_ORDERPRIORITY        String,
            LO_SHIPPRIORITY         UInt8,
            LO_QUANTITY             UInt8,
            LO_EXTENDEDPRICE        UInt32,
            LO_ORDTOTALPRICE        UInt32,
            LO_DISCOUNT             UInt8,
            LO_REVENUE              UInt32,
            LO_SUPPLYCOST           UInt32,
            LO_TAX                  UInt8,
            LO_COMMITDATE           Date,
            LO_SHIPMODE             String
    ) Engine = MergeTree(LO_ORDERDATE,(LO_ORDERKEY,LO_LINENUMBER,LO_ORDERDATE),8192);

    CREATE TABLE customer (
            C_CUSTKEY       UInt32,
            C_NAME          String,
            C_ADDRESS       String,
            C_CITY          String,
            C_NATION        String,
            C_REGION        String,
            C_PHONE         String,
            C_MKTSEGMENT    String,
            C_FAKEDATE      Date
    ) Engine = MergeTree(C_FAKEDATE,(C_CUSTKEY,C_FAKEDATE),8192);

    CREATE TABLE part (
            P_PARTKEY       UInt32,
            P_NAME          String,
            P_MFGR          String,
            P_CATEGORY      String,
            P_BRAND         String,
            P_COLOR         String,
            P_TYPE          String,
            P_SIZE          UInt8,
            P_CONTAINER     String,
            P_FAKEDATE      Date
    ) Engine = MergeTree(P_FAKEDATE,(P_PARTKEY,P_FAKEDATE),8192);

    CREATE TABLE lineorderd AS lineorder ENGINE = Distributed(perftest_3shards_1replicas, default, lineorder, rand());
    CREATE TABLE customerd AS customer ENGINE = Distributed(perftest_3shards_1replicas, default, customer, rand());
    CREATE TABLE partd AS part ENGINE = Distributed(perftest_3shards_1replicas, default, part, rand());

For single-node setup, create just MergeTree tables.
For Distributed setup, you must configure cluster ``perftest_3shards_1replicas`` in configuration file.
Then create MergeTree tables on each node and then create Distributed tables.

Load data (change customer to customerd in case of distributed setup):

.. code-block:: bash

    cat customer.tbl | sed 's/$/2000-01-01/' | clickhouse-client --query "INSERT INTO customer FORMAT CSV"
    cat lineorder.tbl | clickhouse-client --query "INSERT INTO lineorder FORMAT CSV"

