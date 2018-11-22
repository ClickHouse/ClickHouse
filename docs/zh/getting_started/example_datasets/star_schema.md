# Star Schema 基准测试

编译 dbgen: <https://github.com/vadimtk/ssb-dbgen>

```bash
git clone git@github.com:vadimtk/ssb-dbgen.git
cd ssb-dbgen
make
```

在编译过程中可能会有一些警告，这是正常的。

将`dbgen`和`dists.dss`放在一个可用容量大于800GB的磁盘中。

开始生成数据：

```bash
./dbgen -s 1000 -T c
./dbgen -s 1000 -T l
```

在ClickHouse中创建表结构：

``` sql
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
)Engine=MergeTree(LO_ORDERDATE,(LO_ORDERKEY,LO_LINENUMBER,LO_ORDERDATE),8192);

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
)Engine=MergeTree(C_FAKEDATE,(C_CUSTKEY,C_FAKEDATE),8192);

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
)Engine=MergeTree(P_FAKEDATE,(P_PARTKEY,P_FAKEDATE),8192);

CREATE TABLE lineorderd AS lineorder ENGINE = Distributed(perftest_3shards_1replicas, default, lineorder, rand());
CREATE TABLE customerd AS customer ENGINE = Distributed(perftest_3shards_1replicas, default, customer, rand());
CREATE TABLE partd AS part ENGINE = Distributed(perftest_3shards_1replicas, default, part, rand());
```

如果是在单节点中进行的测试，那么只需要创建对应的MergeTree表。
如果是在多节点中进行的测试，您需要在配置文件中配置`perftest_3shards_1replicas`集群的信息。
然后在每个节点中同时创建MergeTree表和Distributed表。

下载数据（如果您是分布式测试的话将'customer'更改为'customerd'）：

```bash
cat customer.tbl | sed 's/$/2000-01-01/' | clickhouse-client --query "INSERT INTO customer FORMAT CSV"
cat lineorder.tbl | clickhouse-client --query "INSERT INTO lineorder FORMAT CSV"
```


[Original article](https://clickhouse.yandex/docs/en/getting_started/example_datasets/star_schema/) <!--hide-->
