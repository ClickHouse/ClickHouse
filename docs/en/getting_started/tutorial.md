# ClickHouse Tutorial

## Setup

Let's get started with sample dataset from open sources. We will use USA civil flights data from 1987 to 2015. It's hard to call this sample a Big Data (contains 166 millions rows, 63 Gb of uncompressed data) but this allows us to quickly get to work. Dataset is available for download [here](https://yadi.sk/d/pOZxpa42sDdgm). Also you may download it from the original datasource [as described here](example_datasets/ontime.md).

At first we will deploy ClickHouse to a single server. Later we will also review the process of deployment to a cluster with support for sharding and replication.

ClickHouse is usually installed from [deb](index.md#from-deb-packages) or [rpm](index.md#from-rpm-packages) packages, but there are [alternatives](index.md#from-docker-image) for the operating systems that do no support them. What do we have in those packages:

* `clickhouse-client` package contains [clickhouse-client](../interfaces/cli.md) application, interactive ClickHouse console client.
* `clickhouse-common` package contains a ClickHouse executable file.
* `clickhouse-server` package contains configuration files to run ClickHouse as a server.

Server config files are located in /etc/clickhouse-server/. Before getting to work please notice the `path` element in config. Path dtermines the location for data storage. It's not really handy to directly edit `config.xml` file considering package updates. Recommended way to override the config elements is to create [files in config.d directory](../operations/configuration_files.md). Also you may want to [set up access rights](../operations/access_rights.md) early on.</p>

`clickhouse-server` won't be launched automatically after package installation. It won't be automatically  restarted after updates either. Start the server with:
``` bash
sudo service clickhouse-server start
```

The default location for server logs is `/var/log/clickhouse-server/`.

Server is ready to handle client connections once `Ready for connections` message was logged.

Use `clickhouse-client` to connect to the server.</p>

<details markdown="1"><summary>Tips for clickhouse-client</summary>
Interactive mode:
``` bash
clickhouse-client
clickhouse-client --host=... --port=... --user=... --password=...
```

Enable multiline queries:
``` bash
clickhouse-client -m
clickhouse-client --multiline
```

Run queries in batch-mode:
``` bash
clickhouse-client --query='SELECT 1'
echo 'SELECT 1' | clickhouse-client
```

Insert data from file of a specified format:
``` bash
clickhouse-client --query='INSERT INTO table VALUES' &lt; data.txt
clickhouse-client --query='INSERT INTO table FORMAT TabSeparated' &lt; data.tsv
```
</details>

## Create Table for Sample Dataset</h3>
<details markdown="1"><summary>Create table query</summary>
``` bash
$ clickhouse-client --multiline
ClickHouse client version 0.0.53720.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.53720.

:) CREATE TABLE ontime
(
    Year UInt16,
    Quarter UInt8,
    Month UInt8,
    DayofMonth UInt8,
    DayOfWeek UInt8,
    FlightDate Date,
    UniqueCarrier FixedString(7),
    AirlineID Int32,
    Carrier FixedString(2),
    TailNum String,
    FlightNum String,
    OriginAirportID Int32,
    OriginAirportSeqID Int32,
    OriginCityMarketID Int32,
    Origin FixedString(5),
    OriginCityName String,
    OriginState FixedString(2),
    OriginStateFips String,
    OriginStateName String,
    OriginWac Int32,
    DestAirportID Int32,
    DestAirportSeqID Int32,
    DestCityMarketID Int32,
    Dest FixedString(5),
    DestCityName String,
    DestState FixedString(2),
    DestStateFips String,
    DestStateName String,
    DestWac Int32,
    CRSDepTime Int32,
    DepTime Int32,
    DepDelay Int32,
    DepDelayMinutes Int32,
    DepDel15 Int32,
    DepartureDelayGroups String,
    DepTimeBlk String,
    TaxiOut Int32,
    WheelsOff Int32,
    WheelsOn Int32,
    TaxiIn Int32,
    CRSArrTime Int32,
    ArrTime Int32,
    ArrDelay Int32,
    ArrDelayMinutes Int32,
    ArrDel15 Int32,
    ArrivalDelayGroups Int32,
    ArrTimeBlk String,
    Cancelled UInt8,
    CancellationCode FixedString(1),
    Diverted UInt8,
    CRSElapsedTime Int32,
    ActualElapsedTime Int32,
    AirTime Int32,
    Flights Int32,
    Distance Int32,
    DistanceGroup UInt8,
    CarrierDelay Int32,
    WeatherDelay Int32,
    NASDelay Int32,
    SecurityDelay Int32,
    LateAircraftDelay Int32,
    FirstDepTime String,
    TotalAddGTime String,
    LongestAddGTime String,
    DivAirportLandings String,
    DivReachedDest String,
    DivActualElapsedTime String,
    DivArrDelay String,
    DivDistance String,
    Div1Airport String,
    Div1AirportID Int32,
    Div1AirportSeqID Int32,
    Div1WheelsOn String,
    Div1TotalGTime String,
    Div1LongestGTime String,
    Div1WheelsOff String,
    Div1TailNum String,
    Div2Airport String,
    Div2AirportID Int32,
    Div2AirportSeqID Int32,
    Div2WheelsOn String,
    Div2TotalGTime String,
    Div2LongestGTime String,
    Div2WheelsOff String,
    Div2TailNum String,
    Div3Airport String,
    Div3AirportID Int32,
    Div3AirportSeqID Int32,
    Div3WheelsOn String,
    Div3TotalGTime String,
    Div3LongestGTime String,
    Div3WheelsOff String,
    Div3TailNum String,
    Div4Airport String,
    Div4AirportID Int32,
    Div4AirportSeqID Int32,
    Div4WheelsOn String,
    Div4TotalGTime String,
    Div4LongestGTime String,
    Div4WheelsOff String,
    Div4TailNum String,
    Div5Airport String,
    Div5AirportID Int32,
    Div5AirportSeqID Int32,
    Div5WheelsOn String,
    Div5TotalGTime String,
    Div5LongestGTime String,
    Div5WheelsOff String,
    Div5TailNum String
)
ENGINE = MergeTree(FlightDate, (Year, FlightDate), 8192);
```
</details>

Now we have a table of [MergeTree type](../operations/table_engines/mergetree.md). MergeTree table engine family is recommended for usage in production. Tables of this kind has a primary key used for incremental sort of table data. This allows fast execution of queries in ranges of a primary key.

## Load Data
``` bash
xz -v -c -d &lt; ontime.csv.xz | clickhouse-client --query="INSERT INTO ontime FORMAT CSV"
```

ClickHouse INSERT query allows to load data in any [supported format](../interfaces/formats.md). Data load requires just O(1) RAM consumption. INSERT query can receive any data volume as input. It is strongly recommended to insert data with [not so small blocks](../introduction/performance/#performance-when-inserting-data). Notice that insert of blocks with size up to max_insert_block_size (= 1&nbsp;048&nbsp;576
        rows by default) is an atomic operation: data block will be inserted completely or not inserted at all. In case of disconnect during insert operation you may not know if the block was inserted successfully. To achieve exactly-once semantics ClickHouse supports idempotency for [replicated tables](../operations/table_engines/replication.md). This means that you may retry insert of the same data block (possibly on a different replicas) but this block will be inserted just once. Anyway in this guide we will load data from our localhost so we may not take care about data blocks generation and exactly-once semantics.

INSERT query into tables of MergeTree type is non-blocking (so does a SELECT query). You can execute SELECT queries right after of during insert operation.

Our sample dataset is a bit not optimal. There are two reasons:

* The first is that String data type is used in cases when [Enum](../data_types/enum.md) or numeric type would fit better.
* The second is that dataset contains redundant fields like Year, Quarter, Month, DayOfMonth, DayOfWeek. In fact a single FlightDate would be enough. Most likely they have been added to improve performance for other DBMS'eswhere DateTime handling functions may be not efficient.

!!! note "Tip"
    ClickHouse [functions for manipulating DateTime fields](../query_language/functions/date_time_functions/) are well-optimized so such redundancy is not required. Anyway many columns is not a reason to worry, because ClickHouse is a [column-oriented DBMS](https://en.wikipedia.org/wiki/Column-oriented_DBMS). This allows you to have as many fields as you need with minimal impact on performance. Hundreds of columns in a table is totally fine for ClickHouse.

## Querying the Sample Dataset

TODO

## Cluster Deployment

ClickHouse cluster is a homogenous cluster. Steps to set up:

1. Install ClickHouse server on all machines of the cluster
2. Set up cluster configs in configuration file
3. Create local tables on each instance
4. Create a [Distributed table](../operations/table_engines/distributed.md)

[Distributed table](../operations/table_engines/distributed.md) is actually a kind of "view" to local tables of ClickHouse cluster. SELECT query from a distributed table will be executed using resources of all cluster's shards. You may specify configs for multiple clusters and create multiple distributed tables providing views to different clusters.

<details markdown="1"><summary>Config for cluster with three shards, one replica each</summary>
``` xml
<remote_servers>
    <perftest_3shards_1replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
        <shard>
            <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
            </replica>
        </shard>
    </perftest_3shards_1replicas>
</remote_servers>
```
</details>
    
Creating a local table:
``` sql
CREATE TABLE ontime_local (...) ENGINE = MergeTree(FlightDate, (Year, FlightDate), 8192);
```

Creating a distributed table providing a view into local tables of the cluster:
``` sql
CREATE TABLE ontime_all AS ontime_local
ENGINE = Distributed(perftest_3shards_1replicas, default, ontime_local, rand());
```

You can create a Distributed table on all machines in the cluster. This would allow to run distributed queries on  any machine of the cluster. Besides distributed table you can also use [remote](../query_language/table_functions/remote.md) table function.

Let's run [INSERT SELECT](../query_language/insert_into.md) into Distributed table to spread the table to multiple servers.

``` sql
INSERT INTO ontime_all SELECT * FROM ontime;
```

!!! warning "Notice"
    The approach given above is not suitable for sharding of large tables.

As you could expect heavy queries are executed N times faster being launched on 3 servers instead of one.<

In this case we have used a cluster with 3 shards each contains a single replica.

To provide for resilience in production environment we recommend that each shard should contain 2-3 replicas distributed between multiple data-centers. Note that ClickHouse supports unlimited number of replicas.

<details markdown="1"><summary>Config for cluster of one shard containing three replicas</summary>
``` xml
<remote_servers>
    ...
    <perftest_1shards_3replicas>
        <shard>
            <replica>
                <host>example-perftest01j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest02j.yandex.ru</host>
                <port>9000</port>
             </replica>
             <replica>
                <host>example-perftest03j.yandex.ru</host>
                <port>9000</port>
             </replica>
        </shard>
    </perftest_1shards_3replicas>
</remote_servers>
```
</details>
    
To enable replication <a href="http://zookeeper.apache.org/" rel="external nofollow">ZooKeeper</a> is required. ClickHouse will take care of data consistency on all replicas and run restore procedure after failure
        automatically. It's recommended to deploy ZooKeeper cluster to separate servers.

ZooKeeper is not a requirement — in some simple cases you can duplicate the data by writing it into all the replicas from your application code. This approach is not recommended — in this case ClickHouse is not able to
        guarantee data consistency on all replicas. This remains the responsibility of your application.

<details markdown="1"><summary>Specify ZooKeeper locations in configuration file</summary>
``` xml
<zookeeper-servers>
    <node>
        <host>zoo01.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo02.yandex.ru</host>
        <port>2181</port>
    </node>
    <node>
        <host>zoo03.yandex.ru</host>
        <port>2181</port>
    </node>
</zookeeper-servers>
```
</details>

Also we need to set macros for identifying shard and replica — it will be used on table creation:
``` xml
<macros>
    <shard>01</shard>
    <replica>01</replica>
</macros>
```
If there are no replicas at the moment on replicated table creation — a new first replica will be instantiated. If there are already live replicas — new replica will clone the data from existing ones. You have an option to create all replicated tables first and that insert data to it. Another option is to create some replicas and add the others after or during data insertion.

``` sql
CREATE TABLE ontime_replica (...)
ENGINE = ReplcatedMergeTree(
    '/clickhouse_perftest/tables/{shard}/ontime',
    '{replica}',
    FlightDate,
    (Year, FlightDate),
    8192);
```

Here we use [ReplicatedMergeTree](../operations/table_engines/replication.md) table engine. In parameters we specify ZooKeeper path containing shard and replica identifiers.

``` sql
INSERT INTO ontime_replica SELECT * FROM ontime;
```
Replication operates in multi-master mode. Data can be loaded into any replica — it will be synced with other instances automatically. Replication is asynchronous so at a given moment of time not all replicas may contain recently inserted data. To allow data insertion at least one replica should be up. Others will sync up data and repair consistency once they will become active again. Please notice that such scheme allows for the possibility of just appended data loss.
