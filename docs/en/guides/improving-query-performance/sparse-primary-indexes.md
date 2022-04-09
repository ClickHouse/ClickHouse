---
sidebar_label: Sparse Primary Indexes
sidebar_position: 20
---



# A Practical Introduction to Sparse Primary Indexes in ClickHouse

In this article we are going to do a deep dive into ClickHouse indexing. We will illustrate and discuss in detail:
- how indexing in ClickHouse is different from traditional relational database management systems
- how ClickHouse is building and using a table’s sparse primary index
- what the best practices are for indexing in ClickHouse

You can optionally execute all Clickhouse SQL statements and queries given in this article by yourself on your own machine.
For installation of ClickHouse and getting started instructions, see the [Quick Start](../../quick-start.mdx).

:::note
This article is focusing on ClickHouse sparse primary indexes.

For ClickHouse <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-data_skipping-indexes" target="_blank">secondary data skipping indexes</a>, see the [Tutorial](./skipping-indexes.md). 


:::


## Data Set 

Throughout this article we will use a sample anonymized web traffic data set.

- We will use a subset of 8.87 million rows (events) from the sample data set. 
- The uncompressed data size is 8.87 million events and about 700 MB. This compresses to 200 mb when stored in ClickHouse.
- In our subset, each row contains three columns that indicate an internet user (`UserID` column) who clicked on a URL (`URL` column) at a specific time (`EventTime` column). 

With these three columns we can already formulate some typical web analytics queries such as:
 
- "What are the top 10 most clicked urls for a specific user?”  
- "What are the top 10 users that most frequently clicked a specific URL?" 
- “What are the most popular times (e.g. days of the week) at which a user clicks on a specific URL?”

## Test Machine

All runtime numbers given in this document are based on running ClickHouse 22.2.1 locally on a MacBook Pro with the Apple M1 Pro chip and 16GB of RAM.


## A full table scan

In order to see how a query is executed over our data set without a primary key, we create a table (with a MergeTree table engine) by executing the following SQL DDL statement:

```sql
CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple();
```



Next insert a subset of the hits data set into the table with the following SQL insert statement. This uses the <a href="https://clickhouse.com/docs/en/sql-reference/table-functions/url/" target="_blank">URL table function</a> in combination with <a href="https://clickhouse.com/blog/whats-new-in-clickhouse-22-1/#schema-inference" target="_blank">schema inference</a> in order to load a  subset of the full dataset hosted remotely at clickhouse.com:

```sql
INSERT INTO hits_NoPrimaryKey SELECT
   intHash32(c11::UInt64) AS UserID,
   c15 AS URL,
   c5 AS EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE URL != '';
```
The response is:
```response 
Ok.

0 rows in set. Elapsed: 145.993 sec. Processed 8.87 million rows, 18.40 GB (60.78 thousand rows/s., 126.06 MB/s.)
``` 


ClickHouse client’s result output shows us that the statement above inserted 8.87 million rows into the table.


Lastly, in order to simplify the discussions later on in this article and to make the diagrams and results reproducible, we <a href="https://clickhouse.com/docs/en/sql-reference/statements/optimize/" target="_blank">optimize</a> the table using the FINAL keyword:

```sql
OPTIMIZE TABLE hits_NoPrimaryKey FINAL;
```

:::note
In general it is not required nor recommended to immediately optimize a table
after loading data into it. Why this is necessary for this example will become apparent.
:::


Now we execute our first web analytics query. The following is calculating the top 10 most clicked urls for the internet user with the UserID 749927693:

```sql
SELECT URL, count(URL) as Count
FROM hits_NoPrimaryKey
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```
The response is:
```response 
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.022 sec.
// highlight-next-line 
Processed 8.87 million rows, 
70.45 MB (398.53 million rows/s., 3.17 GB/s.)
``` 


ClickHouse client’s result output indicates that ClickHouse executed a full table scan! Each single row of the 8.87 million rows of our table was streamed into ClickHouse. That doesn’t scale.

To make this (way) more efficient and (much) faster, we need to use a table with a appropriate primary key. This will allow ClickHouse to automatically (based on the primary key’s column(s)) create a sparse primary index which can then be used to significantly speed up the execution of our example query.

## <a name="original-table"></a>A table with a primary key

Create a table that has a compound primary key with key columns UserID and URL: 

```sql
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
// highlight-next-line    
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
```

[//]: # (<details open>)
<details>
    <summary><font color="black">
    DDL Statement Details
    </font></summary>
    <p><font color="black">

In order to simplify the discussions later on in this article, as well as  make the diagrams and results reproducible, the DDL statement
<ul>
<li>specifies a compound sorting key for the table via an <font face = "monospace">ORDER BY</font> clause</li>
<br/>
<li>explicitly controls how many index entries the primary index will have through the settings:</li>
<br/>
<ul>
<li><font face = "monospace">index_granularity</font>: explicitly set to its default value of 8192. This means that for each group of 8192 rows, the primary index will have one index entry, e.g. if the table contains 16384 rows then the index will have two index entries.
</li>
<br/>
<li><font face = "monospace">index_granularity_bytes</font>: set to 0 in order to disable <a href="https://clickhouse.com/docs/en/whats-new/changelog/2019/#experimental-features-1" target="_blank"><font color="blue">adaptive index granularity</font></a>. Adaptive index granularity means that ClickHouse automatically creates one index entry for a group of n rows
<ul>
<li>if either n is less than 8192 but the size of the combined row data for that n rows is larger than or equal 10 MB (the default value for index_granularity_bytes) or</li>
<li>if the combined row data size for n rows is less than 10 MB but n is 8192.</li>
</ul>
</li>
</ul>
</ul>
</font></p>
</details>


The primary key in the DDL statement above causes the creation of primary index based on the two specified key columns.

<br/>
Next insert the data:

```sql
INSERT INTO hits_UserID_URL SELECT
   intHash32(c11::UInt64) AS UserID,
   c15 AS URL,
   c5 AS EventTime
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE URL != '';
```
The response looks like:
```response 
0 rows in set. Elapsed: 149.432 sec. Processed 8.87 million rows, 18.40 GB (59.38 thousand rows/s., 123.16 MB/s.)
```


<br/>
And optimize the table:

```sql
OPTIMIZE TABLE hits_UserID_URL FINAL;
```

<br/>
We can use the following query to obtain metadata about our table:

```sql
SELECT
    part_type,
    path,
    formatReadableQuantity(rows) AS rows,
    formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
    formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
    formatReadableSize(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
    marks,
    formatReadableSize(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE (table = 'hits_UserID_URL') AND (active = 1)
FORMAT Vertical;
```
 
The response is:

```response
part_type:                   Wide
path:                        ./store/d9f/d9f36a1a-d2e6-46d4-8fb5-ffe9ad0d5aed/all_1_9_2/
rows:                        8.87 million
data_uncompressed_bytes:     733.28 MiB
data_compressed_bytes:       206.94 MiB
primary_key_bytes_in_memory: 96.93 KiB
marks:                       1083
bytes_on_disk:               207.07 MiB


1 rows in set. Elapsed: 0.003 sec.
```

The output of the ClickHouse client shows:
 
- The table’s data is stored in <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-data-storage" target="_blank">wide format</a> in a specific directory on disk meaning that there will be one data file (and one mark file) per table column inside that directory.
- The table has 8.87 million rows.
- The uncompressed data size of all rows together is 733.28 MB.
- The compressed on disk data size of all rows together is 206.94 MB.
- The table has a primary index with 1083 entries (called ‘marks’) and the size of the index is 96.93 KB.
- In total the table’s data and mark files and primary index file together take 207.07 MB on disk.


## An index design for massive data scales

In traditional relational database management systems the primary index would contain one entry per table row. For our data set this would result in the  primary index - often a <a href="https://en.wikipedia.org/wiki/B%2B_tree" target="_blank">B(+)-Tree</a> data structure - containing 8.87 million entries. 

Such an index allows the fast location of specific rows, resulting in high efficiency for lookup queries and point updates. Searching an entry in a B(+)-Tree data structure has average time complexity of <font face = "monospace">O(log2 n)</font>. For a table of 8.87 million rows this means 23 steps are required to locate any index entry.

This capability comes at a cost: additional disk and memory overheads and higher insertion costs when adding new rows to to the table and entries to the index (and also sometimes rebalancing of the B-Tree).

Considering the challenges associated with B-Tee indexes, table engines in ClickHouse utilise a different approach. The ClickHouse <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/" target="_blank">MergeTree Engine Family</a> has been designed and  optimized to handle massive data volumes.

These tables are designed to receive  millions of row inserts per second and store very large (100s of Petabytes) volumes of data.

Data is quickly written to a table <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-data-storage" target="_blank">part by part</a>, with rules applied for merging the parts in the background. 

In ClickHouse each part has its own primary index. When parts are merged then also the merged part’s primary indexes are merged. 

At the very large scale that ClickHouse is designed for, it is paramount to be very disk and memory efficient. Therefore, instead of indexing every row, the primary index for a part has one index entry (known as a ‘mark’) per group of rows (called ‘granule’). 

This sparse indexing is possible because ClickHouse is storing the rows for a part on disk ordered by the primary key column(s). 

Instead of directly locating single rows (like a B-Tree based index), the sparse primary index allows it to quickly (via a binary search over index entries) identify groups of rows that could possibly match the query. 

The located groups of potentially matching rows (granules) are then in parallel streamed into the ClickHouse engine in order to find the matches.

This index design allows for the primary index to be small (it can and must completely fit into the main memory), whilst still significantly speeding up query execution times: especially for range queries that are typical in data analytics use cases.

The following illustrates in detail how ClickHouse is building and using its sparse primary index.
Later on in the article we will discuss some best practices for choosing, removing, and ordering the table columns that are used to build the index (primary key columns).

## <a name="data-storage"></a>Data is stored on disk ordered by primary key column(s)

Our table that we created above has 
- a compound <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#primary-keys-and-indexes-in-queries" target="_blank">primary key</a> <font face = "monospace">(UserID, URL)</font> and 
- a compound <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#choosing-a-primary-key-that-differs-from-the-sorting-key" target="_blank">sorting key</a> <font face = "monospace">(UserID, URL, EventTime)</font>. 

:::note
The primary key needs to be a prefix of the sorting key if both are specified.
:::


The inserted rows are stored on disk in lexicographical order (ascending) by the primary key columns. 

:::note
ClickHouse allows inserting multiple rows with identical primary key column values. In this case (see row 1 and row 2 in the diagram below), the final order is determined by the specified sorting key and therefore the value of the <font face = "monospace">EventTime</font> column.
:::



ClickHouse is a <a href="https://clickhouse.com/docs/en/introduction/distinctive-features/#true-column-oriented-dbms
" target="_blank">column-oriented database management system</a>. As show in the diagram below
- for the on disk representation, there is a single data file (*.bin) per table column where all the values for that column are stored in a <a href="https://clickhouse.com/docs/en/introduction/distinctive-features/#data-compression" target="_blank">compressed</a> format, and
- the 8.87 million rows are stored on disk in lexicographic ascending order by the primary key columns (and sort key columns) i.e. in this case
  - first by <font face = "monospace">UserID</font>, 
  - then by <font face = "monospace">URL</font>, 
  - and lastly by <font face = "monospace">EventTime</font>:

<img src={require('./images/sparse-primary-indexes-01.png').default} class="image"/>
UserID.bin, URL.bin, and EventTime.bin are the data files on disk where the values of the <font face = "monospace">UserID</font>, <font face = "monospace">URL</font>, and <font face = "monospace">EventTime</font> columns are stored. 

<br/>
<br/>


:::note
- As the primary key defines the lexicographical order of the rows on disk, a table can only have one primary key.

- We are numbering rows starting with 0 in order to be aligned with the ClickHouse internal row numbering scheme that is also used for logging messages.
:::

## <a name="granules"></a>Data is organized into granules for parallel data processing

For data processing purposes, a table's column values are logically divided into granules. 
A granule is the smallest indivisible data set that is streamed into ClickHouse for data processing.
This means that instead of reading individual rows, ClickHouse is always reading (in a streaming fashion and in parallel) a whole group (granule) of rows.
:::note
Column values are not physically stored inside granules: granules are just a logical organization of the column values for query processing.
:::

The following diagram shows how the (column values of) 8.87 million rows of our table 
are organized into 1083 granules, as a result of the table's DDL statement containing the setting <font face = "monospace">index_granularity</font> (set to its default value of 8192). 

<img src={require('./images/sparse-primary-indexes-02.png').default} class="image"/>

The first (based on physical order on disk) 8192 rows (their column values) logically belong to granule 0, then the next 8192 rows (their column values) belong to granule 1 and so on. 

:::note
- The last granule (granule 1082) "contains" less than 8192 rows.

- We marked some column values from our primary key columns (<font face = "monospace">UserID</font>, <font face = "monospace">URL</font>) in orange. 

  These orange marked column values are the minimum value of each primary key column in each granule. The exception here is the last granule (granule 1082 in the diagram above) where we mark the maximum values. 

  As we will see below, these orange marked column values will be the entries in the table's primary index.

- We are numbering granules starting with 0 in order to be aligned with the ClickHouse internal numbering scheme that is also used for logging messages.
:::

## <a name="primary-index"></a>The primary index has one entry per granule

The primary index is created based on the granules shown in the diagram above. This index is an uncompressed flat array file (primary.idx), containing so-called numerical index marks starting at 0.

The diagram below shows that the index stores the minimum primary key column values (the values marked in orange in the diagram above) for each granule. 
For example
- the first index entry (‘mark 0’ in the diagram below) is storing the minimum values for the primary key columns of granule 0 from the diagram above,  
- the second index entry (‘mark 1’ in the diagram below) is storing the minimum values for the primary key columns of granule 1 from the diagram above, and so on. 



<img src={require('./images/sparse-primary-indexes-03a.png').default} class="image"/>

In total the index has 1083 entries for our table with 8.87 million rows and 1083 granules: 

<img src={require('./images/sparse-primary-indexes-03b.png').default} class="image"/>

:::note
- The last index entry (‘mark 1082’ in the diagram below) is storing the maximum values for the primary key columns of granule 1082 from the diagram above.

- Index entries (index marks) are not based on specific rows from our table but on granules. E.g. for index entry ‘mark 0’ in the diagram above there is no row in our table where <font face = "monospace">UserID</font> is <font face = "monospace">240.923</font> and <font face = "monospace">URL</font> is <font face = "monospace">"goal://metry=10000467796a411..."</font>, instead, there is a granule 0 for the table where within that granule the minimum UserID vale is <font face = "monospace">240.923</font> and the minimum URL value is <font face = "monospace">"goal://metry=10000467796a411..."</font> and these two values are from separate rows.
:::


The primary key entries are called index marks because each index entry is marking the start of a specific data range. Specifically for the example table:
- UserID index marks:<br/> 
  The stored <font face = "monospace">UserID</font> values in the primary index are sorted in ascending order.<br/> 
  ‘mark 1’ in the diagram above thus indicates that the <font face = "monospace">UserID</font> values of all table rows in granule 1, and in all following granules, are guaranteed to be greater than or equal to 4.073.710.
 
 [As we will see later](#query-on-userid-fast), this global order enables ClickHouse to <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank">use a binary search algorithm</a> over the index marks for the first key column when a query is filtering on the first column of the primary key.

- URL index marks:<br/> 
  The quite similar cardinality of the primary key columns <font face = "monospace">UserID</font> and <font face = "monospace">URL</font> means that the index marks for all key columns after the first column in general only indicate a data range per granule.<br/> 
 For example, ‘mark 0’ for the <font face = "monospace">URL</font> column in the diagram above is indicating that the URL values of all table rows in granule 0 are guaranteed to be larger or equal to <font face = "monospace">goal://metry=10000467796a411...</font>. However, this same guarantee cannot also be given for the <font face = "monospace">URL</font> values of all table rows in granule 1  because ‘mark 1‘ for the <font face = "monospace">UserID</font> column has a different UserID value than ‘mark 0‘. 
  
  We will discuss the consequences of this on query execution performance in more detail later.

## The primary index is used for selecting granules

We can now execute our queries with support from the primary index.

<a name="query-on-userid"></a>
The following calculates the top 10 most clicked urls for the UserID 749927693.

```sql
SELECT URL, count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```
 
The response is:
<a name="query-on-userid-fast"></a>

```response
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.005 sec.
// highlight-next-line  
Processed 8.19 thousand rows, 
740.18 KB (1.53 million rows/s., 138.59 MB/s.)
```

The output for the ClickHouse client is now showing that instead of doing a full table scan, only 8.19 thousand rows were streamed into ClickHouse. 


If <a href="https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings/#server_configuration_parameters-logger" target="_blank">trace logging</a> is enabled then the ClickHouse server log file shows that ClickHouse was running a <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank">binary search</a> over the 1083 UserID index marks, in order to identify granules that possibly can contain rows with a UserID column value of <font face = "monospace">749927693</font>. This requires 19 steps with an average time complexity of <font face = "monospace">O(log2 n)</font>: 
```response
...Executor): Key condition: (column 0 in [749927693, 749927693])
// highlight-next-line 
...Executor): Running binary search on index range for part all_1_9_2 (1083 marks)
...Executor): Found (LEFT) boundary mark: 176
...Executor): Found (RIGHT) boundary mark: 177
...Executor): Found continuous range in 19 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line  
              1/1083 marks by primary key, 1 marks to read from 1 ranges
...Reading ...approx. 8192 rows starting from 1441792
```


We can see in the trace log above, that one mark out of the 1083 existing marks satisfied the query. 

<details>
    <summary><font color="black">
    Trace Log Details
    </font></summary>
    <p><font color="black">

Mark 176 was identified (the 'found left boundary mark' is inclusive, the 'found right boundary mark' is exclusive), and therefore all 8192 rows from granule 176 (which starts at row 1.441.792 - we will see that later on in this article) are then streamed into ClickHouse in order to find the actual rows with a UserID column value of <font face = "monospace">749927693</font>. 
</font></p>
</details>

We can also reproduce this by using the <a href="https://clickhouse.com/docs/en/sql-reference/statements/explain/" target="_blank">EXPLAIN clause</a> in our example query:
```sql
EXPLAIN indexes = 1
SELECT URL, count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```
 
The response looks like:

```response
┌─explain───────────────────────────────────────────────────────────────────────────────┐
│ Expression (Projection)                                                               │
│   Limit (preliminary LIMIT (without OFFSET))                                          │
│     Sorting (Sorting for ORDER BY)                                                    │
│       Expression (Before ORDER BY)                                                    │
│         Aggregating                                                                   │
│           Expression (Before GROUP BY)                                                │
│             Filter (WHERE)                                                            │
│               SettingQuotaAndLimits (Set limits and quota after reading from storage) │
│                 ReadFromMergeTree                                                     │
│                 Indexes:                                                              │
│                   PrimaryKey                                                          │
│                     Keys:                                                             │
│                       UserID                                                          │
│                     Condition: (UserID in [749927693, 749927693])                     │
│                     Parts: 1/1                                                        │
// highlight-next-line
│                     Granules: 1/1083                                                  │
└───────────────────────────────────────────────────────────────────────────────────────┘

16 rows in set. Elapsed: 0.003 sec.
```
The client output is showing that one out of the 1083 granules was selected as possibly containing rows with a UserID column value of 749927693. 


:::note Conclusion
When a query is filtering on a column that is part of a compound key and is the first key column, then ClickHouse is running the binary search algorithm over the key column's index marks.
:::

<br/>


As discussed above, ClickHouse is using its sparse primary index for quickly (via binary search) selecting granules that could possibly contain rows that match a query. 


This is the **first stage (granule selection)** of ClickHouse query execution.

In the **second stage (data reading)**, ClickHouse is locating the selected granules in order to stream all their rows into the ClickHouse engine in order to find the rows that are actually matching the query. 

We discuss that second stage in more detail in the following section.  

## <a name="mark-files"></a>Mark files are used for locating granules

The following diagram illustrates a part of the primary index file for our table. 

<img src={require('./images/sparse-primary-indexes-04.png').default} class="image"/>

As discussed above, via a binary search over the index’s 1083 UserID marks, mark 176 were identified. Its corresponding granule 176 can therefore possibly contain rows with a UserID column value of 749.927.693.

<details>
    <summary><font color="black">
    Granule Selection Details
    </font></summary>
    <p><font color="black">

The diagram above shows that mark 176 is the first index entry where both the minimum UserID value of the associated granule 176 is smaller than 749.927.693, and the minimum UserID value of granule 177 for the next mark (mark 177) is greater than this value. Therefore only the corresponding granule 176 for mark 176 can possibly contain rows with a UserID column value of 749.927.693.
</font></p>
</details>

In order to confirm (or not) that some row(s) in granule 176 contain a UserID column value of 749.927.693, all 8192 rows belonging to this granule need to be streamed into ClickHouse. 

To achieve this, ClickHouse needs to know the physical location of granule 176.

In ClickHouse the physical locations of all granules for our table are stored in mark files. Similar to data files, there is one mark file per table column. 

The following diagram shows the three mark files UserID.mrk, URL.mrk, and EventTime.mrk that store the physical locations of the granules for the table’s UserID, URL, and EventTime columns.
<img src={require('./images/sparse-primary-indexes-05.png').default} class="image"/>

We have discussed how the primary index is a flat uncompressed array file (primary.idx), containing index marks that are numbered starting at 0.  

Similarily, a mark file is also a flat uncompressed array file (*.mrk) containing marks that are numbered starting at 0.

Once ClickHouse has identified and selected the index mark for a granule that can possibly contain matching rows for a query, a positional array lookup can be performed in the mark files in order to obtain the physical locations of the granule.

Each mark file entry for a specific column is storing two locations in the form of offsets: 

- The first offset ('block_offset' in the diagram above) is locating the <a href="https://clickhouse.com/docs/en/development/architecture/#block" target="_blank">block</a> in the <a href="https://clickhouse.com/docs/en/introduction/distinctive-features/#data-compression" target="_blank">compressed</a> column data file that contains the compressed version of the selected granule. This compressed block potentially contains a few compressed granules. The located compressed file block is uncompressed into the main memory on read. 

- The second offset ('granule_offset' in the diagram above) from the mark-file provides the location of the  granule within the uncompressed block data.

All the 8192 rows belonging to the located uncompressed granule are then streamed into ClickHouse for further processing.


:::note Why Mark Files

Why does the primary index not directly contain the physical locations of the granules that are corresponding to index marks? 

Because at that very large scale that ClickHouse is designed for, it is important to be very disk and memory efficient.

The primary index file needs to fit into the main memory. 

For our example query ClickHouse used the primary index and selected a single granule that can possibly contain rows matching our query. Only for that one granule does ClickHouse then need the physical locations in order to stream the corresponding rows for further processing. 

Furthermore, this offset information is only needed for the UserID and URL columns. 

Offset information is not needed for columns that are not used in the query e.g. the EventTime.

For our sample query, Clickhouse needs only the two physical location offsets for granule 176 in the UserID data file (UserID.bin) and the two physical location offsets for granule 176 in the URL data file (URL.data). 

The indirection provided by mark files avoids storing, directly within the primary index, entries for the physical locations of all 1083 granules for all three columns: thus avoiding having unnecessary (potentially unused) data in main memory.
:::

The following diagram and the text below illustrates how for our example query ClickHouse locates granule 176 in the UserID.bin data file.

<img src={require('./images/sparse-primary-indexes-06.png').default} class="image"/>

We discussed earlier in this article that ClickHouse selected the primary index mark 176 and therefore granule 176 as possibly containing matching rows for our query. 

ClickHouse now uses the selected mark number (176) from the index for a positional array lookup in the UserID.mrk mark file in order to get the two offsets for locating granule 176. 

As shown, the first offset is locating the compressed file block within the UserID.bin data file that in turn contains the compressed version of granule 176.
 
Once the located file block is uncompressed into the main memory, the second offset from the mark file can be used to locate granule 176 within the uncompressed data.  

ClickHouse needs to locate (and stream all values from) granule 176 from both the UserID.bin data file and the URL.bin data file in order to execute our example query (top 10 most clicked urls for the internet user with the UserID 749.927.693). 

The diagram above shows how ClickHouse is locating the granule for the UserID.bin data file. 

In parallel, ClickHouse is doing the same for granule 176 for the URL.bin data file. The two respective granules are aligned and streamed into the ClickHouse engine for further processing i.e. aggregating and counting the URL values per group for all rows where the UserID is 749.927.693, before finally outputting the 10 largest URL groups in descending count order.




## <a name="filtering-on-key-columns-after-the-first"></a>Performance issues when filtering on key columns after the first


When a query is filtering on a column that is part of a compound key and is the first key column, then ClickHouse is running the binary search algorithm over the key column's index marks.

But what happens when a query is filtering on a column that is part of a compound key, but is not the first key column? 

:::note
We discuss a scenario when a query is explicitly not filtering on the first key colum, but on any key column after the first. 

When a query is filtering on both the first key column and on any key column(s) after the first then ClickHouse is running binary search over the first key column's index marks.  
:::

<br/>
<br/>

<a name="query-on-url"></a>
We use a query that calculates the top 10 users that have most frequently clicked on the URL "http://public_search":

```sql
SELECT UserID, count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```

The response is: <a name="query-on-url-slow"></a>
```response 
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.086 sec.
// highlight-next-line  
Processed 8.81 million rows, 
799.69 MB (102.11 million rows/s., 9.27 GB/s.)
``` 

The client output indicates that ClickHouse almost executed a full table scan despite the URL column being part of the compound primary key! ClickHouse reads 8.81 million rows from the 8.87 million rows of the table.

If <a href="https://clickhouse.com/docs/en/operations/server-configuration-parameters/settings/#server_configuration_parameters-logger" target="_blank">trace logging</a> is enabled then the ClickHouse server log file shows that ClickHouse used a <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444" target="_blank">generic exclusion search</a> over the 1083 URL index marks in order to identify those granules that possibly can contain rows with a URL column value of "http://public_search":
```response 
...Executor): Key condition: (column 1 in ['http://public_search', 
                                           'http://public_search'])
// highlight-next-line 
...Executor): Used generic exclusion search over index for part all_1_9_2 
              with 1537 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line 
              1076/1083 marks by primary key, 1076 marks to read from 5 ranges
...Executor): Reading approx. 8814592 rows with 10 streams
``` 
We can see in the sample trace log above, that 1076 (via the marks) out of 1083 granules were selected as possibly containing rows with a matching URL value.  

This results in 8.81 million rows being streamed into the ClickHouse engine (in parallel by using 10 streams), in order to identify the rows that are actually contain the URL value "http://public_search".

However, [as we will see later](#query-on-url-fast) only 39 granules out of that selected 1076 granules actually contain matching rows.

Whilst the primary index based on the compound primary key (UserID, URL) was very useful for speeding up queries filtering for rows with a specific UserID value, the index is not providing significant help with speeding up the query that filters for rows with a specific URL value. 

The reason for this is that the URL column is not the first key column and therefore ClickHouse is using a generic exclusion search algorithm (instead of binary search) over the URL column's index marks, and **the effectiveness of that algorithm is dependant on the cardinality difference** between the URL column and it's predecessor key column UserID.

In order to illustrate that, we give some details about how the generic exclusion search works.

<details open>
    <summary><font color="black">
    <a name="generic-exclusion-search-algorithm"></a>Generic exclusion search algorithm details 
    </font></summary>
    <p><font color="black">




The following is illustrating how the <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L14444" target="_blank" ><font color="blue">ClickHouse generic exclusion search algorithm</font></a> works when granules are selected via any column after the first, when the predecessor key column has a low(er) or high(er) cardinality.

As an example for both cases we will assume:
- a query that is searching for rows with URL value = "W3".
- an abstract version of our hits table with simplified values for UserID and URL.
- the same compound primary key (UserID, URL) for the index. This means rows are first ordered by UserID values. Rows with the same UserID value are then ordered by URL.
- a granule size of two i.e. each granule contains two rows.

We have marked the minimum key column values for each granule in orange in the diagrams below..

**Predecessor key column has low(er) cardinality**<a name="generic-exclusion-search-fast"></a>

Suppose UserID had low cardinality. In this case it would be likely that the same UserID value is spread over multiple table rows and granules and therefore index marks. For index marks with the same UserID, the URL values for the index marks are sorted in ascending order (because the table rows are ordered first by UserID and then by URL). This allows efficient filtering as described below:
<img src={require('./images/sparse-primary-indexes-07.png').default} class="image"/>

There are three different scenarios for the granule selection process for our abstract sample data in the diagram above:


1.  Index mark 0 for which the (minimum) **URL value is smaller than W3 and for which the URL value of the directly succeeding index mark is also smaller than W3** can be excluded because mark 0, 1, and 2 have the same UserID value. Note that this exclusion-precondition ensures that granule 0 and the next granule 1 are completely composed of U1 UserID values so that ClickHouse can assume that also the maximum URL value in granule 0 is smaller than W3 and exclude the granule.

2. Index mark 1 for which the **URL value is smaller (or equal) than W3 and for which the URL value of the directly succeeding index mark is greater (or equal) than W3** is selected because it means that granule 1 can possibly contain rows with URL W3).

3. Index marks 2 and 3 for which the **URL value is greater than W3** can be excluded, since index marks of a primary index store the minimum key column values for each granule and therefore granule 2 and 3 can't possibly contain URL value W3.

 

**Predecessor key column has high(er) cardinality**<a name="generic-exclusion-search-slow"></a>

When the UserID has high cardinality then it is unlikely that the same UserID value is spread over multiple table rows and granules. This means the URL values for the index marks are not monotonically increasing:

<img src={require('./images/sparse-primary-indexes-08.png').default} class="image"/>


As we can see in the diagram above, all shown marks whose URL values are smaller than W3 are getting selected for streaming its associated granule's rows into the ClickHouse engine.

This is because whilst all index marks in the diagram fall into scenario 1 described above, they do not satisfy the mentioned exclusion-precondition that *the two directly succeeding index marks both have the same UserID value as the current mark* and thus can’t be excluded.

For example, consider index mark 0 for which the **URL value is smaller than W3 and for which the URL value of the directly succeeding index mark is also smaller than W3**. This can *not* be excluded because the two directly succeeding index marks 1 and 2 do *not* have the same UserID value as the current mark 0.

Note the requirement for the two succeeding index marks to have the same UserID value. This ensures that the granules for the current and the next mark are completely composed of U1 UserID values. If only the next mark had the same UserID, the URL value of the next mark could potentially stem from a table row with a different UserID - which is indeed the case when you look at the diagram above i.e. W2 stems from a row with U2 not U1.

This ultimately prevents ClickHouse from making assumptions about the maximum URL value in granule 0. Instead it has to assume that granule 0 potentially contains rows with URL value W3 and is forced to select mark 0.


<br/>


The same scenario is true for mark 1, 2, and 3.


</font></p>
</details>

:::note Conclusion
The <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1444" target="_blank"><font color="blue">generic exclusion search algorithm</font></a> that ClickHouse is using instead of the <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank"><font color="blue">binary search algorithm</font></a> when a query is filtering on a column that is part of a compound key, but is not the first key column is most effective when the predecessor key column has low(er) cardinality.
:::

In our sample data set both key columns (UserID, URL) have similar high cardinality, and, as explained, the generic exclusion search algorithm is not very effective when the predecessor key column of the URL column has a high(er) or similar cardinality.

:::note note about data skipping index
Because of the similarly high cardinality of UserID and URL, our [<font color="blue">query filtering on URL</font>](#query-on-url) also wouldn't benefit much from creating a [<font color="blue">secondary data skipping index</font>](./skipping-indexes.md) on the URL column
of our [<font color="blue">table with compound primary key (UserID, URL)</font>](#original-table).

For example this two statements create and populate a <a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#primary-keys-and-indexes-in-queries" target="_blank"><font color="blue">minmax</font></a> data skipping index on the URL column of our table:
```sql
ALTER TABLE hits_UserID_URL ADD INDEX url_skipping_index URL TYPE minmax GRANULARITY 4;
ALTER TABLE hits_UserID_URL MATERIALIZE INDEX url_skipping_index;
```
ClickHouse now created an additional index that is storing - per group of 4 consecutive [<font color="blue">granules</font>](#granules) (note the <font face = "monospace">GRANULARITY 4</font> clause in the <font face = "monospace">ALTER TABLE</font> statement above) - the minimum and maximum URL value:

<img src={require('./images/sparse-primary-indexes-13a.png').default} class="image"/>

The first index entry (‘mark 0’ in the diagram above) is storing the minimum and maximum URL values for the [<font color="blue">rows belonging to the first 4 granules of our table</font>](#granules).

The second index entry (‘mark 1’) is storing the minimum and maximum URL values for the rows belonging to the next 4 granules of our table, and so on.

(ClickHouse also created a special [<font color="blue">mark file</font>](#mark-files) for to the data skipping index for [<font color="blue">locating</font>](#mark-files) the groups of granules associated with the index marks.)


Because of the similarly high cardinality of UserID and URL, this secondary data skipping index can't help with excluding granules from being selected when our [<font color="blue">query filtering on URL</font>](#query-on-url) is executed.

The specific URL value that the query is looking for (i.e. 'http://public_search') very likely is between the minimum and maximum value stored by the index for each group of granules resulting in ClickHouse being forced to select the group of granules (because they might contain row(s) matching the query). 






:::


As a consequence, if we want to significantly speed up our sample query that filters for rows with a specific URL then we need to use a primary index optimized to that query.

If in addition we want to keep the good performance of our sample query that filters for rows with a specific UserID then we need to use multiple primary indexes.

The following is showing ways for achieving that.

## <a name="multiple-primary-indexes"></a>Performance tuning with multiple primary indexes


If we want to significantly speed up both of our sample queries - the one that  filters for rows with a specific UserID and the one that filters for rows with a specific URL - then we need to use multiple primary indexes by using one if these three options:

- Creating a **second table** with a different primary key.
- Creating a **materialized view** on our existing table.
- Adding a **projection** to our existing table.

All three options will effectively duplicate our sample data into a additional table in order to reorganize the table primary index and row sort order. 

However, the three options differ in how transparent that additional table is to the user with respect to the routing of queries and insert statements.

When creating a **second table** with a different primary key then queries must be explicitly send to the table version best suited for the query, and new data must be inserted explicitly into both tables in order to keep the tables in sync:
<img src={require('./images/sparse-primary-indexes-09a.png').default} class="image"/>


With a **materialized view** the additional table is hidden and data is automatically kept in sync between both tables:
<img src={require('./images/sparse-primary-indexes-09b.png').default} class="image"/>


And the **projection** is the most transparent option because next to automatically keeping the hidden additional table in sync with data changes, ClickHouse will automatically chose the most effective table version for queries:
<img src={require('./images/sparse-primary-indexes-09c.png').default} class="image"/>

In the following we discuss this three options for creating and using multiple primary indexes in more detail and with real examples.

## <a name="multiple-primary-indexes-via-secondary-tables"></a>Multiple primary indexes via secondary tables

<a name="secondary-table"></a>
We are creating a new additional table where we switch the order of the key columns (compared to our original table) in the primary key:

```sql
CREATE TABLE hits_URL_UserID
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
// highlight-next-line  
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
SETTINGS index_granularity = 8192, index_granularity_bytes = 0;
```

Insert all 8.87 million rows from our [original table](#original-table) into the additional table:

```sql
INSERT INTO hits_URL_UserID 
SELECT * from hits_UserID_URL;
```
 
The response looks like:

```response
Ok.

0 rows in set. Elapsed: 2.898 sec. Processed 8.87 million rows, 838.84 MB (3.06 million rows/s., 289.46 MB/s.)
```

And finally optimize the table:
```sql
OPTIMIZE TABLE hits_URL_UserID FINAL;
```

Because we switched the order of the columns in the primary key, the inserted rows are now stored on disk in a different lexicographical order (compared to our [original table](#original-table)) and therefore also the 1083 granules of that table are containing different values than before:
<img src={require('./images/sparse-primary-indexes-10.png').default} class="image"/>

This is the resulting primary key:
<img src={require('./images/sparse-primary-indexes-11.png').default} class="image"/>

That can now be used to significantly speed up the execution of our example query filtering on the URL column in order to calculate the top 10 users that most frequently clicked on the URL "http://public_search":
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```
 
The response is:
<a name="query-on-url-fast"></a>

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.017 sec.
// highlight-next-line 
Processed 319.49 thousand rows, 
11.38 MB (18.41 million rows/s., 655.75 MB/s.)
```

Now, instead of [almost doing a full table scan](#filtering-on-key-columns-after-the-first), ClickHouse executed that query much more effective.

With the primary index from the [original table](#original-table) where UserID was the first, and URL the second key column, ClickHouse used a [generic exclusion search](#generic-exclusion-search-algorithm) over the index marks for executing that query and that was not very effective because of the similarly high cardinality of UserID and URL.

With URL as the first column in the primary index, ClickHouse is now running <a href="https://github.com/ClickHouse/ClickHouse/blob/22.3/src/Storages/MergeTree/MergeTreeDataSelectExecutor.cpp#L1452" target="_blank">binary search</a> over the index marks. 
The corresponding trace log in the ClickHouse server log file confirms that:
```response
...Executor): Key condition: (column 0 in ['http://public_search', 
                                           'http://public_search'])
// highlight-next-line                                           
...Executor): Running binary search on index range for part all_1_9_2 (1083 marks)
...Executor): Found (LEFT) boundary mark: 644
...Executor): Found (RIGHT) boundary mark: 683
...Executor): Found continuous range in 19 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line 
              39/1083 marks by primary key, 39 marks to read from 1 ranges
...Executor): Reading approx. 319488 rows with 2 streams
```
ClickHouse selected only 39 index marks, instead of 1076 when generic exclusion search was used.


Note that the additional table is optimized for speeding up the execution of our example query filtering on URLs.


Similar to the [bad performance](#query-on-url-slow) of that query with our [original table](#original-table), our [example query filtering on UserIDs](#query-on-userid) will not run very effectively with the new additional table, because UserID is now the second key column in the primary index of that table and therefore ClickHouse will use generic exclusion search for granule selection, which is [not very effective for similarly high cardinality](#generic-exclusion-search-slow) of UserID and URL.
Open the details box for specifics.
<details>
    <summary><font color="black">
    Query filtering on UserIDs now has bad performance<a name="query-on-userid-slow"></a>
    </font></summary>
    <p><font color="black">

```sql
SELECT URL, count(URL) AS Count
FROM hits_URL_UserID
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
```
 
The response is:

```response
┌─URL────────────────────────────┬─Count─┐
│ http://auto.ru/chatay-barana.. │   170 │
│ http://auto.ru/chatay-id=371...│    52 │
│ http://public_search           │    45 │
│ http://kovrik-medvedevushku-...│    36 │
│ http://forumal                 │    33 │
│ http://korablitz.ru/L_1OFFER...│    14 │
│ http://auto.ru/chatay-id=371...│    14 │
│ http://auto.ru/chatay-john-D...│    13 │
│ http://auto.ru/chatay-john-D...│    10 │
│ http://wot/html?page/23600_m...│     9 │
└────────────────────────────────┴───────┘

10 rows in set. Elapsed: 0.024 sec.
// highlight-next-line  
Processed 8.02 million rows, 
73.04 MB (340.26 million rows/s., 3.10 GB/s.)
```

Server Log:
```response
...Executor): Key condition: (column 1 in [749927693, 749927693])
// highlight-next-line
...Executor): Used generic exclusion search over index for part all_1_9_2 
              with 1453 steps
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line 
              980/1083 marks by primary key, 980 marks to read from 23 ranges
...Executor): Reading approx. 8028160 rows with 10 streams
```
</font></p>
</details>




We now have two tables. Optimized for speeding up queries filtering on UserIDs, and speeding up queries filtering on URLs, respectively:
<img src={require('./images/sparse-primary-indexes-12a.png').default} class="image"/>









## Multiple primary indexes via materialized views

Create a <a href="https://clickhouse.com/docs/en/sql-reference/statements/create/view/#materialized" target="_blank">materialized view</a> on our existing table.
```sql
CREATE MATERIALIZED VIEW mv_hits_URL_UserID
ENGINE = MergeTree()
PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime)
POPULATE
AS SELECT * FROM hits_UserID_URL;
```
 
The response looks like:

```response
Ok.

0 rows in set. Elapsed: 2.935 sec. Processed 8.87 million rows, 838.84 MB (3.02 million rows/s., 285.84 MB/s.)
```

:::note
- we switch the order of the key columns (compared to our [<font color="blue">original table</font>](#original-table) ) in the view's primary key
- the materialzed view is backed by a **hidden table** whose row order and primary index is based on the given primary key definition
- we use the <font face = "monospace">POPULATE</font> keyword in order to immediately populate the hidden table with all 8.87 million rows from the source table [<font color="blue">hits_UserID_URL</font>](#original-table) 
- if new rows are inserted into the source table hits_UserID_URL, then that rows are automatically also inserted into the hidden table 
- Effectively the implicitly created hidden table has the same row order and primary index as the [<font color="blue">secondary table that we created explicitly</font>](#multiple-primary-indexes-via-secondary-tables):




<img src={require('./images/sparse-primary-indexes-12b-1.png').default} class="image"/>


ClickHouse is storing the [<font color="blue">column data files</font>](#data-storage) (*.bin), the [<font color="blue">mark files</font>](#mark-files) (*.mrk2) and the [<font color="blue">primary index</font>](#primary-index) (primary.idx) of the hidden table in a special folder withing the ClickHouse server's data directory:


<img src={require('./images/sparse-primary-indexes-12b-2.png').default} class="image"/>

:::

  
The hidden table (and it's primary index) backing the materialized view can now be used to significantly speed up the execution of our example query filtering on the URL column:
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM mv_hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```
 
The response is:

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.026 sec.
// highlight-next-line 
Processed 335.87 thousand rows, 
13.54 MB (12.91 million rows/s., 520.38 MB/s.)
```

Because effectively the hidden table (and it's primary index) backing the materialized view is identical to the [<font color="blue">secondary table that we created explicitly</font>](#multiple-primary-indexes-via-secondary-tables), the query is executed in the same effective way as with the explicitly created table.

The corresponding trace log in the ClickHouse server log file confirms that ClickHouse is running binary search over the index marks:

```response
...Executor): Key condition: (column 0 in ['http://public_search', 
                                           'http://public_search'])
// highlight-next-line
...Executor): Running binary search on index range ...
...
...Executor): Selected 4/4 parts by partition key, 4 parts by primary key,
// highlight-next-line 
              41/1083 marks by primary key, 41 marks to read from 4 ranges
...Executor): Reading approx. 335872 rows with 4 streams
```



## Multiple primary indexes via projections


<a href="https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#projections" target="_blank">Projections</a> are an experimental feature at the moment, therefore we need to tell ClickHouse that we know what we are doing first:

```sql
SET allow_experimental_projection_optimization = 1;
```


Create a projection on our existing table:
```sql
ALTER TABLE hits_UserID_URL
    ADD PROJECTION prj_url_userid
    (
        SELECT *
        ORDER BY (URL, UserID)
    );
```

And materialize the projection:
```sql
ALTER TABLE hits_UserID_URL
    MATERIALIZE PROJECTION prj_url_userid;
```

:::note
- the projection is creating a **hidden table** whose row order and primary index is based on the given <font face = "monospace">ORDER BY</font> clause of the projection
- we use the <font face = "monospace">MATERIALIZE</font> keyword in order to immediately populate the hidden table with all 8.87 million rows from the source table [<font color="blue">hits_UserID_URL</font>](#original-table)
- if new rows are inserted into the source table hits_UserID_URL, then that rows are automatically also inserted into the hidden table
- a query is always (syntactically) targeting the source table hits_UserID_URL, but if the row order and primary index of the hidden table allows a more effective query execution, then that hidden table will be used instead
- Effectively the implicitly created hidden table has the same row order and primary index as the [<font color="blue">secondary table that we created explicitly</font>](#multiple-primary-indexes-via-secondary-tables):

<img src={require('./images/sparse-primary-indexes-12c-1.png').default} class="image"/>

ClickHouse is storing the [<font color="blue">column data files</font>](#data-storage) (*.bin), the [<font color="blue">mark files</font>](#mark-files) (*.mrk2) and the [<font color="blue">primary index</font>](#primary-index) (primary.idx) of the hidden table in a special folder (marked in orange in the screenshot below) next to the source table's data files, mark files, and primary index files:

<img src={require('./images/sparse-primary-indexes-12c-2.png').default} class="image"/>
:::


The hidden table (and it's primary index) created by the projection can now be (implicitly) used to significantly speed up the execution of our example query filtering on the URL column. Note that the query is syntactically targeting the source table of the projection.
```sql
SELECT UserID, count(UserID) AS Count
// highlight-next-line
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
```
 
The response is:

```response
┌─────UserID─┬─Count─┐
│ 2459550954 │  3741 │
│ 1084649151 │  2484 │
│  723361875 │   729 │
│ 3087145896 │   695 │
│ 2754931092 │   672 │
│ 1509037307 │   582 │
│ 3085460200 │   573 │
│ 2454360090 │   556 │
│ 3884990840 │   539 │
│  765730816 │   536 │
└────────────┴───────┘

10 rows in set. Elapsed: 0.029 sec. 
// highlight-next-line 
Processed 319.49 thousand rows, 1
1.38 MB (11.05 million rows/s., 393.58 MB/s.)
```

Because effectively the hidden table (and it's primary index) created by the projection is identical to the [<font color="blue">secondary table that we created explicitly</font>](#multiple-primary-indexes-via-secondary-tables), the query is executed in the same effective way as with the explicitly created table.

The corresponding trace log in the ClickHouse server log file confirms that ClickHouse is running binary search over the index marks:


```response
...Executor): Key condition: (column 0 in ['http://public_search', 
                                           'http://public_search'])
// highlight-next-line                                           
...Executor): Running binary search on index range for part prj_url_userid (1083 marks)
...Executor): ...
// highlight-next-line
...Executor): Choose complete Normal projection prj_url_userid
...Executor): projection required columns: URL, UserID
...Executor): Selected 1/1 parts by partition key, 1 parts by primary key,
// highlight-next-line 
              39/1083 marks by primary key, 39 marks to read from 1 ranges
...Executor): Reading approx. 319488 rows with 2 streams
```


## Removing inefficient key columns


The primary index of our [table with compound primary key (UserID, URL)](#original-table) was very useful for speeding up a [query filtering on UserID](#query-on-userid). But that index is not providing significant help with speeding up a [query filtering on URL](#query-on-url), despite the URL column being part of the compound primary key.

And vice versa:
The primary index of our [table with compound primary key (URL, UserID)](#secondary-table) was speeding up a [query filtering on URL](#query-on-url), but didn't provide much support for a [query filtering on UserID](#query-on-userid).

Because of the similarly high cardinality of the primary key columns UserID and URL, a query that filters on the second key column [doesn’t benefit much from the second key column being in the index](#generic-exclusion-search-slow).

Therefore it makes sense to remove the second key column from the primary index (resulting in less memory consumption of the index) and to [use multiple primary indexes](#multiple-primary-indexes) instead.


However if the key columns in a compound primary key have big differences in cardinality, then it is [beneficial for queries](#generic-exclusion-search-fast) to order the primary key columns by cardinality in ascending order.

The higher the cardinality difference between the key columns is, the more the order of those columns in the key matters. We will demonstrate that in a future article. Stay tuned.

