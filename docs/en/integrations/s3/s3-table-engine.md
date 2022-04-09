---
sidebar_label: S3 Table Engine
sidebar_position: 3
description: Users may naturally wish to treat an S3 bucket as a table, utilizing this within existing queries. To address this, ClickHouse provides the S3 table engine.
---

# S3 Table Engines

While the `s3` functions allow ad-hoc queries to be performed on data stored in S3, they are syntactically verbose for more complex queries. Users may naturally wish to treat an S3 bucket as a table, utilizing this within existing queries. To address this, ClickHouse provides the S3 table engine.

Creating tables backed by this engine uses the DDL syntax shown below:

```sql
CREATE TABLE s3_engine_table (name String, value UInt32)
    ENGINE = S3(path, [aws_access_key_id, aws_secret_access_key,] format, [compression])
    [SETTINGS ...]
```

where,


* path — Bucket URL with a path to the file. Supports following wildcards in read-only mode: *, ?, {abc,def} and {N..M} where N, M — numbers, 'abc', 'def' — strings. For more information, see [here](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3/#wildcards-in-path).
* format — The[ format](https://clickhouse.com/docs/en/interfaces/formats/#formats) of the file.
* aws_access_key_id, aws_secret_access_key - Long-term credentials for the AWS account user. You can use these to authenticate your requests. The parameter is optional. If credentials are not specified, configuration file values are used. For more information, see [Managing credentials](#managing-credentials).
* compression — Compression type. Supported values: none, gzip/gz, brotli/br, xz/LZMA, zstd/zst. The parameter is optional. By default, it will autodetect compression by file extension.

## Reading Data

In the following example, we create a table trips_raw using the first ten tsv files located within the [nyc-taxi](https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/) bucket. Each of these contains 1m rows each.


```sql
CREATE TABLE trips_raw
(
   `trip_id`               UInt32,
   `vendor_id`             Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
   `pickup_date`           Date,
   `pickup_datetime`       DateTime,
   `dropoff_date`          Date,
   `dropoff_datetime`      DateTime,
   `store_and_fwd_flag`    UInt8,
   `rate_code_id`          UInt8,
   `pickup_longitude`      Float64,
   `pickup_latitude`       Float64,
   `dropoff_longitude`     Float64,
   `dropoff_latitude`      Float64,
   `passenger_count`       UInt8,
   `trip_distance`         Float64,
   `fare_amount`           Float32,
   `extra`                 Float32,
   `mta_tax`               Float32,
   `tip_amount`            Float32,
   `tolls_amount`          Float32,
   `ehail_fee`             Float32,
   `improvement_surcharge` Float32,
   `total_amount`          Float32,
   `payment_type_`         Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
   `trip_type`             UInt8,
   `pickup`                FixedString(25),
   `dropoff`               FixedString(25),
   `cab_type`              Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
   `pickup_nyct2010_gid`   Int8,
   `pickup_ctlabel`        Float32,
   `pickup_borocode`       Int8,
   `pickup_ct2010`         String,
   `pickup_boroct2010`     FixedString(7),
   `pickup_cdeligibil`     String,
   `pickup_ntacode`        FixedString(4),
   `pickup_ntaname`        String,
   `pickup_puma`           UInt16,
   `dropoff_nyct2010_gid`  UInt8,
   `dropoff_ctlabel`       Float32,
   `dropoff_borocode`      UInt8,
   `dropoff_ct2010`        String,
   `dropoff_boroct2010`    FixedString(7),
   `dropoff_cdeligibil`    String,
   `dropoff_ntacode`       FixedString(4),
   `dropoff_ntaname`       String,
   `dropoff_puma`          UInt16
) ENGINE = S3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..9}.tsv.gz', 'TabSeparatedWithNames', 'gzip');
```

Notice the use of the {0..9} pattern to limit to the first ten files.

Once created, we can query this table like any other e.g.

| \_path | \_file | trip\_id | pickup\_date | total\_amount |
| :--- | :--- | :--- | :--- | :--- |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999902 | 2015-07-07 | 19.56 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999919 | 2015-07-07 | 10.3 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999944 | 2015-07-07 | 24.3 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999969 | 2015-07-07 | 9.95 |
| datasets-documentation/nyc-taxi/trips\_0.gz | trips\_0.gz | 1199999990 | 2015-07-08 | 9.8 |


```sql
SELECT payment_type, max(tip_amount) as max_tip FROM trips_raw GROUP BY payment_type;
```

| payment\_type | max\_tip |
| :--- | :--- |
| UNK | 0 |
| CSH | 800 |
| CRE | 53.06 |
| NOC | 100 |
| DIS | 100 |


## Inserting Data

Whilst this table engine supports parallel reads, writes are only supported if the table definition does not contain glob patterns. The above table, therefore, would block writes. 

To illustrate writes, create the following table:

```sql
CREATE TABLE trips_dest
(
   `trip_id`               UInt32,
   `pickup_date`           Date,
   `pickup_datetime`       DateTime,
   `dropoff_datetime`      DateTime,
   `tip_amount`            Float32,
   `total_amount`          Float32
) ENGINE = S3('<bucket path>/trips.bin', 'Native');
```

*This query requires write access to the bucket*
```sql
INSERT INTO trips_dest SELECT trip_id, pickup_date, pickup_datetime, dropoff_datetime, tip_amount, total_amount FROM trips LIMIT 10;
```

```sql
SELECT * FROM trips_dest LIMIT 5;
```

| trip\_id | pickup\_date | pickup\_datetime | dropoff\_datetime | tip\_amount | total\_amount |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 14 | 2013-08-02 | 2013-08-02 09:43:58 | 2013-08-02 09:44:13 | 0 | 2 |
| 15 | 2013-08-02 | 2013-08-02 09:44:43 | 2013-08-02 09:45:15 | 0 | 2 |
| 21 | 2013-08-02 | 2013-08-02 11:30:00 | 2013-08-02 17:08:00 | 0 | 172 |
| 21 | 2013-08-02 | 2013-08-02 12:30:00 | 2013-08-02 18:08:00 | 0 | 172 |
| 23 | 2013-08-02 | 2013-08-02 18:00:50 | 2013-08-02 18:01:55 | 0 | 6.5 |

Note that rows can only be inserted into new files. There are no merge cycles or file split operations. Once a file is written, subsequent inserts will fail. Users have two options here:

* Specify the setting `s3_create_new_file_on_insert=1`. This will cause the creation of new files on each insert. A numeric suffix will be appended to the end of each file that will monotonically increase for each insert operation. For the above example, a subsequent insert would cause the creation of a trips_1.bin file.
* Specify the setting `s3_truncate_on_insert=1`. This will cause a truncation of the file, i.e. it will only contain the newly inserted rows once complete.

Both of these settings default to 0 - thus forcing the user to set one of them. s3_truncate_on_insert will take precedence if both are set.

## Miscellaneous


Unlike a traditional merge tree family table, dropping an s3 table will not delete the underlying data. 

Full settings for this table type can be found [here](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3/#settings).

Be aware of the following caveats when using this engine:

* ALTER queries are not supported
* SAMPLE operations are not supported
* There is no notion of indexes, i.e. primary or skip.

## Managing Credentials

In the previous examples, we have passed credentials in the s3 function or table definition. Whilst this may be acceptable for occasional usage, users require less explicit authentication mechanisms in production. To address this, ClickHouse has several options:

In the previous examples, we have passed credentials in the s3 function or table definition. Whilst this may be acceptable for occasional usage, users require less explicit authentication mechanisms in production. To address this, ClickHouse has several options:

* Specify the connection details in the config.xml or an equivalent configuration file under conf.d. The contents of an example file are shown below, assuming installation using the debian package.

    ```xml
    ubuntu@single-node-clickhouse:/etc/clickhouse-server/config.d$ cat s3.xml 
    <clickhouse>
        <s3>
            <endpoint-name>
                <endpoint>https://dalem-files.s3.amazonaws.com/test/</endpoint>
                <access_key_id>key</access_key_id>
                <secret_access_key>secret</secret_access_key>
                <!-- <use_environment_credentials>false</use_environment_credentials> -->	
                <!-- <header>Authorization: Bearer SOME-TOKEN</header> -->
            </endpoint-name>
        </s3>
    </clickhouse>
    ```

    These credentials will be used for any requests where the endpoint above is an exact prefix match for the requested URL. Also, note the ability in this example to declare an authorization header as an alternative to access and secret keys. A complete list of supported settings can be found [here](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3/#settings).



* The example above highlights the availability of the configuration parameter use_environment_credentials. This configuration parameter can also be set globally at the s3 level i.e. 

    ```xml
    <clickhouse>
        <s3>
        <use_environment_credentials>true</use_environment_credentials>
        <s3>
    </clickhouse>
    ```

    This setting turns on an attempt to retrieve s3 credentials from the environment, thus allowing access through IAM roles. Specifically, the following order of retrieval is performed:



    * A lookup for the environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN
    * Check performed in $HOME/.aws
    * Temporary credentials obtained via the AWS Security Token Service - i.e. vi[a AssumeRole](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html) API
    * Checks for credentials in the ECS environment variables AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or AWS_CONTAINER_CREDENTIALS_FULL_URI and AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN.
    * Obtains the credentials via [Amazon EC2 instance metadata](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-metadata.html) provided [AWS_EC2_METADATA_DISABLED](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html#envvars-list-AWS_EC2_METADATA_DISABLED) is not set to true.


These same settings can also be set for a specific endpoint, using the same prefix matching rule.
