---
slug: /ja/changelogs/24.5
title: v24.5 Changelog for Cloud
description: Fast release changelog for v24.5
keywords: [chaneglog, cloud]
---

# v24.5 Changelog for Cloud

Relevant changes for ClickHouse Cloud services based on the v24.5 release.

## Breaking Changes

* Change the column name from duration_ms to duration_microseconds in the system.zookeeper table to reflect the reality that the duration is in the microsecond resolution. [#60774](https://github.com/ClickHouse/ClickHouse/pull/60774) (Duc Canh Le).

* Don't allow to set max_parallel_replicas to 0 as it doesn't make sense. Setting it to 0 could lead to unexpected logical errors. Closes #60140. [#61201](https://github.com/ClickHouse/ClickHouse/pull/61201) (Kruglov Pavel).

* Remove support for INSERT WATCH query (part of the experimental LIVE VIEW feature). [#62382](https://github.com/ClickHouse/ClickHouse/pull/62382) (Alexey Milovidov).

* Usage of functions neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference deprecated (because it is error-prone). Proper window functions should be used instead. To enable them back, set allow_deprecated_error_prone_window_functions=1. [#63132](https://github.com/ClickHouse/ClickHouse/pull/63132) (Nikita Taranov).


## Backward Incompatible Changes

* In the new ClickHouse version, the functions geoDistance, greatCircleDistance, and greatCircleAngle will use 64-bit double precision floating point data type for internal calculations and return type if all the arguments are Float64. This closes #58476. In previous versions, the function always used Float32. You can switch to the old behavior by setting geo_distance_returns_float64_on_float64_arguments to false or setting compatibility to 24.2 or earlier. [#61848](https://github.com/ClickHouse/ClickHouse/pull/61848) (Alexey Milovidov).

* Queries from system.columns will work faster if there is a large number of columns, but many databases or tables are not granted for SHOW TABLES. Note that in previous versions, if you grant SHOW COLUMNS to individual columns without granting SHOW TABLES to the corresponding tables, the system.columns table will show these columns, but in a new version, it will skip the table entirely. Remove trace log messages "Access granted" and "Access denied" that slowed down queries. [#63439](https://github.com/ClickHouse/ClickHouse/pull/63439) (Alexey Milovidov).

* Fix crash in largestTriangleThreeBuckets. This changes the behaviour of this function and makes it to ignore NaNs in the series provided. Thus the resultset might differ from previous versions. [#62646](https://github.com/ClickHouse/ClickHouse/pull/62646) (Raúl Marín).

## New Features

* The new analyzer is enabled by default on new services. 

* Supports dropping multiple tables at the same time like drop table a,b,c;. [#58705](https://github.com/ClickHouse/ClickHouse/pull/58705) (zhongyuankai).

* User can now parse CRLF with TSV format using a setting input_format_tsv_crlf_end_of_line. Closes #56257. [#59747](https://github.com/ClickHouse/ClickHouse/pull/59747) (Shaun Struwig).

* Table engine is grantable now, and it won't affect existing users behavior. [#60117](https://github.com/ClickHouse/ClickHouse/pull/60117) (jsc0218).

* Adds the Form Format to read/write a single record in the application/x-www-form-urlencoded format. [#60199](https://github.com/ClickHouse/ClickHouse/pull/60199) (Shaun Struwig).

* Added possibility to compress in CROSS JOIN. [#60459](https://github.com/ClickHouse/ClickHouse/pull/60459) (p1rattttt).

* New setting input_format_force_null_for_omitted_fields that forces NULL values for omitted fields. [#60887](https://github.com/ClickHouse/ClickHouse/pull/60887) (Constantine Peresypkin).

* Support join with inequal conditions which involve columns from both left and right table. e.g. `t1.y < t2.y`. To enable, SET allow_experimental_join_condition = 1. [#60920](https://github.com/ClickHouse/ClickHouse/pull/60920) (lgbo).

* Add a new function, getClientHTTPHeader. This closes #54665. Co-authored with @lingtaolf. [#61820](https://github.com/ClickHouse/ClickHouse/pull/61820) (Alexey Milovidov).

* For convenience purpose, SELECT * FROM numbers() will work in the same way as SELECT * FROM system.numbers - without a limit. [#61969](https://github.com/ClickHouse/ClickHouse/pull/61969) (YenchangChan).

* Modifying memory table settings through ALTER MODIFY SETTING is now supported. ALTER TABLE memory MODIFY SETTING min_rows_to_keep = 100, max_rows_to_keep = 1000;. [#62039](https://github.com/ClickHouse/ClickHouse/pull/62039) (zhongyuankai).

* Analyzer support recursive CTEs. [#62074](https://github.com/ClickHouse/ClickHouse/pull/62074) (Maksim Kita).

* Earlier our s3 storage and s3 table function didn't support selecting from archive files. I created a solution that allows to iterate over files inside archives in S3. [#62259](https://github.com/ClickHouse/ClickHouse/pull/62259) (Daniil Ivanik).

* Support for conditional function clamp. [#62377](https://github.com/ClickHouse/ClickHouse/pull/62377) (skyoct).

* Add npy output format. [#62430](https://github.com/ClickHouse/ClickHouse/pull/62430) (豪肥肥).

* Analyzer support QUALIFY clause. Closes #47819. [#62619](https://github.com/ClickHouse/ClickHouse/pull/62619) (Maksim Kita).

* Added role query parameter to the HTTP interface. It works similarly to SET ROLE x, applying the role before the statement is executed. This allows for overcoming the limitation of the HTTP interface, as multiple statements are not allowed, and it is not possible to send both SET ROLE x and the statement itself at the same time. It is possible to set multiple roles that way, e.g., ?role=x&role=y, which will be an equivalent of SET ROLE x, y. [#62669](https://github.com/ClickHouse/ClickHouse/pull/62669) (Serge Klochkov).

* Add SYSTEM UNLOAD PRIMARY KEY. [#62738](https://github.com/ClickHouse/ClickHouse/pull/62738) (Pablo Marcos).

* Added SQL functions generateUUIDv7, generateUUIDv7ThreadMonotonic, generateUUIDv7NonMonotonic (with different monotonicity/performance trade-offs) to generate version 7 UUIDs aka. timestamp-based UUIDs with random component. Also added a new function UUIDToNum to extract bytes from a UUID and a new function UUIDv7ToDateTime to extract timestamp component from a UUID version 7. [#62852](https://github.com/ClickHouse/ClickHouse/pull/62852) (Alexey Petrunyaka).

* Raw as a synonym for TSVRaw. [#63394](https://github.com/ClickHouse/ClickHouse/pull/63394) (Unalian).

* Added possibility to do cross join in temporary file if size exceeds limits. [#63432](https://github.com/ClickHouse/ClickHouse/pull/63432) (p1rattttt).

## Performance Improvements

* Skip merging of newly created projection blocks during INSERT-s. [#59405](https://github.com/ClickHouse/ClickHouse/pull/59405) (Nikita Taranov).

* Reduce overhead of the mutations for SELECTs (v2). [#60856](https://github.com/ClickHouse/ClickHouse/pull/60856) (Azat Khuzhin).

* JOIN filter push down improvements using equivalent sets. [#61216](https://github.com/ClickHouse/ClickHouse/pull/61216) (Maksim Kita).

* Add a new analyzer pass to optimize in single value. [#61564](https://github.com/ClickHouse/ClickHouse/pull/61564) (LiuNeng).

* Process string functions XXXUTF8 'asciily' if input strings are all ASCII chars. Inspired by apache/doris#29799. Overall speed up by 1.07x~1.62x. Notice that peak memory usage had been decreased in some cases. [#61632](https://github.com/ClickHouse/ClickHouse/pull/61632) (李扬).

* Enabled fast Parquet encoder by default (output_format_parquet_use_custom_encoder). [#62088](https://github.com/ClickHouse/ClickHouse/pull/62088) (Michael Kolupaev).

* Improve JSONEachRowRowInputFormat by skipping all remaining fields when all required fields are read. [#62210](https://github.com/ClickHouse/ClickHouse/pull/62210) (lgbo).

* Functions splitByChar and splitByRegexp were speed up significantly. [#62392](https://github.com/ClickHouse/ClickHouse/pull/62392) (李扬).

* Improve trivial insert select from files in file/s3/hdfs/url/... table functions. Add separate max_parsing_threads setting to control the number of threads used in parallel parsing. [#62404](https://github.com/ClickHouse/ClickHouse/pull/62404) (Kruglov Pavel).

* Support parallel write buffer for AzureBlobStorage managed by setting azure_allow_parallel_part_upload. [#62534](https://github.com/ClickHouse/ClickHouse/pull/62534) (SmitaRKulkarni).

* Functions to_utc_timestamp and from_utc_timestamp are now about 2x faster. [#62583](https://github.com/ClickHouse/ClickHouse/pull/62583) (KevinyhZou).

* Functions parseDateTimeOrNull, parseDateTimeOrZero, parseDateTimeInJodaSyntaxOrNull and parseDateTimeInJodaSyntaxOrZero now run significantly faster (10x - 1000x) when the input contains mostly non-parseable values. [#62634](https://github.com/ClickHouse/ClickHouse/pull/62634) (LiuNeng).

* Change HostResolver behavior on fail to keep only one record per IP [#62652](https://github.com/ClickHouse/ClickHouse/pull/62652) (Anton Ivashkin).

* Add a new configurationprefer_merge_sort_block_bytes to control the memory usage and speed up sorting 2 times when merging when there are many columns. [#62904](https://github.com/ClickHouse/ClickHouse/pull/62904) (LiuNeng).

* QueryPlan convert OUTER JOIN to INNER JOIN optimization if filter after JOIN always filters default values. Optimization can be controlled with setting query_plan_convert_outer_join_to_inner_join, enabled by default. [#62907](https://github.com/ClickHouse/ClickHouse/pull/62907) (Maksim Kita).

* Enable optimize_rewrite_sum_if_to_count_if by default. [#62929](https://github.com/ClickHouse/ClickHouse/pull/62929) (Raúl Marín).

* Micro-optimizations for the new analyzer. [#63429](https://github.com/ClickHouse/ClickHouse/pull/63429) (Raúl Marín).

* Index analysis will work if DateTime is compared to DateTime64. This closes #63441. [#63443](https://github.com/ClickHouse/ClickHouse/pull/63443) (Alexey Milovidov).

* Speed up indices of type set a little (around 1.5 times) by removing garbage. [#64098](https://github.com/ClickHouse/ClickHouse/pull/64098) (Alexey Milovidov).

# Improvements

* Remove optimize_monotonous_functions_in_order_by setting this is becoming a no-op. [#63004](https://github.com/ClickHouse/ClickHouse/pull/63004) (Raúl Marín).

* Maps can now have Float32, Float64, Array(T), Map(K,V) and Tuple(T1, T2, ...) as keys. Closes #54537. [#59318](https://github.com/ClickHouse/ClickHouse/pull/59318) (李扬).

* Add asynchronous WriteBuffer for AzureBlobStorage similar to S3. [#59929](https://github.com/ClickHouse/ClickHouse/pull/59929) (SmitaRKulkarni).

* Multiline strings with border preservation and column width change. [#59940](https://github.com/ClickHouse/ClickHouse/pull/59940) (Volodyachan).

* Make RabbitMQ nack broken messages. Closes #45350. [#60312](https://github.com/ClickHouse/ClickHouse/pull/60312) (Kseniia Sumarokova).

* Add a setting first_day_of_week which affects the first day of the week considered by functions toStartOfInterval(..., INTERVAL ... WEEK). This allows for consistency with function toStartOfWeek which defaults to Sunday as the first day of the week. [#60598](https://github.com/ClickHouse/ClickHouse/pull/60598) (Jordi Villar).

* Added persistent virtual column _block_offset which stores original number of row in block that was assigned at insert. Persistence of column _block_offset can be enabled by setting enable_block_offset_column. Added virtual column_part_data_version which contains either min block number or mutation version of part. Persistent virtual column _block_number is not considered experimental anymore. [#60676](https://github.com/ClickHouse/ClickHouse/pull/60676) (Anton Popov).

* Functions date_diff and age now calculate their result at nanosecond instead of microsecond precision. They now also offer nanosecond (or nanoseconds or ns) as a possible value for the unit parameter. [#61409](https://github.com/ClickHouse/ClickHouse/pull/61409) (Austin Kothig).

* Now marks are not loaded for wide parts during merges. [#61551](https://github.com/ClickHouse/ClickHouse/pull/61551) (Anton Popov).

* Enable output_format_pretty_row_numbers by default. It is better for usability. [#61791](https://github.com/ClickHouse/ClickHouse/pull/61791) (Alexey Milovidov).

* The progress bar will work for trivial queries with LIMIT from system.zeros, system.zeros_mt (it already works for system.numbers and system.numbers_mt), and the generateRandom table function. As a bonus, if the total number of records is greater than the max_rows_to_read limit, it will throw an exception earlier. This closes #58183. [#61823](https://github.com/ClickHouse/ClickHouse/pull/61823) (Alexey Milovidov).

* Add TRUNCATE ALL TABLES. [#61862](https://github.com/ClickHouse/ClickHouse/pull/61862) (豪肥肥).

* Add a setting input_format_json_throw_on_bad_escape_sequence, disabling it allows saving bad escape sequences in JSON input formats. [#61889](https://github.com/ClickHouse/ClickHouse/pull/61889) (Kruglov Pavel).

* Fixed grammar from "a" to "the" in the warning message. There is only one Atomic engine, so it should be "to the new Atomic engine" instead of "to a new Atomic engine". [#61952](https://github.com/ClickHouse/ClickHouse/pull/61952) (shabroo).

* Fix logical-error when undoing quorum insert transaction. [#61953](https://github.com/ClickHouse/ClickHouse/pull/61953) (Han Fei).

* Automatically infer Nullable column types from Apache Arrow schema. [#61984](https://github.com/ClickHouse/ClickHouse/pull/61984) (Maksim Kita).

* Allow to cancel parallel merge of aggregate states during aggregation. Example: uniqExact. [#61992](https://github.com/ClickHouse/ClickHouse/pull/61992) (Maksim Kita).

* Dictionary source with INVALIDATE_QUERY is not reloaded twice on startup. [#62050](https://github.com/ClickHouse/ClickHouse/pull/62050) (vdimir).

* OPTIMIZE FINAL for ReplicatedMergeTree now will wait for currently active merges to finish and then reattempt to schedule a final merge. This will put it more in line with ordinary MergeTree behaviour. [#62067](https://github.com/ClickHouse/ClickHouse/pull/62067) (Nikita Taranov).

* While read data from a hive text file, it would use the first line of hive text file to resize of number of input fields, and sometimes the fields number of first line is not matched with the hive table defined , such as the hive table is defined to have 3 columns, like test_tbl(a Int32, b Int32, c Int32), but the first line of text file only has 2 fields, and in this suitation, the input fields will be resized to 2, and if the next line of the text file has 3 fields, then the third field can not be read but set a default value 0, which is not right. [#62086](https://github.com/ClickHouse/ClickHouse/pull/62086) (KevinyhZou).

* The syntax highlighting while typing in the client will work on the syntax level (previously, it worked on the lexer level). [#62123](https://github.com/ClickHouse/ClickHouse/pull/62123) (Alexey Milovidov).

* Fix an issue where when a redundant = 1 or = 0 is added after a boolean expression involving the primary key, the primary index is not used. For example, both `SELECT * FROM <table> WHERE <primary-key> IN (<value>) = 1` and `SELECT * FROM <table> WHERE <primary-key> NOT IN (<value>) = 0` will both perform a full table scan, when the primary index can be used. [#62142](https://github.com/ClickHouse/ClickHouse/pull/62142) (josh-hildred).

* Added setting lightweight_deletes_sync (default value: 2 - wait all replicas synchronously). It is similar to setting mutations_sync but affects only behaviour of lightweight deletes. [#62195](https://github.com/ClickHouse/ClickHouse/pull/62195) (Anton Popov).

* Distinguish booleans and integers while parsing values for custom settings: SET custom_a = true; SET custom_b = 1;. [#62206](https://github.com/ClickHouse/ClickHouse/pull/62206) (Vitaly Baranov).

* Support S3 access through AWS Private Link Interface endpoints. Closes #60021, #31074 and #53761. [#62208](https://github.com/ClickHouse/ClickHouse/pull/62208) (Arthur Passos).

* Client has to send header 'Keep-Alive: timeout=X' to the server. If a client receives a response from the server with that header, client has to use the value from the server. Also for a client it is better not to use a connection which is nearly expired in order to avoid connection close race. [#62249](https://github.com/ClickHouse/ClickHouse/pull/62249) (Sema Checherinda).

* Added nano- micro- milliseconds unit for date_trunc. [#62335](https://github.com/ClickHouse/ClickHouse/pull/62335) (Misz606).

* The query cache now no longer caches results of queries against system tables (system.*, information_schema.*, INFORMATION_SCHEMA.*). [#62376](https://github.com/ClickHouse/ClickHouse/pull/62376) (Robert Schulze).

* MOVE PARTITION TO TABLE query can be delayed or can throw TOO_MANY_PARTS exception to avoid exceeding limits on the part count. The same settings and limits are applied as for theINSERT query (see max_parts_in_total, parts_to_delay_insert, parts_to_throw_insert, inactive_parts_to_throw_insert, inactive_parts_to_delay_insert, max_avg_part_size_for_too_many_parts, min_delay_to_insert_ms and max_delay_to_insert settings). [#62420](https://github.com/ClickHouse/ClickHouse/pull/62420) (Sergei Trifonov).

* Make transform always return the first match. [#62518](https://github.com/ClickHouse/ClickHouse/pull/62518) (Raúl Marín).

* Avoid evaluating table DEFAULT expressions while executing RESTORE. [#62601](https://github.com/ClickHouse/ClickHouse/pull/62601) (Vitaly Baranov).

* Allow quota key with different auth scheme in HTTP requests. [#62842](https://github.com/ClickHouse/ClickHouse/pull/62842) (Kseniia Sumarokova).

* Close session if user's valid_until is reached. [#63046](https://github.com/ClickHouse/ClickHouse/pull/63046) (Konstantin Bogdanov).
