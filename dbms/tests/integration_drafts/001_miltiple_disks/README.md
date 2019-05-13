## Test stand for multiple disks feature 

Currently for namual tests, can be easily scripted to be the part of inntergration tests. 

To run you need to have docker & docker-compose. 

```
(Check makefile)
make run
make ch1_shell 
 > clickhouse-client

make logs # Ctrl+C
make cleup
```

### basic

* allows to configure multiple disks & folumes & shemas
* clickhouse check that all disks are write-accessible
* clickhouse can create a table with provided schema

### one volume-one disk custom schema

* clickhouse puts data to correct folder when schema is used
* clickhouse can do merges / detach / attach / freeze on that folder

### one volume-multiple disks schema (JBOD scenario)

* clickhouse uses round-robin to place new parts  
* clickhouse can do merges / detach / attach / freeze on that folder

### two volumes-one disk per volume (fast expensive / slow cheap storage)

* clickhouse uses round-robin to place new parts  
* clickhouse can do merges / detach / attach / freeze on that folder
* clickhouse put parts to different volumes depending on part size

### use 'default' schema for tables created without schema provided.


# ReplicatedMergeTree  

....

For all above: 
clickhouse respect free space limitation setting.
ClickHouse writes important disk-related information to logs. 

## Queries

```
CREATE TABLE schema_default                    (id UInt64) Engine=MergeTree() ORDER BY (id);

 INSERT INTO schema_default SELECT * FROM numbers(1);
CREATE TABLE schema_default_explicit           (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_schema_name='default';
CREATE TABLE schema_default_disk_with_external (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_schema_name='default_disk_with_external';
CREATE TABLE schema_jbod_with_external         (id UInt64) Engine=MergeTree() ORDER BY (id) SETTINGS storage_schema_name='jbod_with_external';

CREATE TABLE replicated_schema_default                    (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id);
CREATE TABLE replicated_schema_default_explicit           (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_schema_name='default';
CREATE TABLE replicated_schema_default_disk_with_external (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_schema_name='default_disk_with_external';
CREATE TABLE replicated_schema_jbod_with_external         (id UInt64) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', '{replica}') ORDER BY (id) SETTINGS storage_schema_name='jbod_with_external';
```


## Extra acceptance criterias

* hardlinks problems. Thouse stetements should be able to work properly (or give a proper feedback) on multidisk scenarios  
  * ALTER TABLE ... UPDATE
  * ALTER TABLE ... TABLE
  * ALTER TABLE ... MODIFY COLUMN ... 
  * ALTER TABLE ... CLEAR COLUMN 
  * ALTER TABLE ... REPLACE PARTITION ... 
* Maintainance - system tables show proper values: 
  * system.parts
  * system.tables
  * system.part_log (target disk?)
* New system table 
  * system.volumes
  * system.disks
  * system.schemas  
* chown / create needed disk folders in docker
