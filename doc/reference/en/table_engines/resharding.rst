Resharding
----------

.. code-block:: sql

  ALTER TABLE t RESHARD [COPY] [PARTITION partition] TO cluster description USING sharding key

Query works only for Replicated tables and for Distributed tables that are looking at Replicated tables.

When executing, query first checks correctness of query, sufficiency of free space on nodes and writes to ZooKeeper at some path a task to to. Next work is done asynchronously.

For using resharding, you must specify path in ZooKeeper for task queue in configuration file:

.. code-block:: xml

  <resharding>
  	<task_queue_path>/clickhouse/task_queue</task_queue_path>
  </resharding>

When running ``ALTER TABLE t RESHARD`` query, node in ZooKeeper is created if not exists.

Cluster description is list of shards with weights to distribute the data.
Shard is specified as address of table in ZooKeeper. Example: /clickhouse/tables/01-03/hits
Relative weight of shard (optional, default is 1) could be specified after WEIGHT keyword.
Example:

.. code-block:: sql

  ALTER TABLE merge.hits
  RESHARD PARTITION 201501
  TO
  	'/clickhouse/tables/01-01/hits' WEIGHT 1,
  	'/clickhouse/tables/01-02/hits' WEIGHT 2,
  	'/clickhouse/tables/01-03/hits' WEIGHT 1,
  	'/clickhouse/tables/01-04/hits' WEIGHT 1
  USING UserID

Sharding key (``UserID`` in example) has same semantic as for Distributed tables. You could specify ``rand()`` as sharding key for random distribution of data.

When query is run, it checks:
 * identity of table structure on all shards.
 * availability of free space on local node in amount of partition size in bytes, with additional 10% reserve.
 * availability of free space on all replicas of all specified shards, except local replica, if exists, in amount of patition size times ratio of shard weight to total weight of all shards, with additional 10% reserve.

Next, asynchronous processing of query is of following steps:
 #. Split patition to parts on local node.
	It merges all parts forming a partition and in the same time, splits them to several, according sharding key.
	Result is placed to /reshard directory in table data directory.
	Source parts doesn't modified and all process doesn't intervent table working data set.

 #. Copying all parts to remote nodes (to each replica of corresponding shard).

 #. Execution of queries ``ALTER TABLE t DROP PARTITION`` on local node and ``ALTER TABLE t ATTACH PARTITION`` on all shards.
	Note: this operation is not atomic. There are time point when user could see absence of data.

	When ``COPY`` keyword is specified, source data is not removed. It is suitable for copying data from one cluster to another with changing sharding scheme in same time.

 #. Removing temporary data from local node.

When having multiple resharding queries, their tasks will be done sequentially.

Query in example is to reshard single partition.
If you don't specify partition in query, then tasks to reshard all partitions will be created. Example:

.. code-block:: sql
  
  ALTER TABLE merge.hits
  RESHARD
  TO ...

When resharding Distributed tables, each shard will be resharded (corresponding query is sent to each shard).

You could reshard Distributed table to itself or to another table.

Resharding is intended for "old" data: in case when during job, resharded partition was modified, task for that partition will be cancelled.

On each server, resharding is done in single thread. It is doing that way to not disturb normal query processing.

As of June 2016, resharding is in "beta" state: it was tested only for small data sets - up to 5 TB.
