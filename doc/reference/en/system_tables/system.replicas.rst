system.replicas
---------------

Contains information and status for replicated tables residing on the local server. This table can be used for monitoring. The table contains a row for every Replicated* table.

Example:

.. code-block:: text

  SELECT *
  FROM system.replicas
  WHERE table = 'visits'
  FORMAT Vertical

  Row 1:
  ──────
  database:           merge
  table:              visits
  engine:             ReplicatedCollapsingMergeTree
  is_leader:          1
  is_readonly:        0
  is_session_expired: 0
  future_parts:       1
  parts_to_check:     0
  zookeeper_path:     /clickhouse/tables/01-06/visits
  replica_name:       example01-06-1.yandex.ru
  replica_path:       /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
  columns_version:    9
  queue_size:         1
  inserts_in_queue:   0
  merges_in_queue:    1
  log_max_index:      596273
  log_pointer:        596274
  total_replicas:     2
  active_replicas:    2

Columns:

.. code-block:: text

  database:          Database name.
  table:              Table name.
  engine:             Table engine name.

  is_leader:          Whether the replica is the leader.
  Only one replica can be the leader at a time. The leader is responsible for selecting background merges to perform.
  Note that writes can be performed to any replica that is available and has a session in ZK, regardless of whether it is a leader.

  is_readonly:        Whether the replica is in read-only mode.
  This mode is turned on if the config doesn't have sections with ZK, if an unknown error occurred when reinitializing sessions in ZK, and during session reinitialization in ZK.

  is_session_expired: Whether the session with ZK has expired.
  Basically the same as 'is_readonly'.

  future_parts:       The number of data parts that will appear as the result of INSERTs or merges that haven't been done yet.

  parts_to_check:     The number of data parts in the queue for verification.
  A part is put in the verification queue if there is suspicion that it might be damaged.

  zookeeper_path:     Path to table data in ZK.
  replica_name:       Replica name in ZK. Different replicas of the same table have different names.
  replica_path:      Path to replica data in ZK. The same as concatenating 'zookeeper_path/replicas/replica_path'.

  columns_version:    Version number of the table structure. Indicates how many times ALTER was performed. If replicas have different versions, it means some replicas haven't made all of the ALTERs yet.

  queue_size:         Size of the queue for operations waiting to be performed. Operations include inserting blocks of data, merges, and certain other actions. It usually coincides with 'future_parts'.

  inserts_in_queue:   Number of inserts of blocks of data that need to be made. Insertions are usually replicated fairly quickly. If this number is large, it means something is wrong.

  merges_in_queue:    The number of merges waiting to be made. Sometimes merges are lengthy, so this value may be greater than one for a long time.

  The next 4 columns have a non-zero value only where there is an active session with ZK.

  log_max_index:      Maximum entry number in the log of general activity.
  log_pointer:        Maximum entry number from the log of general activity that the replica copied to its queue for execution, plus one.
  If log_pointer is much smaller than log_max_index, something is wrong.

  total_replicas:     The total number of known replicas of this table.
  active_replicas:    The number of replicas of this table that have a session in ZK (i.e., the number of functioning replicas).к

If you request all the columns, the table may work a bit slowly, since several reads from ZK are made for each row.
If you don't request the last 4 columns (log_max_index, log_pointer, total_replicas, active_replicas), the table works quickly.

For example, you can check that everything is working correctly like this:

.. code-block:: sql

  SELECT
      database,
      table,
      is_leader,
      is_readonly,
      is_session_expired,
      future_parts,
      parts_to_check,
      columns_version,
      queue_size,
      inserts_in_queue,
      merges_in_queue,
      log_max_index,
      log_pointer,
      total_replicas,
      active_replicas
  FROM system.replicas
  WHERE
         is_readonly
      OR is_session_expired
      OR future_parts > 20
      OR parts_to_check > 10
      OR queue_size > 20
      OR inserts_in_queue > 10
      OR log_max_index - log_pointer > 10
      OR total_replicas < 2
      OR active_replicas < total_replicas

If this query doesn't return anything, it means that everything is fine.
