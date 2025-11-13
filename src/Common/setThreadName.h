#pragma once
#include <string_view>
#include <string>

namespace DB
{

#define THREAD_NAME_VALUES(M)\
    M(AGGREGATED_ZOOKEEPER_LOG, "AggrZooLog") \
    M(AGGREGATOR_DESTRUCTION, "AggregDestruct") \
    M(AGGREGATOR_POOL, "AggregatorPool") \
    M(ARROW_FILE, "ArrowFile") \
    M(ARROW_FLIGHT, "ArrowFlight") \
    M(ARROW_FLIGHT_EXPR, "ArrowFlightExpr") \
    M(ARROW_FLIGHT_SERVER, "ArrowFlightSrv") \
    M(ASYNC_COPY, "AsyncCopy") \
    M(ASYNC_INSERT_QUEUE, "AsyncInsertQue") \
    M(ASYNC_LOGGER, "AsyncLogger") \
    M(ASYNC_METRICS, "AsyncMetrics") \
    M(ASYNC_TABLE_LOADER, "AsyncTblLoader") \
    M(ASYNC_TEXT_LOG, "AsyncTextLog") \
    M(AZURE_BACKUP_READER, "BackupRDAzure") \
    M(AZURE_BACKUP_WRITER, "BackupWRAzure") \
    M(AZURE_COPY_POOL, "AzureObjCopy") \
    M(AZURE_LIST_POOL, "AzureObjList") \
    M(BACKGROUND_BUFFER_FLUSH_SCHEDULE_POOL, "BgBufSchPool") \
    M(BACKGROUND_SCHEDULE_POOL, "BgSchPool") \
    M(BACKUP_ASYNC, "BackupAsync") \
    M(BACKUP_ASYNC_INTERNAL, "BackupAsyncInt") \
    M(BACKUP_COLLECTOR, "BackupCollect") \
    M(BACKUP_COORDINATION, "BackupCoord") \
    M(BACKUP_COORDINATION_INTERNAL, "BackupCoordInt") \
    M(BACKUP_WORKER, "BackupWorker") \
    M(CACHE_DICTIONARY_UPDATE_QUEUE, "UpdQueue") \
    M(CGROUP_MEMORY_OBSERVER, "CgrpMemUsgObsr") \
    M(CLICKHOUSE_WATCH, "ClickHouseWatch") \
    M(CLUSTER_DISCOVERY, "ClusterDiscover") \
    M(COMPLETED_PIPELINE_EXECUTOR, "QueryCompPipeEx") \
    M(CONFIG_RELOADER, "ConfigReloader") \
    M(CONCURRENT_JOIN, "ConcurrentJoin") \
    M(CREATE_TABLES, "CreateTables") \
    M(CUSTOM_RESOURCE_MANAGER, "CustomResMgr") \
    M(DATABASE_BACKUP, "DatabaseBackup") \
    M(DATABASE_ON_DISK, "DatabaseOnDisk") \
    M(DATABASE_REPLICAS, "DBReplicas") \
    M(DATALAKE_REST_CATALOG, "RestCatalog") \
    M(DATALAKE_TABLE_SNAPSHOT, "TableSnapshot") \
    M(DDL_WORKER, "DDLWorker") \
    M(DDL_WORKER_CLEANUP, "DDLWorkerClnup") \
    M(DDL_WORKER_EXECUTER, "DDLWorkerExec") \
    M(DEFAULT_THREAD_POOL, "ThreadPool") \
    M(DETACHED_PARTS_BYTES, "DP_BytesOnDisk") \
    M(DICT_RELOAD, "DictReload") \
    M(DISTRIBUTED_FLUSH, "DistFlush") \
    M(DISTRIBUTED_INIT, "DistInit") \
    M(DISTRIBUTED_SCHEDULE_POOL, "BgDistSchPool") \
    M(DISTRIBUTED_SINK, "DistrOutStrProc") \
    M(DROP_TABLES, "DropTables") \
    M(DWARF_DECODER, "DWARFDecoder") \
    M(ERROR_LOG, "ErrorLog") \
    M(EXTERNAL_LOADER, "ExternalLoader") \
    M(GRPC_SERVER_CALL, "gRPCServerCall") \
    M(GRPC_SERVER_QUEUE, "gRPCServerQueue") \
    M(HASHED_DICT_DTOR, "HashedDictDtor") \
    M(HASHED_DICT_LOAD, "HashedDictLoad") \
    M(HTTP_HANDLER, "HTTPHandler") \
    M(INTERSERVER_HANDLER, "IntersrvHandler") \
    M(IO_URING_MONITOR, "IoUringMonitr") \
    M(KEEPER_HANDLER, "KeeperHandler") \
    M(KEEPER_REQUEST, "KeeperRequest") \
    M(KEEPER_RESPONSE, "KeeperResponse") \
    M(KEEPER_SNAPSHOT, "KeeperSnapshot") \
    M(KEEPER_SNAPSHOT_S3, "KeeperSnapS3") \
    M(KAFKA_BACKGROUND, "KafkaBackgrd") \
    M(KAFKA_BROKER, "KafkaBroker") \
    M(KAFKA_CLEANUP, "KafkaClnup") \
    M(KAFKA_MAIN, "KafkaMain") \
    M(LOAD_MARKS, "LoadMarksThread") \
    M(LOCAL_SERVER_PTY, "LocalServerPty") \
    M(MEMORY_WORKER, "MemoryWorker") \
    M(MERGE_MUTATE, "MergeMutate") \
    M(MERGETREE_COMMON, "Common") \
    M(MERGETREE_FETCH, "Fetch") \
    M(MERGETREE_FETCH_PARTITION, "FetchPartition") \
    M(MERGETREE_FREEZE_PART, "FreezePart") \
    M(MERGETREE_INDEX, "MergeTreeIndex") \
    M(MERGETREE_LOAD_ACTIVE_PARTS, "ActiveParts") \
    M(MERGETREE_LOAD_OUTDATED_PARTS, "OutdatedParts") \
    M(MERGETREE_LOAD_UNEXPECTED_PARTS, "UnexpectedParts") \
    M(MERGETREE_MOVE, "Move") \
    M(MERGETREE_PARTS_CLEANUP, "PartsCleaning") \
    M(MERGETREE_PREWARM_CACHE, "PrewarmCaches") \
    M(MERGETREE_READ, "MergeTreeRead") \
    M(MERGETREE_VECTOR_SIM_INDEX, "VectorSimIndex") \
    M(METRIC_LOG, "MetricLog") \
    M(METRICS_TRANSMITTER, "MetricsTransmtr") \
    M(MSG_BROKER_SCHEDULE_POOL, "BgMBSchPool") \
    M(MYSQL_DATABASE_CLEANUP, "MySQLDBCleaner") \
    M(MYSQL_HANDLER, "MySQLHandler") \
    M(OBJECT_STORAGE_SHUTDOWN, "ObjStorShutdwn") \
    M(ORC_FILE, "ORCFile") \
    M(PARALLEL_COMPRESSORS_POOL, "ParallelCompres") \
    M(PARALLEL_FORMATER, "Formatter") \
    M(PARALLEL_FORMATER_COLLECTOR, "Collector") \
    M(PARALLEL_FORMATER_PARSER, "ChunkParser") \
    M(PARALLEL_PARSING_SEGMENTATOR, "Segmentator") \
    M(PARALLEL_READ, "ParallelRead") \
    M(PARALLEL_WITH_QUERY, "ParallelWithQry") \
    M(PARQUET_DECODER, "ParquetDecoder") \
    M(PARQUET_ENCODER, "ParquetEncoder") \
    M(PARQUET_PREFETCH, "ParquetPrefetch") \
    M(PLAIN_REWRITABLE_META_LOAD, "PlainRWMetaLoad") \
    M(POLYGON_DICT_LOAD, "PolygonDict") \
    M(POOL_DELAYED_EXECUTION, "PoolDelayExec") \
    M(POSTGRES_HANDLER, "PostgresHandler") \
    M(PREFIX_READER, "PrefixReader") \
    M(PREFETCH_READER, "ReadPrepare") \
    M(PROMETHEUS_HANDLER, "PrometheusHndlr") \
    M(PULLING_ASYNC_EXECUTOR, "QueryPullPipeEx") \
    M(PUSHING_ASYNC_EXECUTOR, "QueryPushPipeEx") \
    M(PRETTY_WRITER, "PrettyWriter") \
    M(QUERY_ASYNC_EXECUTOR, "QueryPipelineEx") \
    M(READER_POOL, "Reader") \
    M(READ_TASK_ITERATOR, "ReadTaskIteratr") \
    M(READ_THREAD_POOL, "ThreadPoolRead") \
    M(REMOTE_FS_READ_THREAD_POOL, "VFSRead") \
    M(REMOTE_FS_WRITE_THREAD_POOL, "VFSWrite") \
    M(RESTORE_COORDINATION, "RestoreCoord") \
    M(RESTORE_COORDINATION_INTERNAL, "RestoreCoordInt") \
    M(RESTORE_FIND_TABLE, "Restore_FindTbl") \
    M(RESTORE_MAKE_DATABASE, "Restore_MakeDB") \
    M(RESTORE_MAKE_TABLE, "Restore_MakeTbl") \
    M(RESTORE_TABLE_DATA, "Restore_TblData") \
    M(RESTORE_TABLE_TASK, "Restore_TblTask") \
    M(RUNTIME_DATA, "RuntimeData") \
    M(S3_BACKUP_READER, "BackupReaderS3") \
    M(S3_BACKUP_WRITER, "BackupWriterS3") \
    M(S3_COPY_POOL, "S3ObjStor_copy") \
    M(S3_LIST_POOL, "ListObjectS3") \
    M(SESSION_CLEANUP, "SessionCleanup") \
    M(SEND_TO_SHELL_CMD, "SendToShellCmd") \
    M(SUGGEST, "Suggest") \
    M(SYSTEM_LOG_FLUSH, "SystemLogFlush") \
    M(SYSTEM_REPLICAS, "SysReplicas") \
    M(TCP_HANDLER, "TCPHandler") \
    M(TEST_KEEPER_PROC, "TestKeeperProc") \
    M(TEST_SCHEDULER, "TestScheduler") \
    M(TRACE_COLLECTOR, "TraceCollector") \
    M(TRANSPOSED_METRIC_LOG, "TMetricLog") \
    M(TRUNCATE_TABLE, "TruncTbls") \
    M(UNIQ_EXACT_CONVERT, "UniqExaConvert") \
    M(UNIQ_EXACT_MERGER, "UniqExactMerger") \
    M(USER_DEFINED_WATCH, "UserDefWatch") \
    M(WORKLOAD_ENTRY_WATCH, "WrkldEntWatch") \
    M(WORKLOAD_RESOURCE_MANAGER, "WorkloadResMgr") \
    M(ZOOKEEPER_ACL_WATCHER, "ZooACLWatch") \
    M(ZOOKEEPER_RECV, "ZooKeeperRecv") \
    M(ZOOKEEPER_SEND, "ZooKeeperSend") \


enum class ThreadName : uint8_t
{
    UNKNOWN = 0,

    #define THREAD_NAME_ACTION(NAME, STR) NAME,
    THREAD_NAME_VALUES(THREAD_NAME_ACTION)
    #undef THREAD_NAME_ACTION
};


/** Sets the thread name (maximum length is 15 bytes),
  *  which will be visible in ps, gdb, /proc,
  *  for convenience of observation and debugging.
  *
  */
void setThreadName(ThreadName name);
ThreadName getThreadName();

std::string_view toString(ThreadName name);
}
