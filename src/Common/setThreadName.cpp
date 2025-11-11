#include <unordered_map>
#include "config.h"

#include <pthread.h>

#if defined(OS_DARWIN) || defined(OS_SUNOS)
#elif defined(OS_FREEBSD)
#include <pthread_np.h>
#else
#include <sys/prctl.h>
#endif

#include <cstring>

#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/Jemalloc.h>

constexpr size_t THREAD_NAME_SIZE = 16;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PTHREAD_ERROR;
}

#define THREAD_NAMES_VALUES(M)\
    M(UNKNOWN, "Unknown") \
    M(DEFAULT_THREAD_POOL, "ThreadPool") \
    M(CGROUP_MEMORY_OBSERVER, "CgrpMemUsgObsr") \
    M(ASYNC_METRICS, "AsyncMetrics") \
    M(ASYNC_TABLE_LOADER, "AsyncTblLoader") \
    M(CUSTOM_RESOURCE_MANAGER, "CustomResMgr") \
    M(WORKLOAD_RESOURCE_MANAGER, "WorkloadResMgr") \
    M(TEST_SCHEDULER, "TestScheduler") \
    M(IO_URING_MONITOR, "IoUringMonitr") \
    M(KAFKA_MAIN, "KafkaMain") \
    M(KAFKA_BROKER, "KafkaBroker") \
    M(KAFKA_BACKGROUND, "KafkaBackgrd") \
    M(KAFKA_CLEANUP, "KafkaClnup") \
    M(MEMORY_WORKER, "MemoryWorker") \
    M(WORKLOAD_ENTRY_WATCH, "WrkldEntWatch") \
    M(PLAIN_REWRITABLE_META_LOAD, "PlainRWMetaLoad") \
    M(ZOOKEEPER_ACL_WATCHER, "ZooACLWatch") \
    M(DATABASE_ON_DISK, "DatabaseOnDisk") \
    M(MYSQL_DATABASE_CLEANUP, "MySQLDBCleaner") \
    M(DICT_RELOAD, "DictReload") \
    M(POOL_DELAYED_EXECUTION, "PoolDelayExec") \
    M(CONFIG_RELOADER, "ConfigReloader") \
    M(TEST_KEEPER_PROC, "TestKeeperProc") \
    M(ZOOKEEPER_SEND, "ZkSender") \
    M(ZOOKEEPER_RECV, "ZkReceiver") \
    M(KEEPER_REQUEST, "KeeperRequest") \
    M(KEEPER_RESPONSE, "KeeperResponse") \
    M(KEEPER_SNAPSHOT, "KeeperSnapshot") \
    M(KEEPER_SNAPSHOT_S3, "KeeperSnapS3") \
    M(CLICKHOUSE_WATCH, "ClickHouseWatch") \
    M(USER_DEFINED_WATCH, "UserDefWatch") \
    M(ASYNC_INSERT_QUEUE, "AsyncInsertQue") \
    M(CLUSTER_DISCOVERY, "ClusterDiscover") \
    M(DDL_WORKER_EXECUTER, "DDLWorkerExec") \
    M(DDL_WORKER, "DDLWorker") \
    M(EXTERNAL_LOADER, "ExternalLoader") \
    M(METRIC_LOG, "MetricLog") \
    M(TRANSPOSED_METRIC_LOG, "TMetricLog") \
    M(ERROR_LOG, "ErrorLog") \
    M(AGGREGATED_ZOOKEEPER_LOG, "AggrZooLog") \
    M(SESSION_CLEANUP, "SessionCleanup") \
    M(SYSTEM_LOG_FLUSH, "SystemLogFlush") \
    M(TRACE_COLLECTOR, "TraceCollector") \
    M(ASYNC_LOGGER, "AsyncLogger") \
    M(ASYNC_TEXT_LOG, "AsyncTextLog") \
    M(ARROW_FLIGHT_SERVER, "ArrowFlightSrv") \
    M(ARROW_FLIGHT_EXPR, "ArrowFlightExpr") \
    M(ARROW_FLIGHT, "ArrowFlight") \
    M(ARROW_FILE, "ArrowFile") \
    M(GRPC_SERVER_CALL, "gRPCServerCall") \
    M(GRPC_SERVER_QUEUE, "gRPCServerQueue") \
    M(HTTP_HANDLER, "HTTPHandler") \
    M(INTERSERVER_HANDLER, "IntersrvHandler") \
    M(KEEPER_HANDLER, "KeeperHandler") \
    M(MYSQL_HANDLER, "MySQLHandler") \
    M(POSTGRES_HANDLER, "PostgresHandler") \
    M(PROMETHEUS_HANDLER, "PrometheusHndlr") \
    M(TCP_HANDLER, "TCPHandler") \
    M(LOCAL_SERVER_PTY, "LocalServerPty") \
    M(BACKGROUND_SCHEDULE_POOL, "BgSchPool") \
    M(DISTRIBUTED_SCHEDULE_POOL, "BgDistSchPool") \
    M(MSG_BROKER_SCHEDULE_POOL, "BgMBSchPool") \
    M(DDL_WORKER_CLEANUP, "DDLWorkerClnup") \
    M(BACKGROUND_BUFFER_FLUSH_SCHEDULE_POOL, "BgBufSchPool") \
    M(READ_THREAD_POOL, "ThreadPoolRead") \
    M(REMOTE_FS_READ_THREAD_POOL, "VFSRead") \
    M(PARALLEL_COMPRESSORS_POOL, "ParallelCompres") \
    M(REMOTE_FS_WRITE_THREAD_POOL, "VFSWrite") \
    M(AZURE_COPY_POOL, "AzureObjCopy") \
    M(AZURE_LIST_POOL, "AzureObjList") \
    M(READER_POOL, "Reader") \
    M(READ_TASK_ITERATOR, "ReadTaskIteratr") \
    M(DATALAKE_TABLE_SNAPSHOT, "TableSnapshot") \
    M(S3_LIST_POOL, "ListObjectS3") \
    M(BACKUP_WORKER, "BackupWorker") \
    M(S3_COPY_POOL, "S3ObjStor_copy") \
    M(DATALAKE_REST_CATALOG, "RestCatalog") \
    M(AZURE_BACKUP_READER, "BackupRDAzure") \
    M(AZURE_BACKUP_WRITER, "BackupWRAzure") \
    M(S3_BACKUP_READER, "BackupReaderS3") \
    M(S3_BACKUP_WRITER, "BackupWriterS3") \
    M(ASYNC_COPY, "AsyncCopy") \
    M(BACKUP_COLLECTOR, "BackupCollect") \
    M(BACKUP_ASYNC_INTERNAL, "BackupAsyncInt") \
    M(BACKUP_ASYNC, "BackupAsync") \
    M(AGGREGATOR_DESTRUCTION, "AggregDestruct") \
    M(CREATE_TABLES, "CreateTables") \
    M(DATABASE_BACKUP, "DatabaseBackup") \
    M(PREFIX_READER, "PrefixReader") \
    M(BACKUP_COORDINATION_INTERNAL, "BackupCoordInt") \
    M(BACKUP_COORDINATION, "BackupCoord") \
    M(RESTORE_COORDINATION_INTERNAL, "RestoreCoordInt") \
    M(RESTORE_COORDINATION, "RestoreCoord") \
    M(RESTORE_FIND_TABLE, "Restore_FindTbl") \
    M(RESTORE_MAKE_DATABASE, "Restore_MakeDB") \
    M(RESTORE_TABLE_DATA, "Restore_TblData") \
    M(RESTORE_TABLE_TASK, "Restore_TblTask") \
    M(CONCURRENT_JOIN, "ConcurrentJoin") \
    M(DROP_TABLES, "DropTables") \
    M(AGGREGATOR_POOL, "AggregatorPool") \
    M(DISTRIBUTED_SINK, "DistrOutStrProc") \
    M(MERGE_MUTATE, "MergeMutate") \
    M(MERGETREE_MOVE, "Move") \
    M(MERGETREE_FETCH, "Fetch") \
    M(MERGETREE_COMMON, "Common") \
    M(PARALLEL_WITH_QUERY, "ParallelWithQry") \
    M(TRUNCATE_TABLE, "TruncTbls") \
    M(DETACHED_PARTS_BYTES, "DP_BytesOnDisk") \
    M(OBJECT_STORAGE_SHUTDOWN, "ObjStorShutdwn") \
    M(MERGETREE_VECTOR_SIM_INDEX, "VectorSimIndex") \
    M(MERGETREE_FREEZE_PART, "FreezePart") \
    M(MERGETREE_PARTS_CLEANUP, "PartsCleaning") \
    M(MERGETREE_PREWARM_CACHE, "PrewarmCaches") \
    M(MERGETREE_LOAD_OUTDATED_PARTS, "OutdatedParts") \
    M(MERGETREE_LOAD_UNEXPECTED_PARTS, "UnexpectedParts") \
    M(MERGETREE_LOAD_ACTIVE_PARTS, "ActiveParts") \
    M(MERGETREE_FETCH_PARTITION, "FetchPartition") \
    M(DISTRIBUTED_FLUSH, "DistFlush") \
    M(DISTRIBUTED_INIT, "DistInit") \
    M(DATABASE_REPLICAS, "DBReplicas") \
    M(SYSTEM_REPLICAS, "SysReplicas") \
    M(MERGETREE_INDEX, "MergeTreeIndex") \
    M(SEND_TO_SHELL_CMD, "SendToShellCmd") \
    M(PRETTY_WRITER, "PrettyWriter") \
    M(PARQUET_ENCODER, "ParquetEncoder") \
    M(PARQUET_DECODER, "ParquetDecoder") \
    M(PARALLEL_PARSING_SEGMENTATOR, "Segmentator") \
    M(PARALLEL_FORMATER, "Formatter") \
    M(PARALLEL_FORMATER_COLLECTOR, "Collector") \
    M(PUSHING_ASYNC_EXECUTOR, "QueryPushPipeEx") \
    M(PULLING_ASYNC_EXECUTOR, "QueryPullPipeEx") \
    M(QUERY_ASYNC_EXECUTOR, "QueryPipelineEx") \
    M(COMPLETED_PIPELINE_EXECUTOR, "QueryCompPipeEx") \
    M(HASHED_DICT_LOAD, "HashedDictLoad") \
    M(PREFETCH_READER, "ReadPrepare") \
    M(LOAD_MARKS, "LoadMarksThread") \
    M(PARALLEL_FORMATER_PARSER, "ChunkParser") \
    M(POLYGON_DICT_LOAD, "PolygonDict") \
    M(UNIQ_EXACT_CONVERT, "UniqExaConvert") \
    M(MERGETREE_READ, "MergeTreeRead") \
    M(RUNTIME_DATA, "RuntimeData") \
    M(ORC_FILE, "ORCFile") \
    M(PARQUET_PREFETCH, "ParquetPrefetch") \
    M(CACHE_DICTIONARY_UPDATE_QUEUE, "UpdQueue") \
    M(UNIQ_EXACT_MERGER, "UniqExactMerger") \
    M(SUGGEST, "Suggest") \
    M(PARALLEL_READ, "ParallelRead") \
    M(DWARF_DECODER, "DWARFDecoder") \
    M(RESTORE_MAKE_TABLE, "Restore_MakeTbl") \
    M(HASHED_DICT_DTOR, "HashedDictDtor") \
    M(METRICS_TRANSMITTER, "MetricsTransmtr") \


template<std::size_t N>
constexpr std::string as_string(char const (&s)[N])
{
    static_assert(N <= THREAD_NAME_SIZE, "Thread name too long");
    return std::string(s, N-1);
}

const static std::unordered_map<std::string, ThreadName> str_to_thread_name = []{
    std::unordered_map<std::string, ThreadName> result;

    #define ACTION(NAME, STR) result[as_string(STR)] = ThreadName::NAME;
    THREAD_NAMES_VALUES(ACTION)
    #undef ACTION

    return result;
}();

/// Cache thread_name to avoid prctl(PR_GET_NAME) for query_log/text_log
static thread_local ThreadName thread_name = ThreadName::UNKNOWN;

std::string toString(ThreadName name)
{
    switch (name)
    {
        #define ACTION(NAME, STR) case ThreadName::NAME: return as_string(STR);
        THREAD_NAMES_VALUES(ACTION)
        #undef ACTION
    }
}

ThreadName parseThreadName(const std::string & name)
{
    if (auto it = str_to_thread_name.find(name); it != str_to_thread_name.end())
        return it->second;
    else
        return ThreadName::UNKNOWN;
}

void setThreadName(ThreadName name)
{
    thread_name = name;

    auto thread_name_str = toString(name);
    if (thread_name_str.size() > THREAD_NAME_SIZE - 1)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Thread name cannot be longer than 15 bytes");


#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), thread_name_str.data());
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(thread_name_str.data()))
#elif defined(OS_SUNOS)
    if (0 != pthread_setname_np(pthread_self(), thread_name_str.data()))
#else
    if (0 != prctl(PR_SET_NAME, thread_name_str.data(), 0, 0, 0))
#endif
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot set thread name with prctl(PR_SET_NAME, ...)");

#if USE_JEMALLOC
    DB::Jemalloc::setValue("thread.prof.name", thread_name_str.data());
#endif
}

ThreadName getThreadName()
{
    if (thread_name != ThreadName::UNKNOWN)
        return thread_name;

    char tmp_thread_name[THREAD_NAME_SIZE] = {};

#if defined(OS_DARWIN) || defined(OS_SUNOS)
    if (pthread_getname_np(pthread_self(), tmp_thread_name, THREAD_NAME_SIZE))
        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_getname_np()");
#elif defined(OS_FREEBSD)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), thread_name, THREAD_NAME_SIZE))
//        throw DB::Exception(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with pthread_get_name_np()");
#else
    if (0 != prctl(PR_GET_NAME, tmp_thread_name, 0, 0, 0))
        if (errno != ENOSYS && errno != EPERM)    /// It's ok if the syscall is unsupported or not allowed in some environments.
            throw DB::ErrnoException(DB::ErrorCodes::PTHREAD_ERROR, "Cannot get thread name with prctl(PR_GET_NAME)");
#endif

    return parseThreadName(std::string{tmp_thread_name});
}

}
