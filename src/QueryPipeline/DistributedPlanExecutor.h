#pragma once

#include <Processors/Chunk.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/Progress.h>

namespace DB
{

struct DistributedQueryPlan;

void executeDistributedQuery(const UUID & unique_query_id, const DistributedQueryPlan & distributed_query_plan, ContextPtr context, std::shared_ptr<std::atomic<bool>> is_cancelled);

/// Contains info about hosts assigned to exchange buckets
struct ExchangeStreamSources
{
    /// Exchange stream id -> source host
    std::unordered_map<String, String> stream_hosts;
};

/// Contains all info to send a task to remote worker
struct DistributedQueryTaskDescription
{
    String initial_query_id;
    DistributedQueryTask task;
    String serialized_query_plan;
    ExchangeDescriptions exchanges;
    ExchangeStreamSources exchange_stream_sources;
};

/// Executes a task locally
void doExecuteTask(const DistributedQueryTaskDescription & task, ObjectStoragePtr object_storage,
    const String & object_storage_path, ContextMutablePtr context, std::function<bool()> is_cancelled = nullptr, ProgressCallback progress_callback = nullptr);

/// Returns object storage and path for temporary files
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);

struct ITemporaryFileLookup;
using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;

/// ITemporaryFileLookup that is used in buildQueryPipeline() to create readers and writers for temporary files by temporary file logical names
TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_);

struct IExchangeLookup;
using ExchangeLookupPtr = std::shared_ptr<IExchangeLookup>;

struct ExchangeDescription;

ExchangeLookupPtr createExchangeLookup(
    const String & query_id,
    const std::unordered_map<String, ExchangeDescription> & exchanges_,
    const ExchangeStreamSources & exchange_stream_sources,
    TemporaryFileLookupPtr temporary_files_,
    ContextPtr context);

class ICustomResourceHolder;

/// Helper to clean temporary files after query execution
std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & temporary_files_);

}
