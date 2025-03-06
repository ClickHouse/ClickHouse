#pragma once

#include <Processors/Chunk.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct DistributedQueryPlan;

void executeDistributedQuery(const UUID & unique_query_id, const DistributedQueryPlan & distributed_query_plan, ContextPtr context);

struct DistributedQueryTask;

/// Executes a task locally
void doExecuteTask(const String & serialized_query_plan, const DistributedQueryTask & task, ObjectStoragePtr object_storage, const String & object_storage_path, ContextPtr context);

/// Returns object storage and path for temporary files
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);

struct ITemporaryFileLookup;
using TemporaryFileLookupPtr = std::shared_ptr<ITemporaryFileLookup>;

/// ITemporaryFileLookup that is used in buildQueryPipeline() to create readers and writers for temporary files by temporary file logical names
TemporaryFileLookupPtr createTemporaryFilesLookup(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & input_temporary_files_, const Strings & output_temporary_files_);

class ICustomResourceHolder;

/// Helper to clean temporary files after query execution
std::shared_ptr<ICustomResourceHolder> makeTemporaryFilesCleaner(ObjectStoragePtr object_storage_, const String & object_storage_path_,
    const Strings & temporary_files_);

}
