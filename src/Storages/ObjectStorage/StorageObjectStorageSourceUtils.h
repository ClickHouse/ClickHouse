#pragma once
#include <IO/Archives/IArchiveReader.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/SourceWithKeyCondition.h>
#include <Storages/ObjectStorage/DataLakes/PartitionColumns.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/re2.h>

namespace DB
{

namespace StorageObjectStorageSourceUtils
{

using ObjectInfo = StorageObjectStorage::ObjectInfo;
using ObjectInfoPtr = StorageObjectStorage::ObjectInfoPtr;
using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

std::unique_ptr<ReadBufferFromFileBase>
createReadBuffer(ObjectInfo & object_info, const ObjectStoragePtr & object_storage, const ContextPtr & context_, const LoggerPtr & log);

std::shared_ptr<IInputFormat> getInputFormat(
    std::unique_ptr<ReadBuffer> & read_buf,
    std::unique_ptr<ReadBuffer> & read_buf_schema,
    Block & initial_header,
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfoPtr object_info,
    ContextPtr context_,
    const LoggerPtr & log,
    size_t max_block_size,
    size_t max_parsing_threads,
    bool need_only_count,
    bool read_all_columns);

}

}
