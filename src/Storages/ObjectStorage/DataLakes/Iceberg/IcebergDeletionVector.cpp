#include "config.h"

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVector.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFile.h>
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>
#include <Storages/ObjectStorage/Utils.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
extern const SettingsBool use_puffin_files_cache;
}

}

namespace DB::Iceberg
{

namespace
{

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVectorUncached(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    UInt64 expected_cardinality,
    ContextPtr context,
    LoggerPtr log,
    bool disable_filesystem_cache)
{
    RelativePathWithMetadata puffin_object{puffin_path};
    auto read_settings = context->getReadSettings();
    if (disable_filesystem_cache)
        read_settings.enable_filesystem_cache = false;

    auto read_buffer = createReadBuffer(puffin_object, object_storage, context, log, read_settings);

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(read_buffer.get());
    if (!seekable)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin deletion vector read requires a seekable buffer");

    auto file_size = tryGetFileSizeFromReadBuffer(*read_buffer);
    if (!file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine Puffin file size for '{}'", puffin_path);

    const auto blobs = readPuffinFooterBlobsFromSeekable(*seekable, *file_size);
    bindDeletionVectorBlob(
        blobs,
        content_offset,
        content_size_in_bytes,
        expected_data_file.serialize(),
        expected_cardinality);

    auto deleted_positions = readDeletionVectorFromPuffin(
        *read_buffer, content_offset, content_size_in_bytes, expected_cardinality);

    if (deleted_positions.empty())
        return nullptr;

    auto bitmap = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (UInt64 position : deleted_positions)
        bitmap->add(static_cast<size_t>(position));

    LOG_DEBUG(
        log,
        "Loaded deletion vector from puffin file '{}' for data file '{}': {} deleted rows",
        puffin_path,
        expected_data_file.serialize(),
        deleted_positions.size());

    return bitmap;
}

}

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVector(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    const std::optional<IcebergPathFromMetadata> & referenced_data_file,
    Int64 expected_cardinality,
    ContextPtr context,
    LoggerPtr log)
{
    if (referenced_data_file.has_value() && referenced_data_file.value() != expected_data_file)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Deletion vector referenced_data_file '{}' does not match data file '{}'",
            referenced_data_file->serialize(),
            expected_data_file.serialize());
    }

    if (expected_cardinality < 0)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Deletion vector record_count {} must be non-negative",
            expected_cardinality);
    }

    const UInt64 expected_cardinality_u64 = static_cast<UInt64>(expected_cardinality);

    const bool use_cache = context->getSettingsRef()[Setting::use_puffin_files_cache];
    if (!use_cache)
    {
        LOG_TRACE(log, "Not using Puffin files cache for '{}', because the setting use_puffin_files_cache is false", puffin_path);
        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            context,
            log,
            false);
    }

    RelativePathWithMetadata puffin_object{puffin_path};
    if (!puffin_object.metadata)
        puffin_object.metadata = object_storage->getObjectMetadata(puffin_object.getPath(), /*with_tags=*/ false);

    auto cache_key = PuffinFilesCache::tryCreateKey(
        puffin_path,
        puffin_object.metadata->etag,
        content_offset,
        content_size_in_bytes,
        expected_data_file.serialize());

    if (!cache_key)
    {
        LOG_TRACE(log, "Not using Puffin files cache for '{}', because etag is empty", puffin_path);
        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            context,
            log,
            false);
    }

    auto cache = context->getPuffinFilesCache();
    return cache->getOrSetDeletionVector(*cache_key, [&]()
    {
        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            context,
            log,
            true);
    });
}

}

#endif
