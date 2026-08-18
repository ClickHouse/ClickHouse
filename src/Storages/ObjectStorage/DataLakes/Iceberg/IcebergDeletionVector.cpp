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
#include <Storages/ObjectStorage/DataLakes/PuffinFilesCache.h>
#include <Storages/ObjectStorage/Utils.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>

#include <atomic>

namespace DB
{

namespace ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
extern const SettingsBool use_puffin_files_cache;
}

}

namespace DB::Iceberg
{

void validateDeletionVectorPositionsAgainstDataFile(
    std::span<const UInt64> deleted_positions,
    UInt64 expected_cardinality,
    Int64 data_file_record_count)
{
    if (data_file_record_count < 0)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Data file record_count {} must be non-negative",
            data_file_record_count);
    }

    const UInt64 data_file_rows = static_cast<UInt64>(data_file_record_count);

    if (expected_cardinality > data_file_rows)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Deletion vector cardinality {} exceeds data file record_count {}",
            expected_cardinality,
            data_file_record_count);
    }

    for (UInt64 position : deleted_positions)
    {
        if (position >= data_file_rows)
        {
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Deletion vector position {} is out of range for data file record_count {}",
                position,
                data_file_record_count);
        }
    }
}

namespace
{

using FooterBlobsPtr = PuffinFilesCache::FooterBlobsPtr;

void logUndersizedPuffinFilesCacheOnce(LoggerPtr log, size_t max_size_in_bytes, UInt64 minimum_entry_weight)
{
    static std::atomic_flag logged = ATOMIC_FLAG_INIT;
    if (!logged.test_and_set())
    {
        LOG_WARNING(
            log,
            "Not using Puffin files cache because puffin_files_cache_size ({}) is smaller than the "
            "minimum deletion-vector entry weight (at least {}); falling back to filesystem-cache-enabled reads",
            max_size_in_bytes,
            minimum_entry_weight);
    }
}

FooterBlobsPtr readFooterBlobs(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
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

    return std::make_shared<const std::vector<PuffinBlob>>(readPuffinFooterBlobsFromSeekable(*seekable, *file_size));
}

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVectorUncached(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    UInt64 expected_cardinality,
    Int64 data_file_record_count,
    ContextPtr context,
    LoggerPtr log,
    bool disable_filesystem_cache,
    FooterBlobsPtr preloaded_footer)
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

    FooterBlobsPtr footer_owner = preloaded_footer;
    if (!footer_owner)
        footer_owner = std::make_shared<const std::vector<PuffinBlob>>(readPuffinFooterBlobsFromSeekable(*seekable, *file_size));

    bindDeletionVectorBlob(
        *footer_owner,
        content_offset,
        content_size_in_bytes,
        expected_data_file.serialize(),
        expected_cardinality);

    auto deleted_positions = readDeletionVectorFromPuffin(
        *read_buffer, content_offset, content_size_in_bytes, expected_cardinality);

    validateDeletionVectorPositionsAgainstDataFile(deleted_positions, expected_cardinality, data_file_record_count);

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
    Int64 data_file_record_count,
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

    if (data_file_record_count < 0)
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Data file record_count {} must be non-negative",
            data_file_record_count);
    }

    const UInt64 expected_cardinality_u64 = static_cast<UInt64>(expected_cardinality);

    /// Fail closed before I/O when declared DV cardinality cannot fit in the data file.
    if (expected_cardinality_u64 > static_cast<UInt64>(data_file_record_count))
    {
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Deletion vector cardinality {} exceeds data file record_count {}",
            expected_cardinality,
            data_file_record_count);
    }

    const bool use_cache_setting = context->getSettingsRef()[Setting::use_puffin_files_cache];
    auto cache = use_cache_setting ? context->getPuffinFilesCache() : nullptr;

    /// puffin_files_cache_size=0 means the LRU accepts no entries ("disabled" in server settings).
    /// Take the same uncached path as use_puffin_files_cache=0 so we keep filesystem cache and
    /// skip etag HEAD / getOrSet (which would disable filesystem cache on the miss loader).
    if (!cache || cache->maxSizeInBytes() == 0)
    {
        if (!use_cache_setting)
        {
            LOG_TRACE(log, "Not using Puffin files cache for '{}', because the setting use_puffin_files_cache is false", puffin_path);
        }
        else
        {
            LOG_TRACE(
                log,
                "Not using Puffin files cache for '{}', because puffin_files_cache_size is 0",
                puffin_path);
        }
        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            data_file_record_count,
            context,
            log,
            false,
            nullptr);
    }

    const String storage_identity = PuffinFilesCache::makeStorageIdentity(*object_storage);
    const String referenced_data_file_key = expected_data_file.serialize();

    /// Before etag HEAD: if the budget cannot hold even the key/overhead lower bound, skip the
    /// cached path entirely (keeps filesystem cache, avoids a useless HEAD + immediate eviction).
    {
        const PuffinFilesCacheKey provisional_key{
            storage_identity,
            puffin_path,
            /*etag=*/"",
            content_offset,
            content_size_in_bytes,
            referenced_data_file_key,
            expected_cardinality_u64,
            static_cast<UInt64>(data_file_record_count)};
        const UInt64 minimum_entry_weight
            = PuffinFilesCacheCell::estimateMinimumMemorySize(provisional_key.approximateMemoryBytes());
        if (minimum_entry_weight > cache->maxSizeInBytes())
        {
            logUndersizedPuffinFilesCacheOnce(log, cache->maxSizeInBytes(), minimum_entry_weight);
            return loadDeletionVectorUncached(
                object_storage,
                puffin_path,
                content_offset,
                content_size_in_bytes,
                expected_data_file,
                expected_cardinality_u64,
                data_file_record_count,
                context,
                log,
                false,
                nullptr);
        }
    }

    RelativePathWithMetadata puffin_object{puffin_path};
    if (!puffin_object.metadata)
        puffin_object.metadata = object_storage->getObjectMetadata(puffin_object.getPath(), /*with_tags=*/ false);

    if (puffin_object.metadata->etag.empty())
    {
        LOG_TRACE(
            log,
            "Not using Puffin files cache for '{}', because etag is empty",
            puffin_path);
        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            data_file_record_count,
            context,
            log,
            false,
            nullptr);
    }

    auto footer_key = PuffinFilesCache::tryCreateFooterKey(storage_identity, puffin_path, puffin_object.metadata->etag);
    auto cache_key = PuffinFilesCache::tryCreateKey(
        storage_identity,
        puffin_path,
        puffin_object.metadata->etag,
        content_offset,
        content_size_in_bytes,
        referenced_data_file_key,
        expected_cardinality_u64,
        static_cast<UInt64>(data_file_record_count));

    /// Empty etag is the only reason tryCreate* returns nullopt; that case is handled above.
    if (!footer_key || !cache_key)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PuffinFilesCache::tryCreate* returned nullopt for non-empty etag on '{}'",
            puffin_path);
    }

    /// Recheck with the real etag (provisional estimate used an empty etag string).
    {
        const UInt64 minimum_entry_weight
            = PuffinFilesCacheCell::estimateMinimumMemorySize(cache_key->approximateMemoryBytes());
        if (minimum_entry_weight > cache->maxSizeInBytes())
        {
            logUndersizedPuffinFilesCacheOnce(log, cache->maxSizeInBytes(), minimum_entry_weight);
            return loadDeletionVectorUncached(
                object_storage,
                puffin_path,
                content_offset,
                content_size_in_bytes,
                expected_data_file,
                expected_cardinality_u64,
                data_file_record_count,
                context,
                log,
                false,
                nullptr);
        }
    }

    /// Footer is keyed by file identity only, so N DV slices in one coalesced Puffin share one parse.
    /// Resolve the footer only on a deletion-vector cache miss (nested memo lookup).
    return cache->getOrSetDeletionVector(*cache_key, [&]()
    {
        auto footer = cache->getOrSetFooter(*footer_key, [&]()
        {
            return readFooterBlobs(object_storage, puffin_path, context, log, /*disable_filesystem_cache=*/ true);
        });

        return loadDeletionVectorUncached(
            object_storage,
            puffin_path,
            content_offset,
            content_size_in_bytes,
            expected_data_file,
            expected_cardinality_u64,
            data_file_record_count,
            context,
            log,
            true,
            footer);
    });
}

}

#endif
