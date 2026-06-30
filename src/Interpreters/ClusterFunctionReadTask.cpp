#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/Context.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int LOGICAL_ERROR;
}
namespace Setting
{
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
    extern const SettingsBool s3_validate_etag_on_read;
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context)
{
    if (!object)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`object` cannot be null");

    if (object->data_lake_metadata.has_value())
        data_lake_metadata = object->data_lake_metadata.value();

#if USE_AVRO
    if (std::dynamic_pointer_cast<IcebergDataObjectInfo>(object))
    {
        iceberg_info = dynamic_cast<IcebergDataObjectInfo &>(*object).info;
    }
#endif

    const bool send_over_whole_archive = !context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes];
    path = send_over_whole_archive ? object->getPathOrPathToArchiveIfArchive() : object->getPath();
    file_bucket_info = object->file_bucket_info;

    /// Capture the generation the coordinator saw (notably the ETag) so the worker can pin its read
    /// to it. Scoped to the bucket-splitting path with read-time ETag validation enabled: there the
    /// coordinator already refreshed the metadata while reading the object to compute bucket
    /// boundaries, and the worker reads a sub-range of that same generation. For every other path
    /// (non-bucket s3Cluster, validation disabled, non-S3 backends) we leave it empty so the worker
    /// fetches its own metadata exactly as before - no behavioral change.
    if (object->file_bucket_info && context->getSettingsRef()[Setting::s3_validate_etag_on_read])
    {
        if (auto object_metadata = object->getObjectMetadata())
        {
            etag = object_metadata->etag;
            size_bytes = object_metadata->size_bytes;
            is_size_known = object_metadata->is_size_known;
            last_modified_epoch_us = static_cast<UInt64>(object_metadata->last_modified.epochMicroseconds());
        }
    }
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(const std::string & path_)
    : path(path_)
{
}

ObjectInfoPtr ClusterFunctionReadTaskResponse::getObjectInfo() const
{
    if (isEmpty())
        return {};

    ObjectInfoPtr object;

    if (iceberg_info.has_value())
    {
#if USE_AVRO
        auto iceberg_object = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata{path});
        iceberg_object->info = iceberg_info.value();
        object = iceberg_object;
#else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg support is disabled");
#endif
    }
    else
    {
        object = std::make_shared<ObjectInfo>(path);
    }
    object->data_lake_metadata = data_lake_metadata;
    object->file_bucket_info = file_bucket_info;

    /// Pin the worker's read to the generation the coordinator saw at split time: reconstruct the
    /// object metadata (notably the ETag) so `createReadBuffer` validates every ranged GET against it
    /// instead of re-fetching the current - possibly overwritten - generation. Only when an ETag was
    /// actually captured; otherwise leave the metadata empty so the worker fetches it itself, as before.
    if (!etag.empty())
    {
        ObjectMetadata object_metadata;
        object_metadata.etag = etag;
        object_metadata.size_bytes = size_bytes;
        object_metadata.is_size_known = is_size_known;
        object_metadata.last_modified = Poco::Timestamp(static_cast<Poco::Timestamp::TimeVal>(last_modified_epoch_us));
        object->setObjectMetadata(object_metadata);
    }

    return object;
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t worker_protocol_version) const
{
    auto protocol_version
        = std::min(static_cast<UInt64>(worker_protocol_version), static_cast<UInt64>(DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION));
    writeVarUInt(protocol_version, out);
    writeStringBinary(path, out);

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        SerializedSetsRegistry registry;
        if (data_lake_metadata.schema_transform)
            data_lake_metadata.schema_transform->serialize(out, registry);
        else
            ActionsDAG().serialize(out, registry);

        if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS)
        {
            if (data_lake_metadata.excluded_rows)
                data_lake_metadata.excluded_rows->write(out);
            else
                DataLakeObjectMetadata::ExcludedRows().write(out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO)
    {
        if (file_bucket_info)
        {
            /// Write format name so we can create appropriate file bucket info during deserialization.
            writeStringBinary(file_bucket_info->getFormatName(), out);
            file_bucket_info->serialize(out);
        }
        else
        {
            /// Write empty string as format name if file_bucket_info is not set.
            writeStringBinary("", out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        if (iceberg_info.has_value())
        {
            writeVarUInt(1, out);
            iceberg_info->serializeForClusterFunctionProtocol(out, protocol_version);
        }
        else
        {
            writeVarUInt(0, out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_OBJECT_METADATA)
    {
        writeStringBinary(etag, out);
        writeVarUInt(size_bytes, out);
        writeVarUInt(is_size_known ? UInt64(1) : UInt64(0), out);
        writeVarUInt(last_modified_epoch_us, out);
    }
}

void ClusterFunctionReadTaskResponse::deserialize(ReadBuffer & in)
{
    size_t protocol_version = 0;
    readVarUInt(protocol_version, in);
    if (protocol_version < DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION || protocol_version > DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Supported protocol versions are in range [{}, {}], got: {}",
            DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION,
            protocol_version);
    }

    readStringBinary(path, in);
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        DeserializedSetsRegistry registry;
        auto transform = std::make_shared<ActionsDAG>(ActionsDAG::deserialize(in, registry, Context::getGlobalContextInstance()));

        if (!path.empty() && !transform->getInputs().empty())
        {
            data_lake_metadata.schema_transform = std::move(transform);
        }
        if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS)
        {
            data_lake_metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
            data_lake_metadata.excluded_rows->read(in);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO)
    {
        String format;
        readStringBinary(format, in);
        if (!format.empty())
        {
            file_bucket_info = FormatFactory::instance().getFileBucketInfo(format);
            file_bucket_info->deserialize(in);
        }
    }
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        auto has_iceberg_metadata = false;
        readVarUInt(has_iceberg_metadata, in);
        if (has_iceberg_metadata)
        {
            iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
            iceberg_info->deserializeForClusterFunctionProtocol(in, protocol_version);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_OBJECT_METADATA)
    {
        readStringBinary(etag, in);
        readVarUInt(size_bytes, in);
        UInt64 is_size_known_flag = 0;
        readVarUInt(is_size_known_flag, in);
        is_size_known = is_size_known_flag != 0;
        readVarUInt(last_modified_epoch_us, in);
    }
}

}
