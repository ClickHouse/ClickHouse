#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDeletionVector.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/PuffinDeletionVectorReader.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

}

namespace DB::Iceberg
{

DataLakeObjectMetadata::ExcludedRowsPtr loadDeletionVector(
    ObjectStoragePtr object_storage,
    const String & puffin_path,
    Int64 content_offset,
    Int64 content_size_in_bytes,
    const IcebergPathFromMetadata & expected_data_file,
    const std::optional<IcebergPathFromMetadata> & referenced_data_file,
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

    RelativePathWithMetadata puffin_object{puffin_path};
    auto read_buffer = createReadBuffer(puffin_object, object_storage, context, log);
    auto deleted_positions = readDeletionVectorFromPuffin(*read_buffer, content_offset, content_size_in_bytes);

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
