#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Field.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
};

namespace
{

class KeysIterator : public IObjectIterator
{
public:
    KeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        std::optional<UInt64> snapshot_version_ = std::nullopt)
        : data_files(data_files_)
        , object_storage(object_storage_)
        , callback(callback_)
        , snapshot_version(snapshot_version_)
    {
    }

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    std::optional<UInt64> getSnapshotVersion() const override
    {
        return snapshot_version;
    }

    ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
            if (current_index >= data_files.size())
                return nullptr;

            auto key = data_files[current_index];
            auto object_metadata = object_storage->getObjectMetadata(key, /*with_tags=*/ false);

            if (callback)
                callback(FileProgress(0, object_metadata.size_bytes));

            return std::make_shared<ObjectInfo>(RelativePathWithMetadata{key, std::move(object_metadata)});
        }
    }

private:
    Strings data_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
    std::optional<UInt64> snapshot_version;
};

}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_) const
{
    return std::make_shared<KeysIterator>(std::move(data_files_), object_storage_, callback_);
}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_,
    UInt64 snapshot_version_) const
{
    return std::make_shared<KeysIterator>(std::move(data_files_), object_storage_, callback_, snapshot_version_);
}

ReadFromFormatInfo IDataLakeMetadata::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool supports_tuple_elements)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns, supports_tuple_elements);
}

DataFileMetaInfo::DataFileMetaInfo(
    const Iceberg::IcebergSchemaProcessor & schema_processor,
    Int32 schema_id,
    const std::unordered_map<Int32, Iceberg::ColumnInfo> & columns_info_)
{

    std::vector<Int32> column_ids;
    for (const auto & column : columns_info_)
        column_ids.push_back(column.first);

    auto name_and_types = schema_processor.tryGetFieldsCharacteristics(schema_id, column_ids);
    std::unordered_map<Int32, std::string> name_by_index;
    for (const auto & name_and_type : name_and_types)
    {
        const auto name = name_and_type.getNameInStorage();
        auto index = schema_processor.tryGetColumnIDByName(schema_id, name);
        if (index.has_value())
            name_by_index[index.value()] = name;
    }

    for (const auto & column : columns_info_)
    {
        auto i_name = name_by_index.find(column.first);
        if (i_name != name_by_index.end())
        {
            columns_info[i_name->second] = {column.second.rows_count, column.second.nulls_count, column.second.hyperrectangle};
        }
    }
}

DataFileMetaInfo::DataFileMetaInfo(Poco::JSON::Object::Ptr file_info)
{
    if (!file_info)
        return;

    auto log = getLogger("DataFileMetaInfo");

    if (file_info->has("columns"))
    {
        auto columns = file_info->getArray("columns");
        for (size_t i = 0; i < columns->size(); ++i)
        {
            auto column = columns->getObject(static_cast<UInt32>(i));

            std::string name;
            if (column->has("name"))
                name = column->get("name").toString();
            else
            {
                LOG_WARNING(log, "Can't read column name, ignored");
                continue;
            }

            DB::DataFileMetaInfo::ColumnInfo column_info;
            if (column->has("rows"))
                column_info.rows_count = column->get("rows");
            if (column->has("nulls"))
                column_info.nulls_count = column->get("nulls");
            if (column->has("range"))
            {
                Range range("");
                std::string r = column->get("range");
                try
                {
                    range.deserialize(r, /*base64*/ true);
                    column_info.hyperrectangle = std::move(range);
                }
                catch (const Exception & e)
                {
                    LOG_WARNING(log, "Can't read range for column {}, range '{}' ignored, error: {}", name, r, e.what());
                }
            }

            columns_info[name] = column_info;
        }
    }
}

Poco::JSON::Object::Ptr DataFileMetaInfo::toJson() const
{
    Poco::JSON::Object::Ptr file_info = new Poco::JSON::Object();

    if (!columns_info.empty())
    {
        Poco::JSON::Array::Ptr columns = new Poco::JSON::Array();

        for (const auto & column : columns_info)
        {
            Poco::JSON::Object::Ptr column_info = new Poco::JSON::Object();
            column_info->set("name", column.first);
            if (column.second.rows_count.has_value())
                column_info->set("rows", column.second.rows_count.value());
            if (column.second.nulls_count.has_value())
                column_info->set("nulls", column.second.nulls_count.value());
            if (column.second.hyperrectangle.has_value())
                column_info->set("range", column.second.hyperrectangle.value().serialize(/*base64*/ true));

            columns->add(column_info);
        }

        file_info->set("columns", columns);
    }

    return file_info;
}

constexpr size_t FIELD_MASK_ROWS = 0x1;
constexpr size_t FIELD_MASK_NULLS = 0x2;
constexpr size_t FIELD_MASK_RECT = 0x4;
constexpr size_t FIELD_MASK_ALL = 0x7;

void DataFileMetaInfo::serialize(WriteBuffer & out) const
{
    auto size = columns_info.size();
    writeIntBinary(size, out);
    for (const auto & column : columns_info)
    {
        writeStringBinary(column.first, out);
        size_t field_mask = 0;
        if (column.second.rows_count.has_value())
            field_mask |= FIELD_MASK_ROWS;
        if (column.second.nulls_count.has_value())
            field_mask |= FIELD_MASK_NULLS;
        if (column.second.hyperrectangle.has_value())
            field_mask |= FIELD_MASK_RECT;
        writeIntBinary(field_mask, out);

        if (column.second.rows_count.has_value())
            writeIntBinary(column.second.rows_count.value(), out);
        if (column.second.nulls_count.has_value())
            writeIntBinary(column.second.nulls_count.value(), out);
        if (column.second.hyperrectangle.has_value())
        {
            writeFieldBinary(column.second.hyperrectangle.value().left, out);
            writeFieldBinary(column.second.hyperrectangle.value().right, out);
        }
    }
}

DataFileMetaInfo DataFileMetaInfo::deserialize(ReadBuffer & in)
{
    DataFileMetaInfo result;

    size_t size;
    readIntBinary(size, in);
    
    for (size_t i = 0; i < size; ++i)
    {
        std::string name;
        readStringBinary(name, in);
        size_t field_mask;
        readIntBinary(field_mask, in);
        if ((field_mask & FIELD_MASK_ALL) != field_mask)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected field mask: {}", field_mask);

        ColumnInfo & column = result.columns_info[name];

        if (field_mask & FIELD_MASK_ROWS)
        {
            Int64 value;
            readIntBinary(value, in);
            column.rows_count = value;
        }
        if (field_mask & FIELD_MASK_NULLS)
        {
            Int64 value;
            readIntBinary(value, in);
            column.nulls_count = value;
        }
        if (field_mask & FIELD_MASK_RECT)
        {
            FieldRef left = readFieldBinary(in);
            FieldRef right = readFieldBinary(in);
            column.hyperrectangle = Range(left, true, right, true);
        }
    }

    return result;
}


}
