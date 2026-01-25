#include <Storages/MergeTree/RowDataSerializer.h>

#include <Columns/ColumnString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <city.h>

namespace ProfileEvents
{
    extern const Event HybridStorageRowSerializationMicroseconds;
    extern const Event HybridStorageRowDeserializationMicroseconds;
    extern const Event HybridStorageRowsTooLarge;
    extern const Event HybridStorageChecksumMismatches;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int LOGICAL_ERROR;
}

RowDataSerializer::RowDataSerializer(const Settings & settings_)
    : settings(settings_)
{
}

void RowDataSerializer::writeHeader(WriteBuffer & buf, UInt16 num_columns, UInt8 flags)
{
    /// Magic number
    writeBinaryLittleEndian(MAGIC_NUMBER, buf);
    /// Version
    writeBinaryLittleEndian(FORMAT_VERSION, buf);
    /// Flags
    writeBinaryLittleEndian(flags, buf);
    /// Number of columns
    writeBinaryLittleEndian(num_columns, buf);
    /// Reserved (2 bytes)
    writeBinaryLittleEndian(static_cast<UInt16>(0), buf);
}

bool RowDataSerializer::readHeader(ReadBuffer & buf, UInt16 & num_columns, UInt8 & flags) const
{
    if (buf.available() < HEADER_SIZE)
        return false;

    UInt16 magic;
    readBinaryLittleEndian(magic, buf);
    if (magic != MAGIC_NUMBER)
        return false;

    UInt8 version;
    readBinaryLittleEndian(version, buf);
    if (version != FORMAT_VERSION)
        return false;

    readBinaryLittleEndian(flags, buf);
    readBinaryLittleEndian(num_columns, buf);

    /// Skip reserved bytes
    UInt16 reserved;
    readBinaryLittleEndian(reserved, buf);

    return true;
}

UInt32 RowDataSerializer::calculateChecksum(const char * data, size_t size)
{
    /// Use CityHash64 and truncate to 32 bits for checksum
    return static_cast<UInt32>(CityHash_v1_0_2::CityHash64(data, size));
}

bool RowDataSerializer::verifyChecksum(const String & data) const
{
    if (!settings.enable_checksum)
        return true;

    if (data.size() < HEADER_SIZE + CHECKSUM_SIZE)
        return false;

    size_t data_size = data.size() - CHECKSUM_SIZE;
    UInt32 stored_checksum;

    ReadBufferFromMemory checksum_buf(data.data() + data_size, CHECKSUM_SIZE);
    readBinaryLittleEndian(stored_checksum, checksum_buf);

    UInt32 calculated_checksum = calculateChecksum(data.data(), data_size);

    return stored_checksum == calculated_checksum;
}

String RowDataSerializer::serializeRow(
    const Block & block,
    size_t row_num,
    const NamesAndTypesList & columns_to_serialize,
    const SerializationByName & serializations)
{
    Stopwatch watch;
    WriteBufferFromOwnString buf;

    /// Write header with placeholder values
    writeHeader(buf, static_cast<UInt16>(columns_to_serialize.size()), 0);

    /// Serialize each column value
    UInt16 column_idx = 0;
    for (const auto & name_and_type : columns_to_serialize)
    {
        auto it = serializations.find(name_and_type.name);
        if (it == serializations.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Serialization not found for column '{}' in hybrid storage serializer",
                name_and_type.name);

        const auto & serialization = it->second;
        const auto & column = block.getByName(name_and_type.name).column;

        /// Write column index
        writeBinaryLittleEndian(column_idx, buf);

        /// Serialize column value to temporary buffer to get size
        WriteBufferFromOwnString col_buf;
        serialization->serializeBinary(*column, row_num, col_buf, {});
        String col_data = col_buf.str();

        /// Write size and data
        writeBinaryLittleEndian(static_cast<UInt32>(col_data.size()), buf);
        buf.write(col_data.data(), col_data.size());

        ++column_idx;
    }

    String result = buf.str();

    /// Check size limit (before adding checksum)
    if (result.size() + CHECKSUM_SIZE > settings.max_row_size)
    {
        /// Row too large, return empty string to indicate this row should be skipped
        ProfileEvents::increment(ProfileEvents::HybridStorageRowsTooLarge);
        return "";
    }

    /// Add checksum
    if (settings.enable_checksum)
    {
        UInt32 checksum = calculateChecksum(result.data(), result.size());
        WriteBufferFromOwnString checksum_buf;
        writeBinaryLittleEndian(checksum, checksum_buf);
        result += checksum_buf.str();
    }

    ProfileEvents::increment(ProfileEvents::HybridStorageRowSerializationMicroseconds, watch.elapsedMicroseconds());
    return result;
}

ColumnPtr RowDataSerializer::serializeBlock(
    const Block & block,
    const NamesAndTypesList & columns_to_serialize,
    const SerializationByName & serializations)
{
    auto result_column = ColumnString::create();
    size_t num_rows = block.rows();

    for (size_t row_num = 0; row_num < num_rows; ++row_num)
    {
        String row_data = serializeRow(block, row_num, columns_to_serialize, serializations);

        if (row_data.empty())
        {
            /// Row was too large, insert empty placeholder
            /// The reader will fall back to column-based reading for this row
            result_column->insertDefault();
        }
        else
        {
            result_column->insertData(row_data.data(), row_data.size());
        }
    }

    return result_column;
}

void RowDataSerializer::deserializeRow(
    const String & row_data,
    const NamesAndTypesList & columns_to_deserialize,
    const SerializationByName & serializations,
    MutableColumns & result_columns)
{
    Stopwatch watch;

    if (row_data.empty())
    {
        /// Empty row data means row was too large during serialization
        /// Insert defaults for all columns
        for (auto & col : result_columns)
            col->insertDefault();
        return;
    }

    if (!verifyChecksum(row_data))
    {
        ProfileEvents::increment(ProfileEvents::HybridStorageChecksumMismatches);
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Checksum mismatch in hybrid storage row data");
    }

    size_t data_size = settings.enable_checksum ? row_data.size() - CHECKSUM_SIZE : row_data.size();
    ReadBufferFromMemory buf(row_data.data(), data_size);

    UInt16 num_columns;
    UInt8 flags;
    if (!readHeader(buf, num_columns, flags))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Invalid header in hybrid storage row data");

    /// Build a map of column index to position in result_columns
    std::unordered_map<String, size_t> column_name_to_result_idx;
    size_t result_idx = 0;
    for (const auto & name_and_type : columns_to_deserialize)
    {
        column_name_to_result_idx[name_and_type.name] = result_idx;
        ++result_idx;
    }

    /// Read all column data
    std::vector<bool> column_found(result_columns.size(), false);

    for (UInt16 i = 0; i < num_columns; ++i)
    {
        UInt16 column_idx;
        readBinaryLittleEndian(column_idx, buf);

        UInt32 col_size;
        readBinaryLittleEndian(col_size, buf);

        /// Find the column name by index in columns_to_deserialize
        /// Note: This assumes columns_to_deserialize is in the same order as when serialized
        String col_name;
        size_t idx = 0;
        for (const auto & name_and_type : columns_to_deserialize)
        {
            if (idx == column_idx)
            {
                col_name = name_and_type.name;
                break;
            }
            ++idx;
        }

        auto it = column_name_to_result_idx.find(col_name);
        if (it != column_name_to_result_idx.end() && !col_name.empty())
        {
            /// This column is requested, deserialize it
            auto serialization_it = serializations.find(col_name);
            if (serialization_it != serializations.end())
            {
                serialization_it->second->deserializeBinary(*result_columns[it->second], buf, {});
                column_found[it->second] = true;
            }
            else
            {
                /// Skip this column's data
                buf.ignore(col_size);
            }
        }
        else
        {
            /// Column not needed, skip it
            buf.ignore(col_size);
        }
    }

    /// Insert defaults for any columns that weren't found in the row data
    for (size_t i = 0; i < result_columns.size(); ++i)
    {
        if (!column_found[i])
            result_columns[i]->insertDefault();
    }

    ProfileEvents::increment(ProfileEvents::HybridStorageRowDeserializationMicroseconds, watch.elapsedMicroseconds());
}

void RowDataSerializer::extractColumns(
    const String & row_data,
    const NamesAndTypesList & all_columns_in_row,
    const NamesAndTypesList & columns_to_extract,
    const SerializationByName & serializations,
    MutableColumns & result_columns)
{
    Stopwatch watch;

    if (row_data.empty())
    {
        /// Empty row data means row was too large during serialization
        /// Insert defaults for all columns
        for (auto & col : result_columns)
            col->insertDefault();
        return;
    }

    if (!verifyChecksum(row_data))
    {
        ProfileEvents::increment(ProfileEvents::HybridStorageChecksumMismatches);
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Checksum mismatch in hybrid storage row data");
    }

    size_t data_size = settings.enable_checksum ? row_data.size() - CHECKSUM_SIZE : row_data.size();
    ReadBufferFromMemory buf(row_data.data(), data_size);

    UInt16 num_columns;
    UInt8 flags;
    if (!readHeader(buf, num_columns, flags))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Invalid header in hybrid storage row data");

    /// Build maps for extraction
    std::unordered_map<UInt16, String> idx_to_name;
    UInt16 idx = 0;
    for (const auto & name_and_type : all_columns_in_row)
    {
        idx_to_name[idx] = name_and_type.name;
        ++idx;
    }

    std::unordered_map<String, size_t> name_to_result_idx;
    size_t result_idx = 0;
    for (const auto & name_and_type : columns_to_extract)
    {
        name_to_result_idx[name_and_type.name] = result_idx;
        ++result_idx;
    }

    std::vector<bool> column_found(result_columns.size(), false);

    /// Read column data
    for (UInt16 i = 0; i < num_columns; ++i)
    {
        UInt16 column_idx;
        readBinaryLittleEndian(column_idx, buf);

        UInt32 col_size;
        readBinaryLittleEndian(col_size, buf);

        auto name_it = idx_to_name.find(column_idx);
        if (name_it == idx_to_name.end())
        {
            /// Unknown column index, skip
            buf.ignore(col_size);
            continue;
        }

        const String & col_name = name_it->second;
        auto result_it = name_to_result_idx.find(col_name);

        if (result_it != name_to_result_idx.end())
        {
            /// This column is requested
            auto serialization_it = serializations.find(col_name);
            if (serialization_it != serializations.end())
            {
                serialization_it->second->deserializeBinary(*result_columns[result_it->second], buf, {});
                column_found[result_it->second] = true;
            }
            else
            {
                buf.ignore(col_size);
            }
        }
        else
        {
            /// Column not needed, skip
            buf.ignore(col_size);
        }
    }

    /// Insert defaults for missing columns
    for (size_t i = 0; i < result_columns.size(); ++i)
    {
        if (!column_found[i])
            result_columns[i]->insertDefault();
    }

    ProfileEvents::increment(ProfileEvents::HybridStorageRowDeserializationMicroseconds, watch.elapsedMicroseconds());
}

bool RowDataSerializer::isValid(const String & row_data) const
{
    if (row_data.empty())
        return false;

    if (row_data.size() < HEADER_SIZE + (settings.enable_checksum ? CHECKSUM_SIZE : 0))
        return false;

    ReadBufferFromMemory buf(row_data.data(), HEADER_SIZE);
    UInt16 num_columns;
    UInt8 flags;

    if (!readHeader(buf, num_columns, flags))
        return false;

    return verifyChecksum(row_data);
}

size_t RowDataSerializer::getNumColumns(const String & row_data)
{
    if (row_data.size() < HEADER_SIZE)
        return 0;

    ReadBufferFromMemory buf(row_data.data(), HEADER_SIZE);

    UInt16 magic;
    readBinaryLittleEndian(magic, buf);
    if (magic != MAGIC_NUMBER)
        return 0;

    UInt8 version;
    readBinaryLittleEndian(version, buf);
    if (version != FORMAT_VERSION)
        return 0;

    UInt8 flags;
    readBinaryLittleEndian(flags, buf);

    UInt16 num_columns;
    readBinaryLittleEndian(num_columns, buf);

    return num_columns;
}

}
