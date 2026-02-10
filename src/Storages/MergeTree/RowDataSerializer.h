#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Columns/IColumn.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

/// Serializer for hybrid row-column storage.
/// Serializes row data into binary format for the __row column.
///
/// Binary format:
/// +------------------+------------------+------------------+
/// | Header (8 bytes) | Column Data      | Checksum (4 bytes)|
/// +------------------+------------------+------------------+
///
/// Header:
///   - Magic number (2 bytes): 0x5248 ('RH' for Row Hybrid)
///   - Version (1 byte): 0x01
///   - Flags (1 byte): reserved for compression type, etc.
///   - Num columns (2 bytes): number of columns in row
///   - Reserved (2 bytes): for future use
///
/// Column Data (repeated for each column):
///   - Column index (2 bytes): position in schema
///   - Data size (4 bytes): size of serialized data
///   - Data (variable): serialized column value
///
/// Checksum:
///   - CRC32 (4 bytes): checksum of header + column data
class RowDataSerializer
{
public:
    struct Settings
    {
        size_t max_row_size = 1024 * 1024; /// 1 MB default
        bool enable_checksum = true;
    };

    static constexpr UInt16 MAGIC_NUMBER = 0x5248; /// 'RH' for Row Hybrid
    static constexpr UInt8 FORMAT_VERSION = 0x01;
    static constexpr size_t HEADER_SIZE = 8;
    static constexpr size_t CHECKSUM_SIZE = 4;

    explicit RowDataSerializer(const Settings & settings_);

    /// Serialize a single row from block at given row_num.
    /// Returns serialized data or empty string if row exceeds max_size.
    /// @param block The block containing the row data
    /// @param row_num The row number to serialize
    /// @param columns_to_serialize List of columns to serialize (typically non-key columns)
    /// @param serializations Serialization objects for each column
    /// @return Serialized row data, or empty string if row is too large
    String serializeRow(
        const Block & block,
        size_t row_num,
        const NamesAndTypesList & columns_to_serialize,
        const SerializationByName & serializations);

    /// Serialize entire block into a column of row data.
    /// Each row in the block becomes one entry in the returned column.
    /// @param block The block to serialize
    /// @param columns_to_serialize List of columns to serialize
    /// @param serializations Serialization objects for each column
    /// @return Column containing serialized row data for each row
    ColumnPtr serializeBlock(
        const Block & block,
        const NamesAndTypesList & columns_to_serialize,
        const SerializationByName & serializations);

    /// Deserialize row data into mutable columns.
    /// @param row_data The serialized row data
    /// @param columns_to_deserialize List of columns to extract
    /// @param serializations Serialization objects for each column
    /// @param result_columns Output columns to append to
    void deserializeRow(
        const String & row_data,
        const NamesAndTypesList & columns_to_deserialize,
        const SerializationByName & serializations,
        MutableColumns & result_columns);

    /// Extract specific columns from serialized row data.
    /// Only deserializes the requested columns, skipping others.
    /// @param row_data The serialized row data
    /// @param all_columns_in_row All columns that were serialized in the row
    /// @param columns_to_extract Columns to actually extract
    /// @param serializations Serialization objects for each column
    /// @param result_columns Output columns to append to
    void extractColumns(
        const String & row_data,
        const NamesAndTypesList & all_columns_in_row,
        const NamesAndTypesList & columns_to_extract,
        const SerializationByName & serializations,
        MutableColumns & result_columns);

    /// Check if serialized row data is valid (magic number, version, checksum).
    bool isValid(const String & row_data) const;

    /// Get the number of columns stored in serialized row data.
    static size_t getNumColumns(const String & row_data);

private:
    Settings settings;

    /// Write header to buffer
    void writeHeader(WriteBuffer & buf, UInt16 num_columns, UInt8 flags = 0);

    /// Read and validate header from buffer
    /// @return true if header is valid, false otherwise
    bool readHeader(ReadBuffer & buf, UInt16 & num_columns, UInt8 & flags) const;

    /// Calculate CRC32 checksum
    static UInt32 calculateChecksum(const char * data, size_t size);

    /// Verify checksum of serialized data
    bool verifyChecksum(const String & data) const;
};

}
