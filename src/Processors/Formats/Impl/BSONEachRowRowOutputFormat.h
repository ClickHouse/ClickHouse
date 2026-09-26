#pragma once

#include <Core/NamesAndTypes.h>
#include <Formats/BSONTypes.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IRowOutputFormat.h>

namespace DB
{

class Block;

/*
 * Class for formatting data in BSON format.
 * Each row is formatted as a separate BSON document.
 * Each column is formatted as a single field with column name as a key.
 * It uses the following correspondence between ClickHouse types and BSON types:
 *
 * ClickHouse type         | BSON Type
 * Bool                    | \x08 boolean
 * Int8/UInt8/Enum8        | \x10 int32
 * Int16UInt16/Enum16      | \x10 int32
 * Int32                   | \x10 int32
 * UInt32                  | \x12 int64
 * Int64                   | \x12 int64
 * UInt64                  | \x11 uint64
 * Float32/Float64         | \x01 double
 * Date/Date32             | \x10 int32
 * DateTime                | \x12 int64
 * DateTime64              | \x09 datetime
 * Decimal32               | \x10 int32
 * Decimal64               | \x12 int64
 * Decimal128              | \x05 binary, \x00 binary subtype, size = 16
 * Decimal256              | \x05 binary, \x00 binary subtype, size = 32
 * Int128/UInt128          | \x05 binary, \x00 binary subtype, size = 16
 * Int256/UInt256          | \x05 binary, \x00 binary subtype, size = 32
 * String/FixedString      | \x05 binary, \x00 binary subtype or \x02 string if setting output_format_bson_string_as_string is enabled
 * UUID                    | \x05 binary, \x04 uuid subtype, size = 16
 * Array                   | \x04 array
 * Tuple                   | \x04 array
 * Named Tuple             | \x03 document
 * Map                     | \x03 document
 *
 * Note: on Big-Endian platforms this format will not work properly.
 */

class BSONEachRowRowOutputFormat final : public IRowOutputFormat
{
public:
    BSONEachRowRowOutputFormat(
        WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);

    String getName() const override { return "BSONEachRowRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override { }

    void serializeField(
        const IColumn & column,
        const DataTypePtr & data_type,
        size_t row_num,
        const String & name,
        const String & path,
        std::unordered_map<String, size_t> & nested_document_sizes);

    /// Count field size in bytes that we will get after serialization in BSON format.
    /// It's needed to calculate document size before actual serialization,
    /// because in BSON format we should write the size of the document before its content.
    size_t countBSONFieldSize(
        const IColumn & column,
        const DataTypePtr & data_type,
        size_t row_num,
        const String & name,
        const String & path,
        std::unordered_map<String, size_t> & nested_document_sizes);

    NamesAndTypes fields;
    FormatSettings settings;
};

}
