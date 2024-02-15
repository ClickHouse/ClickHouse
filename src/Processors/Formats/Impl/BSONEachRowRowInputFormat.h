#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <Formats/BSONTypes.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

/*
 * Class for parsing data in BSON format.
 * Each row is parsed as a separate BSON document.
 * Each column is parsed as a single field with column name as a key.
 * It uses the following correspondence between BSON types and ClickHouse types:
 *
 * BSON Type                                   | ClickHouse Type
 * \x01 double                                 | Float32/Float64
 * \x02 string                                 | String/FixedString
 * \x03 document                               | Map/Named Tuple
 * \x04 array                                  | Array/Tuple
 * \x05 binary, \x00 binary subtype            | String/FixedString
 * \x05 binary, \x02 old binary subtype        | String/FixedString
 * \x05 binary, \x03 old uuid subtype          | UUID
 * \x05 binary, \x04 uuid subtype              | UUID
 * \x07 ObjectId                               | String
 * \x08 boolean                                | Bool
 * \x09 datetime                               | DateTime64
 * \x0A null value                             | NULL
 * \x0D JavaScript code                        | String
 * \x0E symbol                                 | String/FixedString
 * \x10 int32                                  | Int32/Decimal32
 * \x12 int64                                  | Int64/Decimal64/DateTime64
 * \x11 uint64                                 | UInt64
 *
 * Other BSON types are not supported.
 * Also, we perform conversion between different integer types
 * (for example, you can insert BSON int32 value into ClickHouse UInt8)
 * Big integers and decimals Int128/UInt128/Int256/UInt256/Decimal128/Decimal256
 * can be parsed from BSON Binary value with \x00 binary subtype. In this case
 * we validate that the size of binary data equals the size of expected value.
 *
 * Note: this format will not work on Big-Endian platforms.
 */

class ReadBuffer;
class BSONEachRowRowInputFormat final : public IRowInputFormat
{
public:
    BSONEachRowRowInputFormat(
        ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "BSONEachRowRowInputFormat"; }
    void resetParser() override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    size_t columnIndex(const StringRef & name, size_t key_index);

    using ColumnReader = std::function<void(StringRef name, BSONType type)>;

    bool readField(IColumn & column, const DataTypePtr & data_type, BSONType bson_type);
    void skipUnknownField(BSONType type, const String & key_name);

    void readTuple(IColumn & column, const DataTypePtr & data_type, BSONType bson_type);
    void readArray(IColumn & column, const DataTypePtr & data_type, BSONType bson_type);
    void readMap(IColumn & column, const DataTypePtr & data_type, BSONType bson_type);

    const FormatSettings format_settings;

    /// Buffer for the read from the stream field name. Used when you have to copy it.
    String current_key_name;

    /// Set of columns for which the values were read. The rest will be filled with default values.
    std::vector<UInt8> read_columns;
    /// Set of columns which already met in row. Exception is thrown if there are more than one column with the same name.
    std::vector<UInt8> seen_columns;
    /// These sets may be different, because if null_as_default=1 read_columns[i] will be false and seen_columns[i] will be true
    /// for row like {..., "non-nullable column name" : null, ...}

    /// Hash table match `field name -> position in the block`.
    Block::NameMap name_map;

    /// Cached search results for previous row (keyed as index in JSON object) - used as a hint.
    std::vector<Block::NameMap::const_iterator> prev_positions;

    DataTypes types;

    size_t current_document_start;
    BSONSizeT current_document_size;
};

class BSONEachRowSchemaReader : public IRowWithNamesSchemaReader
{
public:
    BSONEachRowSchemaReader(ReadBuffer & in_, const FormatSettings & settings_);

private:
    NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) override;
    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    NamesAndTypesList getDataTypesFromBSONDocument(bool skip_unsupported_types);
    DataTypePtr getDataTypeFromBSONField(BSONType type, bool skip_unsupported_types, bool & skip);
};

}
