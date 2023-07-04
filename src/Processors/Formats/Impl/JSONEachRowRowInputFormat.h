#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


/** A stream for reading data in JSON format, where each row is represented by a separate JSON object.
  * Objects can be separated by line feed, other whitespace characters in any number and possibly a comma.
  * Fields can be listed in any order (including, in different lines there may be different order),
  *  and some fields may be missing.
  */
class JSONEachRowRowInputFormat final : public IRowInputFormat
{
public:
    JSONEachRowRowInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        Params params_,
        const FormatSettings & format_settings_,
        bool yield_strings_);

    String getName() const override { return "JSONEachRowRowInputFormat"; }
    void resetParser() override;

private:
    void readPrefix() override;
    void readSuffix() override;

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    const String & columnName(size_t i) const;
    size_t columnIndex(StringRef name, size_t key_index);
    bool advanceToNextKey(size_t key_index);
    void skipUnknownField(StringRef name_ref);
    StringRef readColumnName(ReadBuffer & buf);
    void readField(size_t index, MutableColumns & columns);
    void readJSONObject(MutableColumns & columns);
    void readNestedData(const String & name, MutableColumns & columns);

    const FormatSettings format_settings;

    /// Buffer for the read from the stream field name. Used when you have to copy it.
    /// Also, if processing of Nested data is in progress, it holds the common prefix
    /// of the nested column names (so that appending the field name to it produces
    /// the full column name)
    String current_column_name;

    /// If processing Nested data, holds the length of the common prefix
    /// of the names of related nested columns. For example, for a table
    /// created as follows
    ///        CREATE TABLE t (n Nested (i Int32, s String))
    /// the nested column names are 'n.i' and 'n.s' and the nested prefix is 'n.'
    size_t nested_prefix_length = 0;

    /// Set of columns for which the values were read. The rest will be filled with default values.
    std::vector<UInt8> read_columns;
    /// Set of columns which already met in row. Exception is thrown if there are more than one column with the same name.
    std::vector<UInt8> seen_columns;
    /// These sets may be different, because if null_as_default=1 read_columns[i] will be false and seen_columns[i] will be true
    /// for row like {..., "non-nullable column name" : null, ...}

    /// Hash table match `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;

    /// Cached search results for previous row (keyed as index in JSON object) - used as a hint.
    std::vector<NameMap::LookupResult> prev_positions;

    /// This flag is needed to know if data is in square brackets.
    bool data_in_square_brackets = false;

    bool allow_new_rows = true;

    bool yield_strings;
};

class JSONEachRowSchemaReader : public IRowWithNamesSchemaReader
{
public:
    JSONEachRowSchemaReader(ReadBuffer & in_, bool json_strings, const FormatSettings & format_settings_);

private:
    NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) override;
    void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) override;

    bool json_strings;
    bool first_row = true;
    bool data_in_square_brackets = false;
};

}
