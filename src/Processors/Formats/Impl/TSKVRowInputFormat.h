#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


/** Stream for reading data in TSKV format.
  * TSKV is a very inefficient data format.
  * Similar to TSV, but each field is written as key=value.
  * Fields can be listed in any order (including, in different lines there may be different order),
  *  and some fields may be missing.
  * An equal sign can be escaped in the field name.
  * Also, as an additional element there may be a useless tskv fragment - it needs to be ignored.
  */
class TSKVRowInputFormat final : public IRowInputFormat
{
public:
    TSKVRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "TSKVRowInputFormat"; }

    void resetParser() override;

private:
    void readPrefix() override;
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    const FormatSettings format_settings;

    /// Buffer for the read from the stream the field name. Used when you have to copy it.
    String name_buf;

    /// Hash table matching `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;

    /// Set of columns for which the values were read. The rest will be filled with default values.
    std::vector<UInt8> read_columns;
    /// Set of columns which already met in row. Exception is thrown if there are more than one column with the same name.
    std::vector<UInt8> seen_columns;
    /// These sets may be different, because if null_as_default=1 read_columns[i] will be false and seen_columns[i] will be true
    /// for row like ..., non-nullable column name=\N, ...
};

class TSKVSchemaReader : public IRowWithNamesSchemaReader
{
public:
    TSKVSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

private:
    std::unordered_map<String, DataTypePtr> readRowAndGetNamesAndDataTypes() override;

    const FormatSettings format_settings;
    bool first_row = true;
};

}
