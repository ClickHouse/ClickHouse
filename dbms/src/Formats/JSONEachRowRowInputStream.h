#pragma once

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


/** A stream for reading data in JSON format, where each row is represented by a separate JSON object.
  * Objects can be separated by feed return, other whitespace characters in any number and possibly a comma.
  * Fields can be listed in any order (including, in different lines there may be different order),
  *  and some fields may be missing.
  */
class JSONEachRowRowInputStream : public IRowInputStream
{
public:
    JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & header_, const FormatSettings & format_settings);

    bool read(MutableColumns & columns) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    const String & columnName(size_t i) const;
    size_t columnIndex(const StringRef & name) const;
    bool advanceToNextKey(size_t key_index);
    void skipUnknownField(const StringRef & name_ref);
    StringRef readColumnName(ReadBuffer & buf);
    void readField(size_t index, MutableColumns & columns);
    void readJSONObject(MutableColumns & columns);
    void readNestedData(const String & name, MutableColumns & columns);

private:
    ReadBuffer & istr;
    Block header;

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

    std::vector<UInt8> read_columns;

    /// Hash table match `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;
};

}
