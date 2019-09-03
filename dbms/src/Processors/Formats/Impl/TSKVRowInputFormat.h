#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
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
class TSKVRowInputFormat : public IRowInputFormat
{
public:
    TSKVRowInputFormat(ReadBuffer & in_, Block header_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "TSKVRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    const FormatSettings format_settings;

    /// Buffer for the read from the stream the field name. Used when you have to copy it.
    String name_buf;

    /// Hash table matching `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;

    std::vector<UInt8> read_columns;
};

}
