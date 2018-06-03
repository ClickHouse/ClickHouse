#pragma once

#include <Core/Block.h>
#include <DataStreams/IRowInputStream.h>
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
    JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & header_, bool skip_unknown_);

    bool read(MutableColumns & columns) override;
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

private:
    ReadBuffer & istr;
    Block header;
    bool skip_unknown;

    /// Buffer for the read from the stream field name. Used when you have to copy it.
    String name_buf;

    /// Hash table match `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap name_map;
};

}
