#pragma once

#include <Processors/Formats/IRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/PeekableReadBuffer.h>
#include <msgpack.hpp>

namespace DB
{

class ReadBuffer;

class MsgPackRowInputFormat : public IRowInputFormat
{
public:
    MsgPackRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "MagPackRowInputFormat"; }
private:
    bool readObject();
    void insertObject(IColumn & column, DataTypePtr type, const msgpack::object & object);
    int unpack(msgpack::zone & zone, size_t & offset);

    // msgpack makes a copy of object by default, this function tells unpacker not to copy.
    static bool reference_func(msgpack::type::object_type, size_t, void *) { return true; }

    PeekableReadBuffer buf;
    msgpack::object_handle object_handle;
    msgpack::v1::detail::context ctx;
    DataTypes data_types;
};

}
