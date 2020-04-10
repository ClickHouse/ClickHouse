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

    PeekableReadBuffer buf;
    DataTypes data_types;
    msgpack::object_handle object_handle;
};

}
