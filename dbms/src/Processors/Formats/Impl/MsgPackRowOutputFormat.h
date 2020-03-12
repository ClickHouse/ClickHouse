#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <msgpack.hpp>


namespace DB
{

class MsgPackRowOutputFormat : public IRowOutputFormat
{
public:
    MsgPackRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & settings_);

    String getName() const override { return "MsgPackRowOutputFormat"; }

    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const IDataType &, size_t) override {}
    void serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num);

private:
    FormatSettings settings;
    msgpack::packer<DB::WriteBuffer> packer;
};

}
