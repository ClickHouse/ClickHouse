#pragma once

#include "config_formats.h"
#include "config_core.h"

#if USE_MSGPACK

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <msgpack.hpp>


namespace DB
{

class MsgPackRowOutputFormat final : public IRowOutputFormat
{
public:
    MsgPackRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_);

    String getName() const override { return "MsgPackRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num);

    msgpack::packer<DB::WriteBuffer> packer;
    const FormatSettings format_settings;
};

}

#endif
