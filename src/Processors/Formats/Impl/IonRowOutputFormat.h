#pragma once

#include "config.h"

#if USE_ION

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <ionc/ion.h>
#include "Formats/IonWriter.h"

namespace DB
{

class IonRowOutputFormat final : public IRowOutputFormat
{
public:
    IonRowOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "IonRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writePrefix() override;
    void writeSuffix() override;
    void serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num);
    void finalizeImpl() override;

    const FormatSettings format_settings;
    std::unique_ptr<IonWriter> writer;
    Names names;
};

}

#endif
