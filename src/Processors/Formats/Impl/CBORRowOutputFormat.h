#pragma once

#include <Processors/Formats/IRowOutputFormat.h>

#ifdef USE_CBOR

#include <cbor.h>

namespace DB 
{

class CBOROutput final : public cbor::output
{
public:
    CBOROutput(WriteBuffer & buf);

    unsigned char *data() override;

    unsigned int size() override;

    void put_byte(unsigned char value) override;

    void put_bytes(const unsigned char *data, int size) override;

private:
    WriteBuffer & buffer;
}; 

class CBORRowOutputFormat final : public IRowOutputFormat 
{
public:
    CBORRowOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & format_settings_);

    String getName() const override { return "CBORRowOutputFormat"; }

private:
    void writePrefix() override;
    void writeSuffix() override;
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void serializeField(const IColumn & column, DataTypePtr data_type, size_t row_num);

    CBOROutput cbor_out;
    const FormatSettings format_settings;
    cbor::encoder encoder;
};

}

#endif
