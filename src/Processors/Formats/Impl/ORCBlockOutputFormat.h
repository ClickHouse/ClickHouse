#pragma once

#include <IO/WriteBuffer.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <orc/OrcFile.hh>

namespace DB
{

class WriteBuffer;

class ORCOutputStream : public orc::OutputStream
{
public:
    ORCOutputStream(WriteBuffer & out_);

    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    void write(const void* buf, size_t length) override;

    void close() override {};
    const std::string& getName() const override { return "ORCOutputStream"; };

private:
    WriteBuffer & out;
};

class ORCBlockOutputFormat : public IOutputFormat
{
public:
    ORCBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockOutputFormat"; }
    void consume(Chunk chunk) override;
    void finalize() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    ORC_UNIQUE_PTR<orc::Type> getORCType(const DataTypePtr & type);
    template <typename Decimal, typename DecimalVectorBatch, typename ConvertFunc>
    void writeDecimals(
            orc::ColumnVectorBatch * orc_column,
            const IColumn & column,
            DataTypePtr & type,
            const PaddedPODArray<UInt8> * null_bytemap,
            size_t rows_num,
            ConvertFunc convert);
    template <typename NumberType, typename NumberVectorBatch>
    void writeNumbers(
            orc::ColumnVectorBatch * orc_column,
            const IColumn & column,
            const PaddedPODArray<UInt8> * null_bytemap,
            size_t rows_num);
    void writeColumn(
            orc::ColumnVectorBatch * orc_column,
            const IColumn & column, DataTypePtr & type,
            const PaddedPODArray<UInt8> * null_bytemap,
            size_t rows_num);

    const FormatSettings format_settings;
    ORCOutputStream output_stream;
    DataTypes data_types;
    ORC_UNIQUE_PTR<orc::Writer> writer;
    ORC_UNIQUE_PTR<orc::Type> schema;
    orc::WriterOptions options;
};

}
