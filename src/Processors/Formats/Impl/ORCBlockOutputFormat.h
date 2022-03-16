#pragma once

#include "config_formats.h"

#if USE_ORC
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <orc/OrcFile.hh>

namespace DB
{

class WriteBuffer;

/// orc::Writer writes only in orc::OutputStream
class ORCOutputStream : public orc::OutputStream
{
public:
    explicit ORCOutputStream(WriteBuffer & out_);

    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    void write(const void* buf, size_t length) override;

    void close() override {}
    const std::string& getName() const override { return name; }

private:
    WriteBuffer & out;
    std::string name = "ORCOutputStream";
};

class ORCBlockOutputFormat : public IOutputFormat
{
public:
    ORCBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockOutputFormat"; }

private:
    void consume(Chunk chunk) override;
    void finalizeImpl() override;

    ORC_UNIQUE_PTR<orc::Type> getORCType(const DataTypePtr & type, const std::string & column_name);

    /// ConvertFunc is needed for type UInt8, because firstly UInt8 (char8_t) must be
    /// converted to unsigned char (bugprone-signed-char-misuse in clang).
    template <typename NumberType, typename NumberVectorBatch, typename ConvertFunc>
    void writeNumbers(orc::ColumnVectorBatch & orc_column, const IColumn & column, const PaddedPODArray<UInt8> * null_bytemap, ConvertFunc convert);

    /// ConvertFunc is needed to convert ClickHouse Int128 to ORC Int128.
    template <typename Decimal, typename DecimalVectorBatch, typename ConvertFunc>
    void writeDecimals(orc::ColumnVectorBatch & orc_column, const IColumn & column, DataTypePtr & type,
                        const PaddedPODArray<UInt8> * null_bytemap, ConvertFunc convert);

    template <typename ColumnType>
    void writeStrings(orc::ColumnVectorBatch & orc_column, const IColumn & column, const PaddedPODArray<UInt8> * null_bytemap);

    /// ORC column TimestampVectorBatch stores only seconds and nanoseconds,
    /// GetSecondsFunc and GetNanosecondsFunc are needed to extract them from DataTime type.
    template <typename ColumnType, typename GetSecondsFunc, typename GetNanosecondsFunc>
    void writeDateTimes(orc::ColumnVectorBatch & orc_column, const IColumn & column, const PaddedPODArray<UInt8> * null_bytemap,
                        GetSecondsFunc get_seconds, GetNanosecondsFunc get_nanoseconds);

    void writeColumn(orc::ColumnVectorBatch & orc_column, const IColumn & column, DataTypePtr & type, const PaddedPODArray<UInt8> * null_bytemap);

    /// These two functions are needed to know maximum nested size of arrays to
    /// create an ORC Batch with the appropriate size
    size_t getColumnSize(const IColumn & column, DataTypePtr & type);
    size_t getMaxColumnSize(Chunk & chunk);

    void prepareWriter();

    const FormatSettings format_settings;
    ORCOutputStream output_stream;
    DataTypes data_types;
    ORC_UNIQUE_PTR<orc::Writer> writer;
    ORC_UNIQUE_PTR<orc::Type> schema;
    orc::WriterOptions options;
};

}
#endif
