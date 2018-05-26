#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{


class ParquetBlockOutputStream : public IBlockOutputStream
{
public:
    ParquetBlockOutputStream(WriteBuffer & ostr_, const Block & header_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void flush() override;

    String getContentType() const override { return "application/octet-stream"; }

private:
    WriteBuffer & ostr;
    Block header;

    template <typename NumericType, typename ArrowBuilderType>
    void fillArrowArrayWithNumericColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array);

    void fillArrowArrayWithDateColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array);
    void fillArrowArrayWithStringColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array);

    std::unordered_map<String, std::shared_ptr<arrow::DataType>> internal_type_to_arrow_type = {
        {"UInt8",   arrow::uint8()},
        {"Int8",    arrow::int8()},
        {"UInt16",  arrow::uint16()},
        {"Int16",   arrow::int16()},
        {"UInt32",  arrow::uint32()},
        {"Int32",   arrow::int32()},
        {"UInt64",  arrow::uint64()},
        {"Int64",   arrow::int64()},
        {"Float32", arrow::float32()},
        {"Float64", arrow::float64()},

        {"Date",  arrow::date32()},

        {"String", arrow::utf8()}//,
        // TODO: add other types:
        // 1. FixedString
        // 2. DateTime
    };
};

}
