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

    static void fillArrowArrayWithDateColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array,
                                                 const PaddedPODArray<UInt8> * null_bytemap);
    static void fillArrowArrayWithStringColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array,
                                                   const PaddedPODArray<UInt8> * null_bytemap);
    template <typename NumericType, typename ArrowBuilderType>
    static void fillArrowArrayWithNumericColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array,
                                                    const PaddedPODArray<UInt8> * null_bytemap);

    static const std::unordered_map<String, std::shared_ptr<arrow::DataType>> internal_type_to_arrow_type;
};

}
