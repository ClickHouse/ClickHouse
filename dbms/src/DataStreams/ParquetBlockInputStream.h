#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
// TODO: refine includes
#include <arrow/api.h>
/* #include <DataStreams/MarkInCompressedFile.h> */
/* #include <Common/PODArray.h> */

namespace DB
{

// TODO: move a common parts for parquet and arrow to smth like ArrowBlockInputStream
class ParquetBlockInputStream : public IProfilingBlockInputStream
{
public:
    ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_);

    String getName() const override { return "Parquet"; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;

    template <typename NumericType>
    void fillColumnWithNumericData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);

    void fillColumnWithStringData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column);

    std::unordered_map<arrow::Type::type, std::shared_ptr<IDataType>> arrow_type_to_internal_type = {
        {arrow::Type::UINT8,  std::make_shared<DataTypeUInt8>()},
        {arrow::Type::INT8,   std::make_shared<DataTypeInt8>()},
        {arrow::Type::UINT16, std::make_shared<DataTypeUInt16>()},
        {arrow::Type::INT16,  std::make_shared<DataTypeInt16>()},
        {arrow::Type::UINT32, std::make_shared<DataTypeUInt32>()},
        {arrow::Type::INT32,  std::make_shared<DataTypeInt32>()},
        {arrow::Type::UINT64, std::make_shared<DataTypeUInt64>()},
        {arrow::Type::INT64,  std::make_shared<DataTypeInt64>()},
        {arrow::Type::FLOAT,  std::make_shared<DataTypeFloat32>()},
        {arrow::Type::DOUBLE, std::make_shared<DataTypeFloat64>()},

        {arrow::Type::STRING, std::make_shared<DataTypeString>()}//,
        // TODO:
        /* {arrow::Type::BOOL,   std::make_shared<DataTypeUInt8>()}, */
        /* {arrow::Type::DATE32, Date32, Int32Type}, */
        /* {arrow::Type::DATE64, Date64, Int32Type}//, */
        // TODO: add other types
    };

    // TODO: check that this class implements every part of its parent
};

}
