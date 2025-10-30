#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/NumpyDataTypes.h>
#include <Columns/IColumn_fwd.h>

#include <vector>


namespace DB
{

/** Stream for output data in Npy format.
  * https://numpy.org/doc/stable/reference/generated/numpy.lib.format.html
  */
class NpyOutputFormat : public IOutputFormat
{
public:
    NpyOutputFormat(WriteBuffer & out_, const Block & header_);

    String getName() const override { return "NpyOutputFormat"; }

private:
    String shapeStr() const;

    bool getNumpyDataType(const DataTypePtr & type);

    void consume(Chunk) override;
    void initShape(const ColumnPtr & column);
    void checkShape(ColumnPtr & column);
    void updateSizeIfTypeString(const ColumnPtr & column);

    void finalizeImpl() override;
    void writeHeader();
    void writeColumns();

    bool is_initialized = false;
    bool invalid_shape = false;

    DataTypePtr data_type;
    DataTypePtr nested_data_type;
    std::shared_ptr<NumpyDataType> numpy_data_type;
    UInt64 num_rows = 0;
    std::vector<UInt64> numpy_shape;
    Columns columns;

    /// static header (version 3.0)
    constexpr static auto STATIC_HEADER = "\x93NUMPY\x03\x00";
    constexpr static size_t STATIC_HEADER_LENGTH = 8;
};

}
