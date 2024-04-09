#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromVector.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Formats/FormatSettings.h>
#include <Columns/IColumn.h>
#include <Common/PODArray_fwd.h>

#include <vector>
#include <sstream>
#include <string>


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

    String getContentType() const override { return "application/octet-stream"; }

private:
    struct NumpyDataType
    {
      char endianness;
      char type;
      size_t size;

      NumpyDataType() = default;
      NumpyDataType(char endianness_, char type_, size_t size_)
        : endianness(endianness_), type(type_), size(size_) {}
      String str();
    };

    void initialize(const ColumnPtr & column);
    void consume(Chunk) override;
    void finalizeImpl() override;
    void writeHeader();
    void writeColumns();

    bool is_initialized = false;
    bool has_exception = false;

    DataTypePtr data_type;
    DataTypePtr nested_data_type;
    NumpyDataType numpy_data_type;
    UInt64 num_rows = 0;
    std::vector<UInt64> numpy_shape;
    Columns columns;

    /// static header (version 3.0)
    constexpr static auto STATIC_HEADER = "\x93NUMPY\x03\x00";
    constexpr static size_t STATIC_HEADER_LENGTH = 8;
};

}
