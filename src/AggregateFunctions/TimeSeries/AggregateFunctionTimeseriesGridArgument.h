#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <string_view>


namespace DB
{

class IColumn;
class ReadBuffer;
class WriteBuffer;

/// An argument after the samples of a timeSeries*ToGrid function: a number or an array with one number per grid point.
/// It is taken from the first added row and must be the same in every other row.
class AggregateFunctionTimeseriesGridArgument
{
public:
    /// Takes the value from the first included row if none is taken yet, then checks the other included rows.
    /// A row is included when `flags` is nullptr or its flag equals `flag_value_to_include`.
    void captureOrCheck(
        std::string_view function_name, std::string_view argument_name, size_t grid_size, size_t row_begin, size_t row_end,
        const IColumn & column, const UInt8 * flags, bool flag_value_to_include);

    void merge(std::string_view function_name, std::string_view argument_name, const AggregateFunctionTimeseriesGridArgument & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(std::string_view argument_name, ReadBuffer & buf, size_t grid_size);

    /// The value at grid point `grid_index`. It is 0 if no row has been added, then every window is empty anyway.
    Float64 at(size_t grid_index) const;

private:
    /// One value if the argument is the same at every grid point, `grid_size` values otherwise. Empty until a row is added.
    VectorWithMemoryTracking<Float64> values;
};

}
