#pragma once

#include <cstddef>
#include <string_view>

#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/IDataType_fwd.h>


namespace DB
{

class IColumn;
class ReadBuffer;
class WriteBuffer;

/// The names used in the error messages.
struct TimeseriesScalarOrGridArgumentNames
{
    std::string_view function_name;
    std::string_view argument_name;
};

/// Throws `ILLEGAL_TYPE_OF_ARGUMENT` unless `type` is a native number or an array of native numbers.
void checkTimeseriesScalarOrGridArgumentType(TimeseriesScalarOrGridArgumentNames names, const DataTypePtr & type);

/// An extra argument of a time series aggregate function: a number for the whole grid or an array with one number per
/// grid point. It is captured from the first added row and must be the same in every other row.
class AggregateFunctionTimeseriesScalarOrGridArgument
{
public:
    /// Captures the argument from the rows `[row_begin, row_end)` of `column` if nothing has been captured yet, and throws
    /// if a row carries another value. `flags` and `flag_value_to_include` select the rows, as in `addExtraArguments`.
    void captureOrCheck(TimeseriesScalarOrGridArgumentNames names, size_t grid_size, size_t row_begin, size_t row_end, const IColumn & column, const UInt8 * flags, bool flag_value_to_include);

    void merge(TimeseriesScalarOrGridArgumentNames names, const AggregateFunctionTimeseriesScalarOrGridArgument & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(TimeseriesScalarOrGridArgumentNames names, ReadBuffer & buf, size_t grid_size);

    /// The value at grid point `grid_index`, or 0 if no row has been added.
    [[nodiscard]] Float64 at(size_t grid_index) const;

private:
    /// One value if the argument is the same at every grid point, `grid_size` values otherwise. Empty until a row is added.
    VectorWithMemoryTracking<Float64> values;
};

}
