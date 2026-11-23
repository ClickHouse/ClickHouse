#pragma once

#include <DataTypes/IDataType.h>
#include <base/EnumReflection.h>

#include <string_view>
#include <vector>


namespace DB
{

class IColumn;

/// The payload elements of a histogram sample, in the canonical order after `timestamp`: the elements of the tuples of the
/// `histograms` column of the samples tables. A histogram is stored as it was sent:
/// an integer histogram keeps its counts in the `*_int` elements (`Int64`), a float histogram in the `*_float` elements (`Float64`),
/// the other flavour's scalars are 0 and its arrays are empty; `is_float` tells the flavour.
/// The buckets of a side are pairs of the bucket index and the count sorted by index (see TimeSeriesHistogramsColumns).
enum class TimeSeriesHistogramsColumn : uint8_t
{
    IsFloat,
    CounterResetHint,
    Schema,
    ZeroThreshold,
    Sum,
    CountInt,
    ZeroCountInt,
    CountFloat,
    ZeroCountFloat,
    PositiveInt,
    NegativeInt,
    PositiveFloat,
    NegativeFloat,
    CustomValues,
};

/// The values of the `counter_reset_hint` element: the numbers of `ResetHint` of the Prometheus remote-write protocol, named as
/// Prometheus writes the hint in the text form of a histogram (`counter_reset_hint:not_reset`). A counter hint describes the relation
/// to the preceding sample; the gauge marker describes the sample itself. The hints are computed by TimeSeriesHistogramCounterResetRule.
enum class TimeSeriesHistogramCounterResetHint : int8_t
{
    Unknown = 0,
    Reset = 1,
    NotReset = 2,
    Gauge = 3,
};

/// A codec declared for a subcolumn of the `histograms` column: `subcolumn` is relative to the column, e.g. `timestamp`,
/// `count_int` or `positive_int.index`, and `codec` is written as `CODEC(...)`.
struct TimeSeriesHistogramsCodecDeclaration
{
    String subcolumn;
    std::string_view codec;
};

/// The registry of the payload elements of the `histograms` column.
///
/// The buckets of a side are `Array(Tuple(index Int32, count Int64 or Float64))` sorted by `index`, the indexes unique, as sent
/// (empty buckets included). `index` is Prometheus's bucket index of the schema, for `schema = -53` the position among the custom buckets
/// (bucket k goes up to `custom_values[k]`, the last one to +Inf). The sides and the flavours are separate arrays so that each
/// data stream keeps one type and one codec.
namespace TimeSeriesHistogramsColumns
{
    /// All the payload elements in the canonical order.
    constexpr auto getAll()
    {
        return magic_enum::enum_values<TimeSeriesHistogramsColumn>();
    }

    std::string_view getName(TimeSeriesHistogramsColumn column);
    std::string_view getType(TimeSeriesHistogramsColumn column);

    /// The positions of the elements of a pair of the bucket index and the count.
    constexpr size_t getPairIndexPosition() { return 0; }
    constexpr size_t getPairCountPosition() { return 1; }

    /// The codecs declared for the subcolumns of the generated `histograms` column, in the order of the elements: `timestamp`, then
    /// the scalar elements and `custom_values` by name, and the `index` and `count` subcolumns of each array of pairs. Built once.
    const std::vector<TimeSeriesHistogramsCodecDeclaration> & getCodecDeclarations();

    /// The data type of a payload element, parsed from its definition once.
    const DataTypePtr & getDataType(TimeSeriesHistogramsColumn column);

    /// The canonical empty column of a payload element, created from its data type once: the structure (column classes, recursively)
    /// every column holding that element must have, for `IColumn::structureEquals` checks.
    const IColumn & getEmptyColumn(TimeSeriesHistogramsColumn column);

    /// The name and the position of the `timestamp` element in the stored tuple; the payload elements follow in the order of the enum.
    constexpr std::string_view getTimestampName() { return "timestamp"; }
    constexpr size_t getTimestampPosition() { return 0; }

    /// The position of a payload element in the stored tuple.
    constexpr size_t getTuplePosition(TimeSeriesHistogramsColumn column) { return getTimestampPosition() + 1 + static_cast<size_t>(column); }

    /// The type of the `histograms` column of the samples tables (without the `SimpleAggregateFunction` wrapper), of the outer column
    /// `histograms` of a TimeSeries table and of `timeSeriesSelector`:
    /// `Array(Tuple(timestamp <timestamp_type>, is_float Bool, counter_reset_hint Enum8(...), ..., custom_values Array(Float64)))`.
    DataTypePtr makeHistogramsArrayType(const DataTypePtr & timestamp_type);

    /// If `type` is `Array(Tuple(timestamp, <the payload elements>))` (optionally wrapped in `SimpleAggregateFunction`),
    /// returns the type of the timestamps, otherwise null.
    DataTypePtr tryGetTimestampType(const DataTypePtr & type);

    /// The helpers below render the registry for the documentation of the TimeSeries engine (see `registerStorageTimeSeries`), so that
    /// the names, the types and the descriptions of the histogram columns are written once. The timestamps have the default type.

    /// The type of `makeHistogramsArrayType` with the default timestamp type, as text.
    String describeHistogramsArrayTypeForDocs();

    /// The rows of the markdown table of the elements of the histogram tuple (name, type, description), each ending with a newline.
    String describeHistogramsElementsForDocs();
}

}
