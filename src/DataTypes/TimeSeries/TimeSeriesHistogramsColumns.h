#pragma once

#include <DataTypes/IDataType.h>
#include <base/EnumReflection.h>

#include <string_view>
#include <vector>


namespace DB
{

class IColumn;

/// The payload elements of a histogram sample, in the canonical order after `timestamp`: the elements of the tuples of the
/// `histograms` column of the samples tables (see TimeSeriesColumnNames::Histograms). A histogram is stored as it was sent:
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
struct TimeSeriesHistogramCounterResetHint
{
    static constexpr Int8 Unknown = 0;
    static constexpr Int8 Reset = 1;
    static constexpr Int8 NotReset = 2;
    static constexpr Int8 Gauge = 3;

    /// No value may be below `Unknown`: an `Enum` inserts its smallest value as the default, while its column inserts 0.
    static constexpr std::string_view getTypeName() { return "Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3)"; }
};

/// The name, the type and the codec of an element of the histogram tuple, and its description for the documentation.
/// This is the source of truth for generating the column, recognizing the generated one, validating the samples, and documenting them.
/// `codec` is empty for the arrays of pairs: their codecs are declared for their `index` and `count` subcolumns, which keep one type
/// and one data stream each (see TimeSeriesHistogramsColumns::getCodecDeclarations).
struct TimeSeriesHistogramsColumnDefinition
{
    std::string_view name;
    std::string_view type;
    std::string_view codec;
    std::string_view description;
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
class TimeSeriesHistogramsColumns
{
public:
    /// All the payload elements in the canonical order.
    static constexpr auto getAll()
    {
        return magic_enum::enum_values<TimeSeriesHistogramsColumn>();
    }

    /// The codecs are declared for the subcolumns of the generated `histograms` column (`CODEC histograms.<name> (...)`). The counts
    /// are stored as sent: `T64` bit-packs the integer counts, the float counts get the general purpose codec only (`ALP` doesn't help
    /// on absolute counts), the bucket indexes of the pairs get `Delta` (see `getPairIndexCodec`).
    static constexpr TimeSeriesHistogramsColumnDefinition getDefinition(TimeSeriesHistogramsColumn column)
    {
        switch (column)
        {
            case TimeSeriesHistogramsColumn::IsFloat:
                return {"is_float", "Bool", "CODEC(ZSTD(3))",
                    "Whether the histogram is a float histogram, with its counts in the `*_float` elements; an integer histogram has its counts "
                    "in the `*_int` elements. The readers ignore the elements of the other flavour, which remote write leaves 0 and empty"};
            case TimeSeriesHistogramsColumn::CounterResetHint:
                return {"counter_reset_hint", TimeSeriesHistogramCounterResetHint::getTypeName(), "CODEC(ZSTD(3))",
                    "The counter reset hint: `unknown`, `reset` (a counter reset happened), `not_reset` (no counter reset) or `gauge` "
                    "(a gauge histogram)"};
            case TimeSeriesHistogramsColumn::Schema:
                return {"schema", "Int8", "CODEC(ZSTD(3))",
                    "The bucket schema: from -4 to 8 for exponential buckets, -53 for custom buckets"};
            case TimeSeriesHistogramsColumn::ZeroThreshold:
                return {"zero_threshold", "Float64", "CODEC(ZSTD(3))",
                    "The width of the zero bucket"};
            case TimeSeriesHistogramsColumn::Sum:
                return {"sum", "Float64", "CODEC(ZSTD(3))",
                    "The sum of the observations; the stale marker `NaN` when the series went stale"};
            case TimeSeriesHistogramsColumn::CountInt:
                return {"count_int", "Int64", "CODEC(T64, ZSTD(3))",
                    "The total count of the observations of an integer histogram, 0 for a float histogram"};
            case TimeSeriesHistogramsColumn::ZeroCountInt:
                return {"zero_count_int", "Int64", "CODEC(T64, ZSTD(3))",
                    "The count of the zero bucket of an integer histogram, 0 for a float histogram"};
            case TimeSeriesHistogramsColumn::CountFloat:
                return {"count_float", "Float64", "CODEC(ZSTD(3))",
                    "The total count of the observations of a float histogram, 0 for an integer histogram"};
            case TimeSeriesHistogramsColumn::ZeroCountFloat:
                return {"zero_count_float", "Float64", "CODEC(ZSTD(3))",
                    "The count of the zero bucket of a float histogram, 0 for an integer histogram"};
            case TimeSeriesHistogramsColumn::PositiveInt:
                return {"positive_int", "Array(Tuple(index Int32, count Int64))", "",
                    "The positive buckets of an integer histogram as pairs of the bucket index and the count, sorted by index; "
                    "empty for a float histogram"};
            case TimeSeriesHistogramsColumn::NegativeInt:
                return {"negative_int", "Array(Tuple(index Int32, count Int64))", "",
                    "The negative buckets of an integer histogram as pairs of the bucket index and the count, sorted by index; "
                    "empty for a float histogram"};
            case TimeSeriesHistogramsColumn::PositiveFloat:
                return {"positive_float", "Array(Tuple(index Int32, count Float64))", "",
                    "The positive buckets of a float histogram as pairs of the bucket index and the count, sorted by index; "
                    "empty for an integer histogram"};
            case TimeSeriesHistogramsColumn::NegativeFloat:
                return {"negative_float", "Array(Tuple(index Int32, count Float64))", "",
                    "The negative buckets of a float histogram as pairs of the bucket index and the count, sorted by index; "
                    "empty for an integer histogram"};
            case TimeSeriesHistogramsColumn::CustomValues:
                return {"custom_values", "Array(Float64)", "CODEC(ZSTD(3))",
                    "The upper bounds of the custom buckets (`schema = -53`), empty otherwise"};
        }
    }

    static constexpr std::string_view getName(TimeSeriesHistogramsColumn column) { return getDefinition(column).name; }
    static constexpr std::string_view getType(TimeSeriesHistogramsColumn column) { return getDefinition(column).type; }
    static constexpr std::string_view getDescription(TimeSeriesHistogramsColumn column) { return getDefinition(column).description; }

    /// Whether the element is one of the four arrays of pairs of the bucket index and the count.
    static constexpr bool isPairs(TimeSeriesHistogramsColumn column)
    {
        return (column == TimeSeriesHistogramsColumn::PositiveInt) || (column == TimeSeriesHistogramsColumn::NegativeInt)
            || (column == TimeSeriesHistogramsColumn::PositiveFloat) || (column == TimeSeriesHistogramsColumn::NegativeFloat);
    }

    /// Whether the element holds the counts of the float flavour (`count_float`, `zero_count_float`, `positive_float`, `negative_float`).
    static constexpr bool isFloatFlavour(TimeSeriesHistogramsColumn column)
    {
        return (column == TimeSeriesHistogramsColumn::CountFloat) || (column == TimeSeriesHistogramsColumn::ZeroCountFloat)
            || (column == TimeSeriesHistogramsColumn::PositiveFloat) || (column == TimeSeriesHistogramsColumn::NegativeFloat);
    }

    /// The elements of a pair: the first one is the bucket index, the second one the count.
    static constexpr std::string_view getPairIndexName() { return "index"; }
    static constexpr std::string_view getPairCountName() { return "count"; }
    static constexpr size_t getPairIndexPosition() { return 0; }
    static constexpr size_t getPairCountPosition() { return 1; }

    /// The codec of the `timestamp` element: near-monotonic timestamps barely compress under generic codecs.
    static constexpr std::string_view getTimestampCodec() { return "CODEC(Delta, T64, ZSTD(3))"; }

    /// The codecs of the subcolumns of an array of pairs. The indexes of one histogram increase, so `Delta` makes them small.
    static constexpr std::string_view getPairIndexCodec() { return "CODEC(Delta, ZSTD(3))"; }
    static constexpr std::string_view getPairCountCodec(TimeSeriesHistogramsColumn column)
    {
        return isFloatFlavour(column) ? "CODEC(ZSTD(3))" : "CODEC(T64, ZSTD(3))";
    }

    /// The codecs declared for the subcolumns of the generated `histograms` column, in the order of the elements: `timestamp`, then
    /// the scalar elements and `custom_values` by name, and the `index` and `count` subcolumns of each array of pairs. Built once.
    static const std::vector<TimeSeriesHistogramsCodecDeclaration> & getCodecDeclarations();

    /// The data type of a payload element, parsed from its definition once.
    static const DataTypePtr & getDataType(TimeSeriesHistogramsColumn column);

    /// The canonical empty column of a payload element, created from its data type once: the structure (column classes, recursively)
    /// every column holding that element must have, for `IColumn::structureEquals` checks.
    static const IColumn & getEmptyColumn(TimeSeriesHistogramsColumn column);

    /// The position of the `timestamp` element in the stored tuple; the payload elements follow in the order of the enum.
    static constexpr size_t getTimestampPosition() { return 0; }

    /// The position of a payload element in the stored tuple.
    static constexpr size_t getTuplePosition(TimeSeriesHistogramsColumn column) { return getTimestampPosition() + 1 + static_cast<size_t>(column); }

    /// The type of the `histograms` column of the samples tables (without the `SimpleAggregateFunction` wrapper), of the outer column
    /// `histograms` of a TimeSeries table and of `timeSeriesSelector`:
    /// `Array(Tuple(timestamp <timestamp_type>, is_float Bool, counter_reset_hint Enum8(...), ..., custom_values Array(Float64)))`.
    static DataTypePtr makeHistogramsArrayType(const DataTypePtr & timestamp_type);

    /// If `type` is `Array(Tuple(timestamp, <the payload elements>))` (optionally wrapped in `SimpleAggregateFunction`),
    /// returns the type of the timestamps, otherwise null.
    static DataTypePtr tryGetTimestampType(const DataTypePtr & type);

    /// The helpers below render the registry for the documentation of the TimeSeries engine (see `registerStorageTimeSeries`), so that
    /// the names, the types, the codecs and the descriptions of the histogram columns are written once. `timestamp_type_text` is the
    /// type of the timestamps as the documentation writes it, e.g. `DateTime64(3)`.

    /// The type of `makeHistogramsArrayType` as text: `Array(Tuple(timestamp <timestamp_type>, is_float Bool, ..., custom_values Array(Float64)))`.
    static String describeArrayTypeForDocs(std::string_view timestamp_type_text);

    /// The rows of the markdown table of the elements of the histogram tuple (name, type, description), each ending with a newline.
    /// The type of `timestamp` is marked as the default one, and `timestamp_description` is its description: the timestamps aren't in
    /// the registry.
    static String describeElementsForDocs(std::string_view timestamp_type_text, std::string_view timestamp_description);

    /// The codec declarations of the `histograms` column grouped by codec, as text for the documentation:
    /// "`Delta, T64, ZSTD(3)` for `timestamp`; `ZSTD(3)` for `is_float`, ...; `T64, ZSTD(3)` for `count_int`, ...; `Delta, ZSTD(3)` for `positive_int.index`, ...".
    static String describeInnerCodecsForDocs();

    /// The codec declarations of the `histograms` column as `SHOW CREATE TABLE` prints them at the end of the column list of a samples
    /// table: "    CODEC `histograms.timestamp` (Delta, T64, ZSTD(3)),\n" ... The last line has neither a comma nor a newline.
    /// An inner table prints `Delta` with the byte size of the values: with `for_inner_table` the timestamps get `Delta(8)` (the
    /// `DateTime64(3)` of the documentation) and the bucket indexes `Delta(4)`.
    static String describeInnerCodecsForCreateExample(bool for_inner_table = false);
};

}
