#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <base/EnumReflection.h>

#include <string_view>


namespace DB
{

/// The payload columns of the "histograms" inner table, in the canonical order after `id` and `timestamp`.
/// Integer and float histograms use different typed column sets because integer counts get integer codecs
/// (`T64`, `DoubleDelta`), which compress them better than a shared `Float64` representation would.
enum class TimeSeriesHistogramsColumn : uint8_t
{
    IsFloat,
    CounterResetHint,
    Schema,
    ZeroThreshold,
    Sum,
    PositiveSpans,
    NegativeSpans,
    CustomValues,
    CountInt,
    ZeroCountInt,
    PositiveValuesInt,
    NegativeValuesInt,
    CountFloat,
    ZeroCountFloat,
    PositiveValuesFloat,
    NegativeValuesFloat,
};

/// The name, the type and the codec of a generated column of the "histograms" inner table, and its description for the documentation.
/// This is the source of truth for generating the columns, recognizing the generated ones, validating them, and documenting them.
struct TimeSeriesHistogramsColumnDefinition
{
    std::string_view name;
    std::string_view type;
    std::string_view codec;
    std::string_view description;
};

/// The registry of the payload columns of the "histograms" inner table and of the outer columns mirroring them.
///
/// The outer columns of a TimeSeries table representing histogram samples are the flattened form of
/// `histograms Nested(timestamp <timestamp_type>, is_float Bool, ...)`, i.e. `histograms.timestamp Array(<timestamp_type>)`,
/// `histograms.is_float Array(Bool)`, ... Element k of every array is histogram sample k of the row. The names mirror the columns
/// of the "histograms" inner table (with `timestamp` in place of `id`), so both writing and reading map the columns by name.
class TimeSeriesHistogramsColumns
{
public:
    /// All the payload columns in the canonical order.
    static constexpr auto getAll()
    {
        return magic_enum::enum_values<TimeSeriesHistogramsColumn>();
    }

    /// The trailing `ZSTD(3)` is explicit in every codec: the offsets of an array receive only the generic tail of a codec
    /// pipeline, and the size-aware default codec would give small parts `LZ4`.
    static constexpr TimeSeriesHistogramsColumnDefinition getDefinition(TimeSeriesHistogramsColumn column)
    {
        switch (column)
        {
            case TimeSeriesHistogramsColumn::IsFloat:
                return {"is_float", "Bool", "CODEC(ZSTD(3))",
                    "`false` for an integer histogram, `true` for a float histogram: it tells which of the two typed column sets a row uses, the other set is zero or empty"};
            case TimeSeriesHistogramsColumn::CounterResetHint:
                return {"counter_reset_hint", "UInt8", "CODEC(ZSTD(3))",
                    "0 - unknown, 1 - a counter reset happened, 2 - no counter reset, 3 - a gauge histogram"};
            case TimeSeriesHistogramsColumn::Schema:
                return {"schema", "Int8", "CODEC(ZSTD(3))",
                    "The bucket schema: from -4 to 8 for exponential buckets, -53 for custom buckets"};
            case TimeSeriesHistogramsColumn::ZeroThreshold:
                return {"zero_threshold", "Float64", "CODEC(ZSTD(3))",
                    "The width of the zero bucket"};
            case TimeSeriesHistogramsColumn::Sum:
                return {"sum", "Float64", "CODEC(ZSTD(3))",
                    "The sum of the observations; the stale marker `NaN` when the series went stale"};
            case TimeSeriesHistogramsColumn::PositiveSpans:
                return {"positive_spans", "Array(Tuple(offset Int32, length UInt32))", "CODEC(ZSTD(3))",
                    "The layout of the positive buckets, as Prometheus sends it"};
            case TimeSeriesHistogramsColumn::NegativeSpans:
                return {"negative_spans", "Array(Tuple(offset Int32, length UInt32))", "CODEC(ZSTD(3))",
                    "The layout of the negative buckets, as Prometheus sends it"};
            case TimeSeriesHistogramsColumn::CustomValues:
                return {"custom_values", "Array(Float64)", "CODEC(ZSTD(3))",
                    "The upper bounds of the custom buckets (`schema = -53`), empty otherwise"};
            case TimeSeriesHistogramsColumn::CountInt:
                return {"count_int", "UInt64", "CODEC(DoubleDelta, ZSTD(3))",
                    "The total count of the observations of an integer histogram"};
            case TimeSeriesHistogramsColumn::ZeroCountInt:
                return {"zero_count_int", "UInt64", "CODEC(DoubleDelta, ZSTD(3))",
                    "The count of the zero bucket of an integer histogram"};
            case TimeSeriesHistogramsColumn::PositiveValuesInt:
                return {"positive_values_int", "Array(UInt64)", "CODEC(T64, ZSTD(3))",
                    "The absolute counts of the positive buckets of an integer histogram"};
            case TimeSeriesHistogramsColumn::NegativeValuesInt:
                return {"negative_values_int", "Array(UInt64)", "CODEC(T64, ZSTD(3))",
                    "The absolute counts of the negative buckets of an integer histogram"};
            case TimeSeriesHistogramsColumn::CountFloat:
                return {"count_float", "Float64", "CODEC(ZSTD(3))",
                    "The total count of the observations of a float histogram"};
            case TimeSeriesHistogramsColumn::ZeroCountFloat:
                return {"zero_count_float", "Float64", "CODEC(ZSTD(3))",
                    "The count of the zero bucket of a float histogram"};
            case TimeSeriesHistogramsColumn::PositiveValuesFloat:
                return {"positive_values_float", "Array(Float64)", "CODEC(Delta, ZSTD(3))",
                    "The counts of the positive buckets of a float histogram"};
            case TimeSeriesHistogramsColumn::NegativeValuesFloat:
                return {"negative_values_float", "Array(Float64)", "CODEC(Delta, ZSTD(3))",
                    "The counts of the negative buckets of a float histogram"};
        }
    }

    static constexpr std::string_view getName(TimeSeriesHistogramsColumn column) { return getDefinition(column).name; }
    static constexpr std::string_view getType(TimeSeriesHistogramsColumn column) { return getDefinition(column).type; }
    static constexpr std::string_view getCodec(TimeSeriesHistogramsColumn column) { return getDefinition(column).codec; }
    static constexpr std::string_view getDescription(TimeSeriesHistogramsColumn column) { return getDefinition(column).description; }

    /// The data type of a payload column, parsed from its definition once.
    static const DataTypePtr & getDataType(TimeSeriesHistogramsColumn column);

    /// Returns the name of the outer column mirroring a column of the "histograms" inner table:
    /// `histograms.<inner_column_name>`, e.g. `histograms.timestamp` or `histograms.count_int`.
    static String getOuterColumnName(std::string_view inner_column_name);

    /// Returns the name of the column of the "histograms" inner table mirrored by an outer column of the `histograms` group,
    /// or an empty string if the name isn't such a column.
    static std::string_view getInnerColumnName(std::string_view outer_column_name);

    /// The outer column `histograms.timestamp`, whose type depends on the timestamp type of the table.
    static NameAndTypePair getOuterTimestampColumn(const DataTypePtr & timestamp_type);

    /// The outer columns mirroring the payload columns, in the canonical order: `histograms.is_float Array(Bool)`, ...
    /// Their types don't depend on the table, so the list is built once.
    static const NamesAndTypesList & getOuterPayloadColumns();

    /// The type of the `histogram` column returned by `timeSeriesSelector`: `Array(Tuple(is_float Bool, ...))` with the payload
    /// columns as named elements, built once. It holds no element for a float sample and one element for a histogram sample,
    /// which is a row of the "histograms" inner table as stored.
    static const DataTypePtr & getHistogramColumnType();
};

}
