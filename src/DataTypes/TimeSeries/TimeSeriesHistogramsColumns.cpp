#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>

#include <Columns/IColumn.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>

#include <fmt/format.h>


namespace DB
{

namespace TimeSeriesHistogramsColumns
{

namespace
{

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

constexpr std::string_view getCounterResetHintTypeName() { return "Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3)"; }

/// The codecs are declared for the subcolumns of the generated `histograms` column (`CODEC histograms.<name> (...)`). The counts
/// are stored as sent: `T64` bit-packs the integer counts, the float counts get the general purpose codec only (`ALP` doesn't help
/// on absolute counts), the bucket indexes of the pairs get `Delta` (see `getPairIndexCodec`).
constexpr TimeSeriesHistogramsColumnDefinition getDefinition(TimeSeriesHistogramsColumn column)
{
    switch (column)
    {
        case TimeSeriesHistogramsColumn::IsFloat:
            return {"is_float", "Bool", "CODEC(ZSTD(3))",
                "Whether the histogram is a float histogram, with its counts in the `*_float` elements; an integer histogram has its counts "
                "in the `*_int` elements. The readers ignore the elements of the other flavour, which remote write leaves 0 and empty"};
        case TimeSeriesHistogramsColumn::CounterResetHint:
            return {"counter_reset_hint", getCounterResetHintTypeName(), "CODEC(ZSTD(3))",
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

constexpr std::string_view getDescription(TimeSeriesHistogramsColumn column) { return getDefinition(column).description; }

/// Whether the element is one of the four arrays of pairs of the bucket index and the count.
constexpr bool isPairs(TimeSeriesHistogramsColumn column)
{
    return (column == TimeSeriesHistogramsColumn::PositiveInt) || (column == TimeSeriesHistogramsColumn::NegativeInt)
        || (column == TimeSeriesHistogramsColumn::PositiveFloat) || (column == TimeSeriesHistogramsColumn::NegativeFloat);
}

/// Whether the element holds the counts of the float flavour (`count_float`, `zero_count_float`, `positive_float`, `negative_float`).
constexpr bool isFloatFlavour(TimeSeriesHistogramsColumn column)
{
    return (column == TimeSeriesHistogramsColumn::CountFloat) || (column == TimeSeriesHistogramsColumn::ZeroCountFloat)
        || (column == TimeSeriesHistogramsColumn::PositiveFloat) || (column == TimeSeriesHistogramsColumn::NegativeFloat);
}

/// The elements of a pair: the first one is the bucket index, the second one the count.
constexpr std::string_view getPairIndexName() { return "index"; }
constexpr std::string_view getPairCountName() { return "count"; }

/// The codec of the `timestamp` element: near-monotonic timestamps barely compress under generic codecs.
constexpr std::string_view getTimestampCodec() { return "CODEC(Delta, T64, ZSTD(3))"; }

/// The codecs of the subcolumns of an array of pairs. The indexes of one histogram increase, so `Delta` makes them small.
constexpr std::string_view getPairIndexCodec() { return "CODEC(Delta, ZSTD(3))"; }
constexpr std::string_view getPairCountCodec(TimeSeriesHistogramsColumn column)
{
    return isFloatFlavour(column) ? "CODEC(ZSTD(3))" : "CODEC(T64, ZSTD(3))";
}

const DataTypePtr & getDefaultTimestampType()
{
    static const DataTypePtr type = std::make_shared<DataTypeDateTime64>(3);
    return type;
}

}

std::string_view getName(TimeSeriesHistogramsColumn column)
{
    return getDefinition(column).name;
}

std::string_view getType(TimeSeriesHistogramsColumn column)
{
    return getDefinition(column).type;
}

const std::vector<TimeSeriesHistogramsCodecDeclaration> & getCodecDeclarations()
{
    static const std::vector<TimeSeriesHistogramsCodecDeclaration> declarations = []
    {
        std::vector<TimeSeriesHistogramsCodecDeclaration> result;
        result.push_back({String{getTimestampName()}, getTimestampCodec()});
        for (const auto column : getAll())
        {
            if (isPairs(column))
            {
                result.push_back({fmt::format("{}.{}", getName(column), getPairIndexName()), getPairIndexCodec()});
                result.push_back({fmt::format("{}.{}", getName(column), getPairCountName()), getPairCountCodec(column)});
            }
            else
            {
                chassert(!getDefinition(column).codec.empty());
                result.push_back({String{getName(column)}, getDefinition(column).codec});
            }
        }
        return result;
    }();
    return declarations;
}

const DataTypePtr & getDataType(TimeSeriesHistogramsColumn column)
{
    static const auto data_types = []
    {
        std::array<DataTypePtr, getAll().size()> result;
        for (auto payload_column : getAll())
            result[static_cast<size_t>(payload_column)] = DataTypeFactory::instance().get(String{getType(payload_column)});
        return result;
    }();
    return data_types[static_cast<size_t>(column)];
}

const IColumn & getEmptyColumn(TimeSeriesHistogramsColumn column)
{
    static const auto columns = []
    {
        std::array<ColumnPtr, getAll().size()> result;
        for (auto payload_column : getAll())
            result[static_cast<size_t>(payload_column)] = getDataType(payload_column)->createColumn();
        return result;
    }();
    return *columns[static_cast<size_t>(column)];
}

DataTypePtr makeHistogramsArrayType(const DataTypePtr & timestamp_type)
{
    DataTypes element_types{timestamp_type};
    Strings element_names{String{getTimestampName()}};
    for (auto column : getAll())
    {
        element_types.push_back(getDataType(column));
        element_names.emplace_back(getName(column));
    }
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names)));
}

DataTypePtr tryGetTimestampType(const DataTypePtr & type)
{
    /// A type wrapped in `SimpleAggregateFunction` is the nested type with a custom name, so no unwrapping is needed.
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    const auto * tuple_type = array_type ? typeid_cast<const DataTypeTuple *>(array_type->getNestedType().get()) : nullptr;
    if (!tuple_type || (tuple_type->getElements().size() != getAll().size() + 1))
        return nullptr;

    const auto & timestamp_type = tuple_type->getElements()[getTimestampPosition()];
    if (!makeHistogramsArrayType(timestamp_type)->equals(*std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(tuple_type->getElements(), tuple_type->getElementNames()))))
        return nullptr;
    return timestamp_type;
}

String describeHistogramsArrayTypeForDocs()
{
    return makeHistogramsArrayType(getDefaultTimestampType())->getName();
}

String describeHistogramsElementsForDocs()
{
    String result = fmt::format("| `{}` | `{}` by default | The timestamp of the histogram sample, of the type of the timestamps of `samples` |\n",
        getTimestampName(), getDefaultTimestampType()->getName());
    for (const auto column : getAll())
        result += fmt::format("| `{}` | `{}` | {} |\n", getName(column), getType(column), getDescription(column));
    return result;
}

}

}
