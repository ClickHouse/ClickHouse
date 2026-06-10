#include <Functions/FunctionFactory.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
constexpr Float64 small_delta_tolerance = 1e-12;

struct Bucket
{
    Float64 upper_bound;
    Float64 count;
};

bool almostEqual(Float64 a, Float64 b, Float64 epsilon)
{
    if (std::isnan(a) && std::isnan(b))
        return true;

    if (a == b)
        return true;

    Float64 abs_sum = std::abs(a) + std::abs(b);
    Float64 diff = std::abs(a - b);
    if (a == 0 || b == 0 || abs_sum < std::numeric_limits<Float64>::min())
        return diff < epsilon * std::numeric_limits<Float64>::min();

    return diff / std::min(abs_sum, std::numeric_limits<Float64>::max()) < epsilon;
}

Float64 bucketQuantile(Float64 q, std::vector<Bucket> buckets)
{
    /// Prometheus returns scalar edge values for out-of-range quantiles even if the histogram itself is malformed.
    if (std::isnan(q))
        return std::numeric_limits<Float64>::quiet_NaN();
    if (q < 0)
        return -std::numeric_limits<Float64>::infinity();
    if (q > 1)
        return std::numeric_limits<Float64>::infinity();

    if (buckets.empty())
        return std::numeric_limits<Float64>::quiet_NaN();

    if (std::any_of(buckets.begin(), buckets.end(), [](const Bucket & bucket) { return std::isnan(bucket.upper_bound); }))
        return std::numeric_limits<Float64>::quiet_NaN();

    std::sort(
        buckets.begin(), buckets.end(), [](const Bucket & left, const Bucket & right) { return left.upper_bound < right.upper_bound; });
    if (!std::isinf(buckets.back().upper_bound) || buckets.back().upper_bound < 0)
        return std::numeric_limits<Float64>::quiet_NaN();

    /// Aggregations can produce multiple classic buckets with the same upper bound; Prometheus sums them before interpolation.
    std::vector<Bucket> coalesced_buckets;
    coalesced_buckets.reserve(buckets.size());
    coalesced_buckets.push_back(buckets.front());
    for (size_t i = 1; i != buckets.size(); ++i)
    {
        if (buckets[i].upper_bound == coalesced_buckets.back().upper_bound)
            coalesced_buckets.back().count += buckets[i].count;
        else
            coalesced_buckets.push_back(buckets[i]);
    }
    buckets = std::move(coalesced_buckets);

    if (buckets.size() < 2)
        return std::numeric_limits<Float64>::quiet_NaN();

    /// Prometheus tolerates tiny counter-inversion noise in classic histograms before forcing monotonic bucket counts.
    Float64 previous_count = buckets.front().count;
    for (size_t i = 1; i != buckets.size(); ++i)
    {
        Float64 current_count = buckets[i].count;
        if (current_count == previous_count)
            continue;

        if (almostEqual(previous_count, current_count, small_delta_tolerance) || current_count < previous_count)
        {
            buckets[i].count = previous_count;
            continue;
        }

        previous_count = current_count;
    }

    Float64 observations = buckets.back().count;
    if (observations == 0)
        return std::numeric_limits<Float64>::quiet_NaN();

    Float64 rank = q * observations;
    size_t bucket = buckets.size() - 1;
    for (size_t i = 0; i + 1 != buckets.size(); ++i)
    {
        if (buckets[i].count >= rank)
        {
            bucket = i;
            break;
        }
    }

    if (bucket == buckets.size() - 1)
        return buckets[buckets.size() - 2].upper_bound;

    if (bucket == 0 && buckets[0].upper_bound <= 0)
        return buckets[0].upper_bound;

    Float64 bucket_start = 0;
    Float64 bucket_end = buckets[bucket].upper_bound;
    Float64 count = buckets[bucket].count;
    if (bucket > 0)
    {
        bucket_start = buckets[bucket - 1].upper_bound;
        count -= buckets[bucket - 1].count;
        rank -= buckets[bucket - 1].count;
    }

    return bucket_start + (bucket_end - bucket_start) * (rank / count);
}

void checkArrayArgument(const ColumnsWithTypeAndName & arguments, size_t argument_index, const char * function_name)
{
    if (!isArray(arguments[argument_index].type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument {} of function {} must be an Array(Float64) (passed: {})",
            argument_index + 1,
            function_name,
            arguments[argument_index].type->getName());
    }

    const auto & array_type = typeid_cast<const DataTypeArray &>(*arguments[argument_index].type);
    if (!WhichDataType(array_type.getNestedType()).isFloat64())
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument {} of function {} must be an Array(Float64) (passed: {})",
            argument_index + 1,
            function_name,
            arguments[argument_index].type->getName());
    }
}
}

/// Function timeSeriesPrometheusHistogramQuantile(q, bounds, counts) calculates Prometheus histogram_quantile() for classic buckets.
class FunctionTimeSeriesPrometheusHistogramQuantile : public IFunction
{
public:
    static constexpr auto name = "timeSeriesPrometheusHistogramQuantile";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesPrometheusHistogramQuantile>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeFloat64>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 3)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with three arguments: {}(q, bounds, counts)",
                name,
                name);
        }

        if (!WhichDataType(arguments[0].type).isFloat64())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 1 of function {} must be a Float64 (passed: {})",
                name,
                arguments[0].type->getName());
        }

        checkArrayArgument(arguments, 1, name);
        checkArrayArgument(arguments, 2, name);
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto q_column_holder = arguments[0].column->convertToFullColumnIfConst();
        auto bounds_column_holder = arguments[1].column->convertToFullColumnIfConst();
        auto counts_column_holder = arguments[2].column->convertToFullColumnIfConst();

        const auto * q_column = checkAndGetColumn<ColumnFloat64>(q_column_holder.get());
        const auto * bounds_array = checkAndGetColumn<ColumnArray>(bounds_column_holder.get());
        const auto * counts_array = checkAndGetColumn<ColumnArray>(counts_column_holder.get());
        if (!q_column || !bounds_array || !counts_array)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns of arguments of function {}, expected Float64, Array(Float64), Array(Float64)",
                getName());
        }

        const auto * bounds = checkAndGetColumn<ColumnFloat64>(&bounds_array->getData());
        const auto * counts = checkAndGetColumn<ColumnFloat64>(&counts_array->getData());
        if (!bounds || !counts)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal nested columns of arguments of function {}, expected Array(Float64), Array(Float64)",
                getName());
        }

        const auto & bounds_offsets = bounds_array->getOffsets();
        const auto & counts_offsets = counts_array->getOffsets();
        if (bounds_offsets.size() != input_rows_count || counts_offsets.size() != input_rows_count)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal array offsets of arguments of function {}, expected {} rows",
                getName(),
                input_rows_count);
        }

        auto result = ColumnFloat64::create();
        result->reserve(input_rows_count);

        size_t previous_bounds_offset = 0;
        size_t previous_counts_offset = 0;
        for (size_t row = 0; row != input_rows_count; ++row)
        {
            size_t bounds_offset = bounds_offsets[row];
            size_t counts_offset = counts_offsets[row];
            size_t bounds_size = bounds_offset - previous_bounds_offset;
            size_t counts_size = counts_offset - previous_counts_offset;
            if (bounds_size != counts_size)
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Arguments 2 and 3 of function {} must have arrays of equal size", getName());
            }

            std::vector<Bucket> buckets;
            buckets.reserve(bounds_size);
            for (size_t i = 0; i != bounds_size; ++i)
            {
                buckets.push_back({bounds->getElement(previous_bounds_offset + i), counts->getElement(previous_counts_offset + i)});
            }

            result->insertValue(bucketQuantile(q_column->getElement(row), std::move(buckets)));
            previous_bounds_offset = bounds_offset;
            previous_counts_offset = counts_offset;
        }

        return result;
    }
};

REGISTER_FUNCTION(TimeSeriesPrometheusHistogramQuantile)
{
    FunctionDocumentation::Description description = R"(
Calculates Prometheus `histogram_quantile()` for classic histogram buckets.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesPrometheusHistogramQuantile(q, bounds, counts)";
    FunctionDocumentation::Arguments arguments = {
        {"q", "The requested quantile.", {"Float64"}},
        {"bounds", "Classic histogram bucket upper bounds.", {"Array(Float64)"}},
        {"counts", "Classic histogram cumulative bucket counts.", {"Array(Float64)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the interpolated quantile value.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Example",
            "SELECT timeSeriesPrometheusHistogramQuantile(0.9, [1, 2, inf], [20, 40, 40])",
            "1.8",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesPrometheusHistogramQuantile>(documentation);
}

}
