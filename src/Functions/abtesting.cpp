#include <Functions/abtesting.h>

#if !defined(ARCADIA_BUILD) && USE_STATS

#include <math.h>

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#define STATS_ENABLE_STDVEC_WRAPPERS
#include <stats.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

static const String BETA = "beta";
static const String GAMMA = "gamma";

template <bool higher_is_better>
Variants bayesian_ab_test(String distribution, PODArray<Float64> & xs, PODArray<Float64> & ys)
{
    const size_t r = 1000, c = 100;

    Variants variants(xs.size(), {0.0, 0.0, 0.0, 0.0});
    std::vector<std::vector<Float64>> samples_matrix;

    for (size_t i = 0; i < xs.size(); ++i)
    {
        variants[i].x = xs[i];
        variants[i].y = ys[i];
    }

    if (distribution == BETA)
    {
        Float64 alpha, beta;

        for (size_t i = 0; i < xs.size(); ++i)
            if (xs[i] < ys[i])
                throw Exception("Conversions cannot be larger than trials", ErrorCodes::BAD_ARGUMENTS);

        for (size_t i = 0; i < xs.size(); ++i)
        {
            alpha = 1.0 + ys[i];
            beta = 1.0 + xs[i] - ys[i];

            samples_matrix.emplace_back(stats::rbeta<std::vector<Float64>>(r, c, alpha, beta));
        }
    }
    else if (distribution == GAMMA)
    {
        Float64 shape, scale;

        for (size_t i = 0; i < xs.size(); ++i)
        {
            shape = 1.0 + xs[i];
            scale = 250.0 / (1 + 250.0 * ys[i]);

            std::vector<Float64> samples = stats::rgamma<std::vector<Float64>>(r, c, shape, scale);
            for (auto & sample : samples)
                sample = 1 / sample;
            samples_matrix.emplace_back(std::move(samples));
        }
    }

    PODArray<Float64> means;
    for (auto & samples : samples_matrix)
    {
        Float64 total = 0.0;
        for (auto sample : samples)
            total += sample;
        means.push_back(total / samples.size());
    }

    // Beats control
    for (size_t i = 1; i < xs.size(); ++i)
    {
        for (size_t n = 0; n < r * c; ++n)
        {
            if (higher_is_better)
            {
                if (samples_matrix[i][n] > samples_matrix[0][n])
                    ++variants[i].beats_control;
            }
            else
            {
                if (samples_matrix[i][n] < samples_matrix[0][n])
                    ++variants[i].beats_control;
            }
        }
    }

    for (auto & variant : variants)
        variant.beats_control = static_cast<Float64>(variant.beats_control) / r / c;

    // To be best
    PODArray<size_t> count_m(xs.size(), 0);
    PODArray<Float64> row(xs.size(), 0);

    for (size_t n = 0; n < r * c; ++n)
    {
        for (size_t i = 0; i < xs.size(); ++i)
            row[i] = samples_matrix[i][n];

        Float64 m;
        if (higher_is_better)
            m = *std::max_element(row.begin(), row.end());
        else
            m = *std::min_element(row.begin(), row.end());

        for (size_t i = 0; i < xs.size(); ++i)
        {
            if (m == samples_matrix[i][n])
            {
                ++variants[i].best;
                break;
            }
        }
    }

    for (auto & variant : variants)
        variant.best = static_cast<Float64>(variant.best) / r / c;

    return variants;
}

String convertToJson(const PODArray<String> & variant_names, const Variants & variants)
{
    FormatSettings settings;

    WriteBufferFromOwnString buf;

    writeCString("{\"data\":[", buf);
    for (size_t i = 0; i < variants.size(); ++i)
    {
        writeCString("{\"variant_name\":", buf);
        writeJSONString(variant_names[i], buf, settings);
        writeCString(",\"x\":", buf);
        writeText(variants[i].x, buf);
        writeCString(",\"y\":", buf);
        writeText(variants[i].y, buf);
        writeCString(",\"beats_control\":", buf);
        writeText(variants[i].beats_control, buf);
        writeCString(",\"to_be_best\":", buf);
        writeText(variants[i].best, buf);
        writeCString("}", buf);
        if (i != variant_names.size() -1)
            writeCString(",", buf);
    }
    writeCString("]}", buf);

    return buf.str();
}

class FunctionBayesAB : public IFunction
{
public:
    static constexpr auto name = "bayesAB";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionBayesAB>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    size_t getNumberOfArguments() const override { return 5; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    static bool toFloat64(const ColumnConst * col_const_arr, PODArray<Float64> & output)
    {
        Array src_arr = col_const_arr->getValue<Array>();

        for (size_t i = 0, size = src_arr.size(); i < size; ++i)
        {
            switch (src_arr[i].getType())
            {
                case Field::Types::Int64:
                    output.push_back(static_cast<Float64>(src_arr[i].get<const Int64 &>()));
                    break;
                case Field::Types::UInt64:
                    output.push_back(static_cast<Float64>(src_arr[i].get<const UInt64 &>()));
                    break;
                case Field::Types::Float64:
                    output.push_back(src_arr[i].get<const Float64 &>());
                    break;
                default:
                    return false;
            }
        }

        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnString::create();

        PODArray<Float64> xs, ys;
        PODArray<String> variant_names;
        String dist;
        bool higher_is_better;

        if (const ColumnConst * col_dist = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
        {
            dist = col_dist->getDataAt(0).data;
            dist = Poco::toLower(dist);
            if (dist != BETA && dist != GAMMA)
                throw Exception("First argument for function " + getName() + " cannot be " + dist, ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("First argument for function " + getName() + " must be Constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_higher_is_better = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
            higher_is_better = col_higher_is_better->getBool(0);
        else
            throw Exception("Second argument for function " + getName() + " must be Constatnt boolean", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get()))
        {
            Array src_arr = col_const_arr->getValue<Array>();

            for (size_t i = 0; i < src_arr.size(); ++i)
            {
                if (src_arr[i].getType() != Field::Types::String)
                    throw Exception("Third argument for function " + getName() + " must be Array of constant strings", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                variant_names.push_back(src_arr[i].get<const String &>());
            }
        }
        else
            throw Exception("Third argument for function " + getName() + " must be Array of constant strings", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[3].column.get()))
        {
            if (!toFloat64(col_const_arr, xs))
                throw Exception("Forth and fifth Argument for function " + getName() + " must be Array of constant Numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception("Forth argument for function " + getName() + " must be Array of constant numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[4].column.get()))
        {
            if (!toFloat64(col_const_arr, ys))
                throw Exception("Fifth Argument for function " + getName() + " must be Array of constant Numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception("Fifth argument for function " + getName() + " must be Array of constant numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (variant_names.size() != xs.size() || xs.size() != ys.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of arguments doesn't match: variant_names: {}, xs: {}, ys: {}", variant_names.size(), xs.size(), ys.size());

        if (variant_names.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of arguments must be larger than 1. variant_names: {}, xs: {}, ys: {}", variant_names.size(), xs.size(), ys.size());

        if (std::count_if(xs.begin(), xs.end(), [](Float64 v) { return v < 0; }) > 0 ||
            std::count_if(ys.begin(), ys.end(), [](Float64 v) { return v < 0; }) > 0)
            throw Exception("Negative values don't allowed", ErrorCodes::BAD_ARGUMENTS);

        Variants variants;
        if (higher_is_better)
            variants = bayesian_ab_test<true>(dist, xs, ys);
        else
            variants = bayesian_ab_test<false>(dist, xs, ys);

        auto dst = ColumnString::create();
        std::string result_str = convertToJson(variant_names, variants);
        dst->insertData(result_str.c_str(), result_str.length());
        return dst;
    }
};

void registerFunctionBayesAB(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBayesAB>();
}

}

#else

namespace DB
{

class FunctionFactory;

void registerFunctionBayesAB(FunctionFactory & /* factory */)
{
}

}

#endif
