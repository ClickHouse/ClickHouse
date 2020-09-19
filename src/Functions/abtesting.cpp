#include <Functions/abtesting.h>

#if !defined(ARCADIA_BUILD) && USE_STATS

#include <math.h>
#include <sstream>

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/density.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>

#define STATS_ENABLE_STDVEC_WRAPPERS
#include <stats.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

static const String BETA = "beta";
static const String GAMMA = "gamma";

template <bool higher_is_better>
Variants bayesian_ab_test(String distribution, PODArray<Float64> & xs, PODArray<Float64> & ys)
{
    const size_t r = 1000, c = 100;

    Variants variants;
    for (size_t i = 0; i < xs.size(); ++i)
        variants.emplace_back(Variant(xs[i], ys[i]));

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

            variants[i].samples = stats::rbeta<std::vector<Float64>>(r, c, alpha, beta);
        }
    }
    else if (distribution == GAMMA)
    {
        Float64 shape, scale;

        for (size_t i = 0; i < xs.size(); ++i)
        {
            shape = 1.0 + xs[i];
            scale = 250.0 / (1 + 250.0 * ys[i]);

            variants[i].samples = stats::rgamma<std::vector<Float64>>(r, c, shape, scale);
            for (auto & sample : variants[i].samples)
                sample = 1.0 / sample;
        }
    }

    PODArray<Float64> means;
    for (auto & variant : variants)
    {
        Float64 total = 0.0;
        for (auto sample : variant.samples)
            total += sample;
        means.push_back(total / variant.samples.size());
    }

    // Beats control
    for (size_t i = 1; i < xs.size(); ++i)
    {
        for (size_t n = 0; n < r * c; ++n)
        {
            if (higher_is_better)
            {
                if (variants[i].samples[n] > variants[0].samples[n])
                    ++variants[i].beats_control;
            }
            else
            {
                if (variants[i].samples[n] < variants[0].samples[n])
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
            row[i] = variants[i].samples[n];

        Float64 m;
        if (higher_is_better)
            m = *std::max_element(row.begin(), row.end());
        else
            m = *std::min_element(row.begin(), row.end());

        for (size_t i = 0; i < xs.size(); ++i)
        {
            if (m == variants[i].samples[n])
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

String convertToJson(const PODArray<String> & variant_names, Variants & variants, const bool include_density)
{
    FormatSettings settings;
    std::stringstream s;

    {
        WriteBufferFromOStream buf(s);

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

            if (include_density)
            {
                std::vector<Float64> result_xs;
                std::vector<Float64> result_ys;

                density(variants[i].samples, result_xs, result_ys);

                writeCString(",\"samples\":{", buf);
                writeCString("\"x\":[", buf);
                writeText(result_xs[0], buf);
                for (size_t j = 1; j < result_xs.size(); ++j)
                {
                    writeCString(",", buf);
                    writeText(result_xs[j], buf);
                }

                writeCString("],\"y\":[", buf);
                writeText(result_ys[0], buf);
                for (size_t j = 1; j < result_ys.size(); ++j)
                {
                    writeCString(",", buf);
                    writeText(result_ys[j], buf);
                }
                writeCString("]}", buf);
            }

            writeCString("}", buf);
            if (i != variant_names.size() -1) writeCString(",", buf);
        }
        writeCString("]}", buf);
    }

    return s.str();
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

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &arguments) const override
    {
        if ((arguments.size() < 5) || (arguments.size() > 6))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                    + toString(arguments.size()) + ", should be 5 or 6.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
        {
            block.getByPosition(result).column = ColumnString::create();
            return;
        }

        PODArray<Float64> xs, ys;
        PODArray<String> variant_names;
        String dist;
        bool higher_is_better;
        bool include_density;

        if (const ColumnConst * col_dist = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get()))
        {
            dist = col_dist->getDataAt(0).data;
            dist = Poco::toLower(dist);
            if (dist != BETA && dist != GAMMA)
                throw Exception("First argument for function " + getName() + " cannot be " + dist, ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("First argument for function " + getName() + " must be Constant string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_higher_is_better = checkAndGetColumnConst<ColumnUInt8>(block.getByPosition(arguments[1]).column.get()))
            higher_is_better = col_higher_is_better->getBool(0);
        else
            throw Exception("Second argument for function " + getName() + " must be Constatnt boolean", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[2]).column.get()))
        {
            if (!col_const_arr)
                throw Exception("Third argument for function " + getName() + " must be Array of constant strings", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            Array src_arr = col_const_arr->getValue<Array>();

            for (size_t i = 0; i < src_arr.size(); ++i)
            {
                if (src_arr[i].getType() != Field::Types::String)
                    throw Exception("Third argument for function " + getName() + " must be Array of constant strings", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                variant_names.push_back(src_arr[i].get<const String &>());
            }
        }

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[3]).column.get()))
        {
            if (!col_const_arr)
                throw Exception("Forth argument for function " + getName() + " must be Array of constant numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!toFloat64(col_const_arr, xs))
                throw Exception("Forth and fifth Argument for function " + getName() + " must be Array of constant Numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(block.getByPosition(arguments[4]).column.get()))
        {
            if (!col_const_arr)
                throw Exception("Fifth argument for function " + getName() + " must be Array of constant numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!toFloat64(col_const_arr, ys))
                throw Exception("Fifth Argument for function " + getName() + " must be Array of constant Numbers", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (arguments.size() == 6)
        {
            if (const ColumnConst * col_density = checkAndGetColumnConst<ColumnUInt8>(block.getByPosition(arguments[5]).column.get()))
                include_density = col_density->getBool(0);
            else
                throw Exception("Sixth argument for function " + getName() + " must be Constatnt boolean", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
        {
            include_density = false;
        }

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
        std::string result_str = convertToJson(variant_names, variants, include_density);
        dst->insertData(result_str.c_str(), result_str.length());
        block.getByPosition(result).column = std::move(dst);
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
