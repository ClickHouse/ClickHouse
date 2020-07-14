#include <math.h>
#include <sstream>

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/abtesting.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromOStream.h>

#define STATS_ENABLE_STDVEC_WRAPPERS
#include <stats.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

template <bool higher_is_better>
ABTestResult bayesian_ab_test(std::string distribution, std::vector<double> xs, std::vector<double> ys)
{
    const size_t R = 1000, C = 100;

    ABTestResult result;
    std::vector<std::vector<double>> samples_matrix;

    if (distribution == "beta")
    {
        double alpha, beta;

        for (size_t i = 0; i < xs.size(); ++i)
            if (xs[i] < ys[i])
                throw Exception("Conversions cannot be larger than trials", ErrorCodes::BAD_ARGUMENTS);

        for (size_t i = 0; i < xs.size(); ++i)
        {
            alpha = 1.0 + ys[i];
            beta = 1.0 + xs[i] - ys[i];
            samples_matrix.push_back(stats::rbeta<std::vector<double>>(R, C, alpha, beta));
        }
    }
    else if (distribution == "gamma")
    {
        double shape, scale;

        for (size_t i = 0; i < xs.size(); ++i)
        {
            shape = 1.0 + xs[i];
            scale = 250.0 / (1 + 250.0 * ys[i]);
            samples_matrix.push_back(stats::rgamma<std::vector<double>>(R, C, shape, scale));
        }
    }

    std::vector<double> means;
    for (size_t i = 0; i < xs.size(); ++i)
    {
        auto mean = accumulate(samples_matrix[i].begin(), samples_matrix[i].end(), 0.0) / samples_matrix[i].size();
        means.push_back(mean);
    }

    // Beats control
    result.beats_control.resize(xs.size(), 0);
    for (size_t i = 1; i < xs.size(); ++i)
    {
        for (size_t n = 0; n < R * C; ++n)
        {
            if (higher_is_better)
            {
                if (samples_matrix[i][n] > samples_matrix[0][n])
                    ++result.beats_control[i];
            }
            else
            {
                if (samples_matrix[i][n] < samples_matrix[0][n])
                    ++result.beats_control[i];
            }
        }
    }

    for (size_t i = 1; i < xs.size(); ++i)
        result.beats_control[i] = static_cast<double>(result.beats_control[i]) / R / C;

    // To be best
    std::vector<size_t> count_m(xs.size(), 0);
    std::vector<double> row(xs.size(), 0);

    result.best.resize(xs.size(), 0);

    for (size_t n = 0; n < R * C; ++n)
    {
        for (size_t i = 0; i < xs.size(); ++i)
            row[i] = samples_matrix[i][n];

        double m;
        if (higher_is_better)
            m = *std::max_element(row.begin(), row.end());
        else
            m = *std::min_element(row.begin(), row.end());

        for (size_t i = 0; i < xs.size(); ++i)
        {
            if (m == samples_matrix[i][n])
            {
                ++result.best[i];
                break;
            }
        }
    }

    for (size_t i = 0; i < xs.size(); ++i)
        result.best[i] = static_cast<double>(result.best[i]) / R / C;

    return result;
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

    size_t getNumberOfArguments() const override { return 5; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    const IColumn * getNestedConstColumn(Block & block, const ColumnNumbers & arguments, const size_t n)
    {
        const IColumn * col = block.getByPosition(arguments[n]).column.get();
        const IColumn * nested_col;
        ColumnPtr materialized_column;

        if (const ColumnConst * const_arr = checkAndGetColumnConst<ColumnArray>(col))
        {
            materialized_column = const_arr->convertToFullColumn();
            const auto & materialized_arr = typeid_cast<const ColumnArray &>(*materialized_column);
            nested_col = &materialized_arr.getData();
        }
        else
            throw Exception("Illegal column " + col->getName() + " as argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return nested_col;
    }

    std::vector<double> getDoubleValues(const IColumn * col)
    {
        const ColumnFloat64 * column = checkAndGetColumn<ColumnFloat64>(*col);
        if (!column)
            throw Exception("Illegal type of argument for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::vector<double> ret;
        for (size_t i = 0; i < column->size(); ++i)
            ret.push_back(column->getData()[i]);

        return ret;
    }

    std::vector<std::string> getStringValues(const IColumn * col)
    {
        const ColumnString * column = checkAndGetColumn<ColumnString>(*col);
        if (!column)
            throw Exception("Illegal type of argument for function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        std::vector<std::string> ret;
        for (size_t i = 0; i < column->size(); ++i)
            ret.push_back(column->getDataAt(i).data);

        return ret;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        std::vector<double> xs, ys;
        std::vector<std::string> variant_names;
        std::string dist;
        bool higher_is_better;

        if (const ColumnConst * col_dist = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get()))
            dist = col_dist->getDataAt(0).data;
        else
            throw Exception("First argument for function " + getName() + " must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (const ColumnConst * col_higher_is_better = checkAndGetColumnConst<ColumnUInt8>(block.getByPosition(arguments[1]).column.get()))
            higher_is_better = col_higher_is_better->getBool(0);
        else
            throw Exception("Second argument for function " + getName() + " must be Boolean", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        variant_names = getStringValues(getNestedConstColumn(block, arguments, 2));
        xs = getDoubleValues(getNestedConstColumn(block, arguments, 3));
        ys = getDoubleValues(getNestedConstColumn(block, arguments, 4));
        if (variant_names.size() != xs.size() || xs.size() != ys.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of arguments doen't match: variant_names: {}, xs: {}, ys: {}", variant_names.size(), xs.size(), ys.size());

        if (std::count_if(xs.begin(), xs.end(), [](double v) { return v < 0; }) > 0 ||
            std::count_if(ys.begin(), ys.end(), [](double v) { return v < 0; }) > 0)
            throw Exception("Negative values don't allowed", ErrorCodes::BAD_ARGUMENTS);

        ABTestResult test_result;
        if (dist == "beta")
        {
            if (higher_is_better)
                test_result = bayesian_ab_test<true>(dist, xs, ys);
            else
                test_result = bayesian_ab_test<false>(dist, xs, ys);
        }
        else if (dist == "gamma")
        {
            if (higher_is_better)
                test_result = bayesian_ab_test<false>(dist, xs, ys);
            else
                test_result = bayesian_ab_test<true>(dist, xs, ys);
        }
        else
            throw Exception("First argument for function " + getName() + " cannot be " + dist, ErrorCodes::BAD_ARGUMENTS);

        FormatSettings settings;
        std::stringstream s;

        {
            WriteBufferFromOStream buf(s);

            writeCString("{\"data\":[", buf);
            for (size_t i = 0; i < xs.size(); ++i)
            {
                writeCString("{\"variant_name\":", buf);
                writeJSONString(variant_names[i], buf, settings);
                writeCString(",\"beats_control\":", buf);
                writeText(test_result.beats_control[i], buf);
                writeCString(",\"to_be_best\":", buf);
                writeText(test_result.best[i], buf);
                writeCString("}", buf);
                if (i != xs.size() -1) writeCString(",", buf);
            }
            writeCString("]}", buf);
        }

        auto dst = ColumnString::create();
        std::string result_str = s.str();
        dst->insertData(result_str.c_str(), result_str.length());
        block.getByPosition(result).column = std::move(dst);
    }
};

void registerFunctionBayesAB(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBayesAB>();
}

}
