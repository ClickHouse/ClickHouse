#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Common/NaNUtils.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>

#include <random>

#include <boost/random.hpp>
#include <boost/random/uniform_real_distribution.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_SLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

struct UniformDistribution
{
    static constexpr const char * getName() { return "uniformDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::uniform_real_distribution<>(parameters[0], parameters[1]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct NormalDistribution
{
    static constexpr const char * getName() { return "normalDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::normal_distribution<>(parameters[0], parameters[1]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct LogNormalDistribution
{
    static constexpr const char * getName() { return "logNormalDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::lognormal_distribution<>(parameters[0], parameters[1]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct ChiSquaredDistribution
{
    static constexpr const char * getName() { return "chiSquaredDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::chi_squared_distribution<>(parameters[0]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct StudentTDistribution
{
    static constexpr const char * getName() { return "studentTDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::student_t_distribution<>(parameters[0]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct FisherFDistribution
{
    static constexpr const char * getName() { return "fisherFDistribution"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    void generate(std::vector<Float64> & parameters, ColumnFloat64::Container & container) const
    {
        auto distribution = std::fisher_f_distribution<>(parameters[0]);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};


/// Function which will generate values according to the distibution
/// Accepts only constant arguments
template <typename Distribution>
class FunctionDistribution : public IFunction
{
private:
    mutable Distribution distribution;

public:
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionDistribution<Distribution>>();
    }

    static constexpr auto name = Distribution::getName();
    String getName() const override { return Distribution::getName(); }
    size_t getNumberOfArguments() const override { return Distribution::getNumberOfArguments(); }
    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & type : arguments)
        {
            WhichDataType which(type);
            if (!which.isFloat() && !which.isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected Float64", type->getName(), getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        std::vector<Float64> parameters(arguments.size());
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            const IColumn * col = arguments[i].column.get();

            if (!isColumnConst(*col))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The {}th argument of function must be constant.", getName());

            parameters[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), assert_cast<const ColumnConst &>(*col).getField());

            if (isNaN(parameters[i]) || !std::isfinite(parameters[i]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter number {} of function {} cannot be NaN of infinite", i, getName());
        }

        auto res_column = ColumnFloat64::create(input_rows_count);
        auto & res_data = res_column->getData();
        distribution.generate(parameters, res_data);

        return res_column;
    }
};


REGISTER_FUNCTION(Distribution)
{
    factory.registerFunction<FunctionDistribution<UniformDistribution>>();
    factory.registerFunction<FunctionDistribution<NormalDistribution>>();
    factory.registerFunction<FunctionDistribution<LogNormalDistribution>>();
    factory.registerFunction<FunctionDistribution<ChiSquaredDistribution>>();
    factory.registerFunction<FunctionDistribution<StudentTDistribution>>();
    factory.registerFunction<FunctionDistribution<FisherFDistribution>>();
}

}
