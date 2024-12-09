#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
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

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
struct UniformDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randUniform"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(Float64 min, Float64 max, ColumnFloat64::Container & container)
    {
        auto distribution = std::uniform_real_distribution<>(min, max);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct NormalDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randNormal"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(Float64 mean, Float64 variance, ColumnFloat64::Container & container)
    {
        auto distribution = std::normal_distribution<>(mean, variance);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct LogNormalDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randLogNormal"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(Float64 mean, Float64 variance, ColumnFloat64::Container & container)
    {
        auto distribution = std::lognormal_distribution<>(mean, variance);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct ExponentialDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randExponential"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    static void generate(Float64 lambda, ColumnFloat64::Container & container)
    {
        auto distribution = std::exponential_distribution<>(lambda);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct ChiSquaredDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randChiSquared"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    static void generate(Float64 degree_of_freedom, ColumnFloat64::Container & container)
    {
        if (degree_of_freedom <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument (degrees of freedom) of function {} should be greater than zero", getName());

        auto distribution = std::chi_squared_distribution<>(degree_of_freedom);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct StudentTDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randStudentT"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    static void generate(Float64 degree_of_freedom, ColumnFloat64::Container & container)
    {
        if (degree_of_freedom <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument (degrees of freedom) of function {} should be greater than zero", getName());

        auto distribution = std::student_t_distribution<>(degree_of_freedom);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct FisherFDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randFisherF"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(Float64 d1, Float64 d2, ColumnFloat64::Container & container)
    {
        if (d1 <= 0 || d2 <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument (degrees of freedom) of function {} should be greater than zero", getName());

        auto distribution = std::fisher_f_distribution<>(d1, d2);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct BernoulliDistribution
{
    using ReturnType = DataTypeUInt8;
    static constexpr const char * getName() { return "randBernoulli"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    static void generate(Float64 p, ColumnUInt8::Container & container)
    {
        if (p < 0.0f || p > 1.0f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of function {} should be inside [0, 1] because it is a probability", getName());

        auto distribution = std::bernoulli_distribution(p);
        for (auto & elem : container)
            elem = static_cast<UInt8>(distribution(thread_local_rng));
    }
};

struct BinomialDistribution
{
    using ReturnType = DataTypeUInt64;
    static constexpr const char * getName() { return "randBinomial"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(UInt64 t, Float64 p, ColumnUInt64::Container & container)
    {
        if (p < 0.0f || p > 1.0f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of function {} should be inside [0, 1] because it is a probability", getName());

        auto distribution = std::binomial_distribution(t, p);
        for (auto & elem : container)
            elem = static_cast<UInt64>(distribution(thread_local_rng));
    }
};

struct NegativeBinomialDistribution
{
    using ReturnType = DataTypeUInt64;
    static constexpr const char * getName() { return "randNegativeBinomial"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(UInt64 t, Float64 p, ColumnUInt64::Container & container)
    {
        if (p < 0.0f || p > 1.0f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of function {} should be inside [0, 1] because it is a probability", getName());

        auto distribution = std::negative_binomial_distribution(t, p);
        for (auto & elem : container)
            elem = static_cast<UInt64>(distribution(thread_local_rng));
    }
};

struct PoissonDistribution
{
    using ReturnType = DataTypeUInt64;
    static constexpr const char * getName() { return "randPoisson"; }
    static constexpr size_t getNumberOfArguments() { return 1; }

    static void generate(UInt64 n, ColumnUInt64::Container & container)
    {
        auto distribution = std::poisson_distribution(n);
        for (auto & elem : container)
            elem = static_cast<UInt64>(distribution(thread_local_rng));
    }
};

}

/** Function which will generate values according to the specified distribution
  * Accepts only constant arguments
  * Similar to the functions rand and rand64 an additional 'tag' argument could be added to the
  * end of arguments list (this argument will be ignored) which will guarantee that functions are not sticked together
  * during optimizations.
  * Example: SELECT randNormal(0, 1, 1), randNormal(0, 1, 2) FROM numbers(10)
  * This query will return two different columns
  */
template <typename Distribution>
class FunctionRandomDistribution : public IFunction
{
private:

    template <typename ResultType>
    ResultType getParameterFromConstColumn(size_t parameter_number, const ColumnsWithTypeAndName & arguments) const
    {
        if (parameter_number >= arguments.size())
            throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Parameter number ({}) is greater than the size of arguments ({}). This is a bug",
                            parameter_number, arguments.size());

        const IColumn * col = arguments[parameter_number].column.get();

        if (!isColumnConst(*col))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Parameter number {} of function {} must be constant.", parameter_number, getName());

        auto parameter = applyVisitor(FieldVisitorConvertToNumber<ResultType>(), assert_cast<const ColumnConst &>(*col).getField());

        if (isNaN(parameter) || !std::isfinite(parameter))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter number {} of function {} cannot be NaN of infinite", parameter_number, getName());

        return parameter;
    }

public:
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionRandomDistribution<Distribution>>();
    }

    static constexpr auto name = Distribution::getName();
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return Distribution::getNumberOfArguments(); }
    bool isVariadic() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        auto desired = Distribution::getNumberOfArguments();
        if (arguments.size() != desired && arguments.size() != desired + 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Wrong number of arguments for function {}. Should be {} or {}",
                            getName(), desired, desired + 1);

        for (size_t i = 0; i < Distribution::getNumberOfArguments(); ++i)
        {
            const auto & type = arguments[i];
            WhichDataType which(type);
            if (!which.isFloat() && !which.isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected Float64 or integer", type->getName(), getName());
        }

        return std::make_shared<typename Distribution::ReturnType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        if constexpr (std::is_same_v<Distribution, BernoulliDistribution>)
        {
            auto res_column = ColumnUInt8::create(input_rows_count);
            auto & res_data = res_column->getData();
            Distribution::generate(getParameterFromConstColumn<Float64>(0, arguments), res_data);
            return res_column;
        }
        else if constexpr (std::is_same_v<Distribution, BinomialDistribution> || std::is_same_v<Distribution, NegativeBinomialDistribution>)
        {
            auto res_column = ColumnUInt64::create(input_rows_count);
            auto & res_data = res_column->getData();
            Distribution::generate(getParameterFromConstColumn<UInt64>(0, arguments), getParameterFromConstColumn<Float64>(1, arguments), res_data);
            return res_column;
        }
        else if constexpr (std::is_same_v<Distribution, PoissonDistribution>)
        {
            auto res_column = ColumnUInt64::create(input_rows_count);
            auto & res_data = res_column->getData();
            Distribution::generate(getParameterFromConstColumn<UInt64>(0, arguments), res_data);
            return res_column;
        }
        else
        {
            auto res_column = ColumnFloat64::create(input_rows_count);
            auto & res_data = res_column->getData();
            if constexpr (Distribution::getNumberOfArguments() == 1)
            {
                Distribution::generate(getParameterFromConstColumn<Float64>(0, arguments), res_data);
            }
            else if constexpr (Distribution::getNumberOfArguments() == 2)
            {
                Distribution::generate(getParameterFromConstColumn<Float64>(0, arguments), getParameterFromConstColumn<Float64>(1, arguments), res_data);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "More than two arguments specified for function {}", getName());
            }

            return res_column;
        }
    }
};


REGISTER_FUNCTION(Distribution)
{
    factory.registerFunction<FunctionRandomDistribution<UniformDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the uniform distribution in the specified range.
Accepts two parameters - minimum bound and maximum bound.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randUniform(0, 1) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });

    factory.registerFunction<FunctionRandomDistribution<NormalDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the normal distribution.
Accepts two parameters - mean and variance.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randNormal(0, 5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<LogNormalDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the lognormal distribution (a distribution of a random variable whose logarithm is normally distributed).
Accepts two parameters - mean and variance.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randLogNormal(0, 5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<ExponentialDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the exponential distribution.
Accepts one parameter - lambda value.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randExponential(0, 5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<ChiSquaredDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the chi-squared distribution (a distribution of a sum of the squares of k independent standard normal random variables).
Accepts one parameter - degree of freedom.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randChiSquared(5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });

    factory.registerFunction<FunctionRandomDistribution<StudentTDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the t-distribution.
Accepts one parameter - degree of freedom.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randStudentT(5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<FisherFDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the f-distribution.
The F-distribution is the distribution of X = (S1 / d1) / (S2 / d2) where d1 and d2 are degrees of freedom.
Accepts two parameters - degrees of freedom.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randFisherF(5) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<BernoulliDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the Bernoulli distribution.
Accepts one parameter - probability of success.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randBernoulli(0.1) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<BinomialDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the binomial distribution.
Accepts two parameters - number of experiments and probability of success in each experiment.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randBinomial(10, 0.1) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<NegativeBinomialDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the negative binomial distribution.
Accepts two parameters - number of experiments and probability of success in each experiment.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randNegativeBinomial(10, 0.1) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });


    factory.registerFunction<FunctionRandomDistribution<PoissonDistribution>>(
    FunctionDocumentation{
    .description=R"(
Returns a random number from the poisson distribution.
Accepts one parameter - the mean number of occurrences.

Typical usage:
[example:typical]
)",
    .examples{
        {"typical", "SELECT randPoisson(3) FROM numbers(100000);", ""}},
    .categories{"Distribution"}
    });
}

}
