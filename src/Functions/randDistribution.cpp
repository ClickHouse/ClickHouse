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

    static void generate(Float64 mean, Float64 stddev, ColumnFloat64::Container & container)
    {
        auto distribution = std::normal_distribution<>(mean, stddev);
        for (auto & elem : container)
            elem = distribution(thread_local_rng);
    }
};

struct LogNormalDistribution
{
    using ReturnType = DataTypeFloat64;
    static constexpr const char * getName() { return "randLogNormal"; }
    static constexpr size_t getNumberOfArguments() { return 2; }

    static void generate(Float64 mean, Float64 stddev, ColumnFloat64::Container & container)
    {
        auto distribution = std::lognormal_distribution<>(mean, stddev);
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
        )",
        .syntax="randUniform(min, max)",
        .arguments={
            {"min", "Left boundary of the range. `Float64`."},
            {"max", "Right boundary of the range. `Float64`."}
        },
        .returned_value="A random number of type Float64.",
        .examples{
            {
                "Typical usage",
                "SELECT randUniform(5.5, 10) FROM numbers(5)",
                R"(
┌─randUniform(5.5, 10)─┐
│    8.094978491443102 │
│   7.3181248914450885 │
│    7.177741903868262 │
│    6.483347380953762 │
│    6.122286382885112 │
└──────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });

    factory.registerFunction<FunctionRandomDistribution<NormalDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the normal distribution.
        )",
        .syntax="randNormal(mean, stddev)",
        .arguments={
            {"mean", "Mean value of distribution. `Float64`"}, 
            {"stddev", "[standard deviation](https://en.wikipedia.org/wiki/Standard_deviation) of the distribution. `Float64`."}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage", 
                "SELECT randNormal(10, 2) FROM numbers(5)",
                R"(
┌──randNormal(10, 2)─┐
│ 13.389228911709653 │
│  8.622949707401295 │
│ 10.801887062682981 │
│ 4.5220192605895315 │
│ 10.901239123982567 │
└────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<LogNormalDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the lognormal distribution (a distribution of a random variable whose logarithm is normally distributed).
Accepts two parameters - mean and standard deviation.
        )",
        .syntax="randLogNormal(mean, stddev)",
        .arguments={
            {"mean", "The mean value of the distribution. `Float64`"},
            {"stddev", "The [standard deviation](https://en.wikipedia.org/wiki/Standard_deviation) of the distribution.`Float64`"}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randLogNormal(100, 5) FROM numbers(5)",
                R"(
┌─randLogNormal(100, 5)─┐
│  1.295699673937363e48 │
│  9.719869109186684e39 │
│  6.110868203189557e42 │
│  9.912675872925529e39 │
│ 2.3564708490552458e42 │
└───────────────────────┘                
                )"}
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<ExponentialDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the exponential distribution.
Accepts one parameter - lambda value.
        )",
        .syntax="randExponential(lambda)",
        .arguments={
            {"lambda", "Lambda value. `Float64`."}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randExponential(1/10) FROM numbers(5)",
                R"(
┌─randExponential(divide(1, 10))─┐
│              44.71628934340778 │
│              4.211013337903262 │
│             10.809402553207766 │
│              15.63959406553284 │
│             1.8148392319860158 │
└────────────────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<ChiSquaredDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the chi-squared distribution (a distribution of a sum of the squares of k independent standard normal random variables).
        )",
        .syntax="randChiSquared(degree_of_freedom)",
        .arguments={
            {"degree_of_freedom", "The degree of freedom. `Float64`."}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randChiSquared(10) FROM numbers(5)",
                R"(
┌─randChiSquared(10)─┐
│ 10.015463656521543 │
│  9.621799919882768 │
│   2.71785015634699 │
│ 11.128188665931908 │
│  4.902063104425469 │
└────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });

    factory.registerFunction<FunctionRandomDistribution<StudentTDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the t-distribution.
        )",
        .syntax="randStudentT(degree_of_freedom)",
        .arguments={
            {"degree_of_freedom", "Degree of freedom. `Float64`."}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randStudentT(10) FROM numbers(5)",
                R"(
┌─────randStudentT(10)─┐
│   1.2217309938538725 │
│   1.7941971681200541 │
│ -0.28192176076784664 │
│   0.2508897721303792 │
│  -2.7858432909761186 │
└──────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<FisherFDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the f-distribution.
The F-distribution is the distribution of X = (S1 / d1) / (S2 / d2) where d1 and d2 are degrees of freedom.
Accepts two parameters - degrees of freedom.
        )",
        .syntax="randFisherF(d1, d2)",
        .arguments={
            {"d1", "d1 degree of freedom in `X = (S1 / d1) / (S2 / d2)`. `Float64`."},
            {"d2", "d2 degree of freedom in `X = (S1 / d1) / (S2 / d2)`. `Float64`."}
        },
        .returned_value="Random number. [Float64](../data-types/float.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randFisherF(10, 3) FROM numbers(5)",
                R"(
┌──randFisherF(10, 3)─┐
│   7.286287504216609 │
│ 0.26590779413050386 │
│ 0.22207610901168987 │
│  0.7953362728449572 │
│ 0.19278885985221572 │
└─────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<BernoulliDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the Bernoulli distribution.
Accepts one parameter - probability of success.
        )",
        .syntax="randBernoulli(probability)",
        .arguments={
            {"probability", "The probability of success, a value between `0` and `1`. `Float64`"}
        },
        .returned_value="Random number. [UInt64](../data-types/int-uint.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randBernoulli(.75) FROM numbers(5);",
                R"(
┌─randBernoulli(0.75)─┐
│                   1 │
│                   1 │
│                   0 │
│                   1 │
│                   1 │
└─────────────────────┘
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<BinomialDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the binomial distribution.
Accepts two parameters - number of experiments and probability of success in each experiment.
        )",
        .syntax="randBinomial(experiments, probability)",
        .arguments={
            {"experiments", "Number of experiments. `UInt64`."},
            {"probability", "Probability of success in each experiment, a value between `0` and `1`. `Float64`."}
        },
        .returned_value="Random number. [UInt64](../data-types/int-uint.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randBinomial(100, .75) FROM numbers(5)",
                R"(
┌─randBinomial(100, 0.75)─┐
│                      74 │
│                      78 │
│                      76 │
│                      77 │
│                      80 │
└─────────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<NegativeBinomialDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the negative binomial distribution.
Accepts two parameters - number of experiments and probability of success in each experiment.
        )",
        .syntax="randNegativeBinomial(experiments, probability)",
        .arguments={
            {"experiments", "Number of experiments. `UInt64`."},
            {"probability", "Probability of success in each experiment, a value between `0` and `1`. `Float64`."}
        },
        .returned_value="Random number. [UInt64](../data-types/int-uint.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randNegativeBinomial(100, .75) FROM numbers(5)",
                R"(
┌─randNegativeBinomial(100, 0.75)─┐
│                              33 │
│                              32 │
│                              39 │
│                              40 │
│                              50 │
└─────────────────────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });


    factory.registerFunction<FunctionRandomDistribution<PoissonDistribution>>(
    FunctionDocumentation{
        .description=R"(
Returns a random number from the poisson distribution.
Accepts one parameter - the mean number of occurrences.
        )",
        .syntax="randPoisson(n)",
        .arguments={
            {"n", "Mean number of occurrences. `UInt64`."}
        },
        .returned_value="Random number. [UInt64](../data-types/int-uint.md).",
        .examples{
            {
                "Typical usage",
                "SELECT randPoisson(10) FROM numbers(5)",
                R"(
┌─randPoisson(10)─┐
│               8 │
│               8 │
│               7 │
│              10 │
│               6 │
└─────────────────┘                
                )"
            }
        },
        .category=FunctionDocumentation::Category::RandomNumber
    });
}

}
