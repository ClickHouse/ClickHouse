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

        auto distribution = std::binomial_distribution<UInt64>(t, p);
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

        auto distribution = std::negative_binomial_distribution<UInt64>(t, p);
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
        auto distribution = std::poisson_distribution<UInt64>(n);
        for (auto & elem : container)
            elem = static_cast<UInt64>(distribution(thread_local_rng));
    }
};

}

/** Function which will generate values according to the specified distribution
  * Accepts only constant arguments
  * Similar to the functions rand and rand64 an additional 'tag' argument could be added to the
  * end of arguments list (this argument will be ignored) which suppresses common subexpression elimination.
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
    FunctionDocumentation::Description description = R"(
Returns a random Float64 number drawn uniformly from the interval $[\min, \max]$.
    )";
    FunctionDocumentation::Syntax syntax = "randUniform(min, max[, x])";
    FunctionDocumentation::Arguments arguments = {
        {"min", "Left boundary of the range (inclusive).", {"Float64"}},
        {"max", "Right boundary of the range (inclusive).", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a random number drawn uniformly from the interval formed by `min` and `max`.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT randUniform(5.5, 10) FROM numbers(5)", R"(
┌─randUniform(5.5, 10)─┐
│    8.094978491443102 │
│   7.3181248914450885 │
│    7.177741903868262 │
│    6.483347380953762 │
│    6.122286382885112 │
└──────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRandomDistribution<UniformDistribution>>(documentation);

    FunctionDocumentation::Description description_normal = R"(
Returns a random Float64 number drawn from a [normal distribution](https://en.wikipedia.org/wiki/Normal_distribution).
    )";
    FunctionDocumentation::Syntax syntax_normal = "randNormal(mean, stddev[, x])";
    FunctionDocumentation::Arguments arguments_normal = {
        {"mean", "The mean value of distribution", {"Float64"}},
        {"stddev", "The standard deviation of the distribution", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_normal = {"Returns a random Float64 number drawn from the specified normal distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_normal = {
        {"Usage example", "SELECT randNormal(10, 2) FROM numbers(5)", R"(
┌──randNormal(10, 2)─┐
│ 13.389228911709653 │
│  8.622949707401295 │
│ 10.801887062682981 │
│ 4.5220192605895315 │
│ 10.901239123982567 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_normal = {22, 10};
    FunctionDocumentation::Category category_normal = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_normal = {description_normal, syntax_normal, arguments_normal, returned_value_normal, examples_normal, introduced_in_normal, category_normal};

    factory.registerFunction<FunctionRandomDistribution<NormalDistribution>>(documentation_normal);


    FunctionDocumentation::Description description_lognormal = R"(
Returns a random Float64 number drawn from a [log-normal distribution](https://en.wikipedia.org/wiki/Log-normal_distribution).
    )";
    FunctionDocumentation::Syntax syntax_lognormal = "randLogNormal(mean, stddev[, x])";
    FunctionDocumentation::Arguments arguments_lognormal = {
        {"mean", "The mean value of distribution.", {"Float64"}},
        {"stddev", "The standard deviation of the distribution.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_lognormal = {"Returns a random Float64 number drawn from the specified log-normal distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_lognormal = {
        {"Usage example", "SELECT randLogNormal(100, 5) FROM numbers(5)", R"(
┌─randLogNormal(100, 5)─┐
│  1.295699673937363e48 │
│  9.719869109186684e39 │
│  6.110868203189557e42 │
│  9.912675872925529e39 │
│ 2.3564708490552458e42 │
└───────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_lognormal = {22, 10};
    FunctionDocumentation::Category category_lognormal = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_lognormal = {description_lognormal, syntax_lognormal, arguments_lognormal, returned_value_lognormal, examples_lognormal, introduced_in_lognormal, category_lognormal};

    factory.registerFunction<FunctionRandomDistribution<LogNormalDistribution>>(documentation_lognormal);


    FunctionDocumentation::Description description_exponential = R"(
Returns a random Float64 number drawn from an [exponential distribution](https://en.wikipedia.org/wiki/Exponential_distribution).
    )";
    FunctionDocumentation::Syntax syntax_exponential = "randExponential(lambda[, x])";
    FunctionDocumentation::Arguments arguments_exponential = {
        {"lambda", "Rate parameter or lambda value of the distribution", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_exponential = {"Returns a random Float64 number drawn from the specified exponential distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_exponential = {
        {"Usage example", "SELECT randExponential(1/10) FROM numbers(5)", R"(
┌─randExponential(divide(1, 10))─┐
│              44.71628934340778 │
│              4.211013337903262 │
│             10.809402553207766 │
│              15.63959406553284 │
│             1.8148392319860158 │
└────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_exponential = {22, 10};
    FunctionDocumentation::Category category_exponential = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_exponential = {description_exponential, syntax_exponential, arguments_exponential, returned_value_exponential, examples_exponential, introduced_in_exponential, category_exponential};

    factory.registerFunction<FunctionRandomDistribution<ExponentialDistribution>>(documentation_exponential);


    FunctionDocumentation::Description description_chisquared = R"(
Returns a random Float64 number drawn from a [chi-square distribution](https://en.wikipedia.org/wiki/Chi-squared_distribution).
    )";
    FunctionDocumentation::Syntax syntax_chisquared = "randChiSquared(degree_of_freedom[, x])";
    FunctionDocumentation::Arguments arguments_chisquared = {
        {"degree_of_freedom", "Degrees of freedom.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_chisquared = {"Returns a random Float64 number drawn from the specified chi-square distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_chisquared = {
        {"Usage example", "SELECT randChiSquared(10) FROM numbers(5)", R"(
┌─randChiSquared(10)─┐
│ 10.015463656521543 │
│  9.621799919882768 │
│   2.71785015634699 │
│ 11.128188665931908 │
│  4.902063104425469 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_chisquared = {22, 10};
    FunctionDocumentation::Category category_chisquared = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_chisquared = {description_chisquared, syntax_chisquared, arguments_chisquared, returned_value_chisquared, examples_chisquared, introduced_in_chisquared, category_chisquared};

    factory.registerFunction<FunctionRandomDistribution<ChiSquaredDistribution>>(documentation_chisquared);

    FunctionDocumentation::Description description_studentt = R"(
Returns a random Float64 number drawn from a [Student's t-distribution](https://en.wikipedia.org/wiki/Student%27s_t-distribution).
    )";
    FunctionDocumentation::Syntax syntax_studentt = "randStudentT(degree_of_freedom[, x])";
    FunctionDocumentation::Arguments arguments_studentt = {
        {"degree_of_freedom", "Degrees of freedom.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_studentt = {"Returns a random Float64 number drawn from the specified Student's t-distribution.", {"Float64"}};
    FunctionDocumentation::Examples examples_studentt = {
        {"Usage example", "SELECT randStudentT(10) FROM numbers(5)", R"(
┌─────randStudentT(10)─┐
│   1.2217309938538725 │
│   1.7941971681200541 │
│ -0.28192176076784664 │
│   0.2508897721303792 │
│  -2.7858432909761186 │
└──────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_studentt = {22, 10};
    FunctionDocumentation::Category category_studentt = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_studentt = {description_studentt, syntax_studentt, arguments_studentt, returned_value_studentt, examples_studentt, introduced_in_studentt, category_studentt};

    factory.registerFunction<FunctionRandomDistribution<StudentTDistribution>>(documentation_studentt);


    FunctionDocumentation::Description description_fisherf = R"(
Returns a random Float64 number drawn from an [F-distribution](https://en.wikipedia.org/wiki/F-distribution).
    )";
    FunctionDocumentation::Syntax syntax_fisherf = "randFisherF(d1, d2[, x])";
    FunctionDocumentation::Arguments arguments_fisherf = {
        {"d1", "d1 degree of freedom in `X = (S1 / d1) / (S2 / d2)`.", {"Float64"}},
        {"d2", "d2 degree of freedom in `X = (S1 / d1) / (S2 / d2)`.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fisherf = {"Returns a random Float64 number drawn from the specified F-distribution", {"Float64"}};
    FunctionDocumentation::Examples examples_fisherf = {
        {"Usage example", "SELECT randFisherF(10, 3) FROM numbers(5)", R"(
┌─randFisherF(10, 20)─┐
│  0.7204609609506184 │
│  0.9926258472572916 │
│  1.4010752726735863 │
│ 0.34928401507025556 │
│  1.8216216009473598 │
└─────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_fisherf = {22, 10};
    FunctionDocumentation::Category category_fisherf = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_fisherf = {description_fisherf, syntax_fisherf, arguments_fisherf, returned_value_fisherf, examples_fisherf, introduced_in_fisherf, category_fisherf};

    factory.registerFunction<FunctionRandomDistribution<FisherFDistribution>>(documentation_fisherf);


    FunctionDocumentation::Description description_bernoulli = R"(
Returns a random Float64 number drawn from a [Bernoulli distribution](https://en.wikipedia.org/wiki/Bernoulli_distribution).
    )";
    FunctionDocumentation::Syntax syntax_bernoulli = "randBernoulli(probability[, x])";
    FunctionDocumentation::Arguments arguments_bernoulli = {
        {"probability", "The probability of success as a value between `0` and `1`.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_bernoulli = {"Returns a random Float64 number drawn from the specified Bernoulli distribution.", {"UInt64"}};
    FunctionDocumentation::Examples examples_bernoulli = {
        {"Usage example", "SELECT randBernoulli(.75) FROM numbers(5)", R"(
┌─randBernoulli(0.75)─┐
│                   1 │
│                   1 │
│                   0 │
│                   1 │
│                   1 │
└─────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_bernoulli = {22, 10};
    FunctionDocumentation::Category category_bernoulli = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_bernoulli = {description_bernoulli, syntax_bernoulli, arguments_bernoulli, returned_value_bernoulli, examples_bernoulli, introduced_in_bernoulli, category_bernoulli};

    factory.registerFunction<FunctionRandomDistribution<BernoulliDistribution>>(documentation_bernoulli);


    FunctionDocumentation::Description description_binomial = R"(
Returns a random Float64 number drawn from a [binomial distribution](https://en.wikipedia.org/wiki/Binomial_distribution).
    )";
    FunctionDocumentation::Syntax syntax_binomial = "randBinomial(experiments, probability[, x])";
    FunctionDocumentation::Arguments arguments_binomial = {
        {"experiments", "The number of experiments", {"UInt64"}},
        {"probability", "The probability of success in each experiment as a value between `0` and `1`", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_binomial = {"Returns a random Float64 number drawn from the specified binomial distribution.", {"UInt64"}};
    FunctionDocumentation::Examples examples_binomial = {
        {"Usage example", "SELECT randBinomial(100, .75) FROM numbers(5)", R"(
┌─randBinomial(100, 0.75)─┐
│                      74 │
│                      78 │
│                      76 │
│                      77 │
│                      80 │
└─────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_binomial = {22, 10};
    FunctionDocumentation::Category category_binomial = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_binomial = {description_binomial, syntax_binomial, arguments_binomial, returned_value_binomial, examples_binomial, introduced_in_binomial, category_binomial};

    factory.registerFunction<FunctionRandomDistribution<BinomialDistribution>>(documentation_binomial);


    FunctionDocumentation::Description description_negativebinomial = R"(
Returns a random Float64 number drawn from a [negative binomial distribution](https://en.wikipedia.org/wiki/Negative_binomial_distribution).
    )";
    FunctionDocumentation::Syntax syntax_negativebinomial = "randNegativeBinomial(experiments, probability[, x])";
    FunctionDocumentation::Arguments arguments_negativebinomial = {
        {"experiments", "The number of experiments.", {"UInt64"}},
        {"probability", "`The probability of failure in each experiment as a value between `0` and `1`.", {"Float64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_negativebinomial = {"Returns a random Float64 number drawn from the specified negative binomial distribution", {"UInt64"}};
    FunctionDocumentation::Examples examples_negativebinomial = {
        {"Usage example", "SELECT randNegativeBinomial(100, .75) FROM numbers(5)", R"(
┌─randNegativeBinomial(100, 0.75)─┐
│                              33 │
│                              32 │
│                              39 │
│                              40 │
│                              50 │
└─────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_negativebinomial = {22, 10};
    FunctionDocumentation::Category category_negativebinomial = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_negativebinomial = {description_negativebinomial, syntax_negativebinomial, arguments_negativebinomial, returned_value_negativebinomial, examples_negativebinomial, introduced_in_negativebinomial, category_negativebinomial};

    factory.registerFunction<FunctionRandomDistribution<NegativeBinomialDistribution>>(documentation_negativebinomial);


    FunctionDocumentation::Description description_poisson = R"(
Returns a random Float64 number drawn from a [Poisson distribution](https://en.wikipedia.org/wiki/Poisson_distribution) distribution.
    )";
    FunctionDocumentation::Syntax syntax_poisson = "randPoisson(n[, x])";
    FunctionDocumentation::Arguments arguments_poisson = {
        {"n", "The mean number of occurrences.", {"UInt64"}},
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_poisson = {"Returns a random Float64 number drawn from the specified Poisson distribution.", {"UInt64"}};
    FunctionDocumentation::Examples examples_poisson = {
        {"Usage example", "SELECT randPoisson(10) FROM numbers(5)", R"(
┌─randPoisson(10)─┐
│               8 │
│               8 │
│               7 │
│              10 │
│               6 │
└─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_poisson = {22, 10};
    FunctionDocumentation::Category category_poisson = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation_poisson = {description_poisson, syntax_poisson, arguments_poisson, returned_value_poisson, examples_poisson, introduced_in_poisson, category_poisson};

    factory.registerFunction<FunctionRandomDistribution<PoissonDistribution>>(documentation_poisson);
}

}
