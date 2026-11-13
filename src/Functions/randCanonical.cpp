#include <Common/randomSeed.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <pcg-random/pcg_random.hpp>

#include <random>

namespace DB
{

namespace
{

struct CanonicalRandImpl
{
    static void execute(char * output, size_t size)
    {
        pcg64_fast rng1(randomSeed());
        pcg64_fast rng2(randomSeed());
        std::uniform_real_distribution<Float64> distribution1(min, max);
        std::uniform_real_distribution<Float64> distribution2(min, max);

        for (const char * end = output + size; output < end; output += 16)
        {
            unalignedStore<Float64>(output, distribution1(rng1));
            unalignedStore<Float64>(output + 8, distribution2(rng2));
        }
    }
    /// It is guaranteed (by PaddedPODArray) that we can overwrite up to 15 bytes after end.

private:
    const static constexpr Float64 min = 0;
    const static constexpr Float64 max = 1;
};


struct NameCanonicalRand
{
    static constexpr auto name = "randCanonical";
};

class FunctionCanonicalRand final : public FunctionRandomImpl<CanonicalRandImpl, Float64, NameCanonicalRand>
{
public:
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionCanonicalRand>(); }
};


struct NamePostgresRandom
{
    static constexpr auto name = "random";
};

class FunctionPostgresRandom final : public FunctionRandomImpl<CanonicalRandImpl, Float64, NamePostgresRandom>
{
public:
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionPostgresRandom>(); }

    /// PostgreSQL random() accepts no arguments, unlike randCanonical([x]).
    bool isVariadic() const override { return false; }
    String getSignatureString() const override { return "() -> Float64"; }
};

}

REGISTER_FUNCTION(CanonicalRand)
{
    FunctionDocumentation::Description description = R"(
Returns a random distributed `Float64` number with uniform distribution between `0` (inclusive) and `1` (exclusive).
    )";
    FunctionDocumentation::Syntax syntax = "randCanonical([x])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Optional and ignored. The only purpose of the argument is to prevent [common subexpression elimination](/reference/functions/regular-functions/overview#common-subexpression-elimination) when the same function call is used multiple times in a query.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a random Float64 number.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT randCanonical();", "0.345217890123456"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCanonicalRand>(documentation);

    FunctionDocumentation::Description postgres_description = R"(
Returns a non-cryptographic pseudo-random `Float64` number between `0` (inclusive) and `1` (exclusive), as in PostgreSQL.

As with other ClickHouse random functions, identical calls in a query may share a result because of [common subexpression elimination](/reference/functions/regular-functions/overview#common-subexpression-elimination).
    )";
    FunctionDocumentation::Syntax postgres_syntax = "random()";
    FunctionDocumentation::Examples postgres_examples = {
        {"Usage example", "SELECT random();", "0.345217890123456"}
    };
    FunctionDocumentation::IntroducedIn postgres_introduced_in = {26, 11};
    FunctionDocumentation postgres_documentation = {
        postgres_description, postgres_syntax, {}, {}, returned_value, postgres_examples, postgres_introduced_in, category
    };

    factory.registerFunction<FunctionPostgresRandom>(postgres_documentation, FunctionFactory::Case::Insensitive);
}

}
