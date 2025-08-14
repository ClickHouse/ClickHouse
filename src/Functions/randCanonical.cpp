#include <Common/randomSeed.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <pcg-random/pcg_random.hpp>

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

class FunctionCanonicalRand : public FunctionRandomImpl<CanonicalRandImpl, Float64, NameCanonicalRand>
{
public:
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionCanonicalRand>(); }
};

}

REGISTER_FUNCTION(CanonicalRand)
{
    FunctionDocumentation::Description description = R"(
Returns a random and uniformly distributed `Float64` number between `0` (inclusive) and `1` (exclusive).
    )";
    FunctionDocumentation::Syntax syntax = "randCanonical()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a random Float64 number.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT randCanonical();", "0.345217890123456"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::RandomNumber;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCanonicalRand>(documentation);
}

}
