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
    factory.registerFunction<FunctionCanonicalRand>(FunctionDocumentation{
        .description=R"(
The function generates pseudo random results with independent and identically distributed uniformly distributed values in [0, 1).
Non-deterministic. Return type is Float64.
        )",
        .examples{{"randCanonical", "SELECT randCanonical()", ""}},
        .categories{"Mathematical"}});
}

}
