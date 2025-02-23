#include "FunctionQuantizedL2Distance.h"
#include "Functions/FunctionDequantize.h"
#include "base/types.h"

namespace DB
{

struct L2Accumulate16Bit
{
    static inline float accumulate(UInt16 x, UInt16 y)
    {
        float diff = Lookup16Bit::dequantize_lookup[x] - Lookup16Bit::dequantize_lookup[y];
        return diff * diff;
    }
};

struct L2Accumulate8Bit
{
    static constexpr std::array<float, 256 * 256> distance_lookup = []() constexpr
    {
        std::array<float, 256 * 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            for (size_t j = 0; j < 256; ++j)
            {
                float diff = Lookup8Bit::dequantize_lookup[i] - Lookup8Bit::dequantize_lookup[j];
                table[i * 256 + j] = diff * diff;
            }
        }
        return table;
    }();

    static inline float accumulate(UInt8 x, UInt8 y) { return distance_lookup[(static_cast<size_t>(x) << 8) | y]; }
};

struct L2Accumulate4Bit
{
    static inline std::array<float, 256 * 256> distance_lookup = []() constexpr
    {
        std::array<float, 256 * 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            for (size_t j = 0; j < 256; ++j)
            {
                auto first_i = Lookup4Bit::dequantize_lookup[i & 0x0F];
                auto second_i = Lookup4Bit::dequantize_lookup[i >> 4];

                auto first_j = Lookup4Bit::dequantize_lookup[j & 0x0F];
                auto second_j = Lookup4Bit::dequantize_lookup[j >> 4];

                float diff1 = first_i - first_j;
                float diff2 = second_i - second_j;
                table[i * 256 + j] = diff1 * diff1 + diff2 * diff2;
            }
        }
        return table;
    }();

    static inline float accumulate(UInt8 x, UInt8 y) { return distance_lookup[(static_cast<size_t>(x) << 8) | y]; }
};

struct L2Accumulate1Bit
{
    static inline std::array<float, 256 * 256> distance_lookup = []() constexpr
    {
        std::array<float, 256 * 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            for (size_t j = 0; j < 256; ++j)
            {
                UInt8 xor_val = static_cast<UInt8>(i ^ j);
                int popcnt = 0;
                for (int k = 0; k < 8; ++k)
                {
                    popcnt += (xor_val >> k) & 0x1;
                }

                table[i * 256 + j] = 4.0f * popcnt;
            }
        }
        return table;
    }();

    static inline float accumulate(UInt8 x, UInt8 y) { return distance_lookup[(static_cast<size_t>(x) << 8) | y]; }
};

template <typename T, typename AccumulatePolicy>
struct L2DistanceGeneric
{
    using ValueType = T;

    template <typename FloatType>
    struct State
    {
        FloatType sum = 0;
    };

    template <typename ResultType>
    static inline void accumulate(State<ResultType> & state, T x, T y)
    {
        state.sum += AccumulatePolicy::accumulate(x, y);
    }

    template <typename ResultType>
    static inline void combine(State<ResultType> & state, const State<ResultType> & other_state)
    {
        state.sum += other_state.sum;
    }

    template <typename ResultType>
    static inline ResultType finalize(const State<ResultType> & state)
    {
        if constexpr (std::is_same_v<AccumulatePolicy, L2Accumulate1Bit>)
            return state.sum;
        else
            return std::sqrt(state.sum);
    }
};

REGISTER_FUNCTION(Quantized16BitL2Distance)
{
    static constexpr char name[] = "quantized16BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2DistanceGeneric<UInt16, L2Accumulate16Bit>, name>>();
}

REGISTER_FUNCTION(Quantized8BitL2Distance)
{
    static constexpr char name[] = "quantized8BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2DistanceGeneric<UInt8, L2Accumulate8Bit>, name>>();
}

REGISTER_FUNCTION(Quantized4BitL2Distance)
{
    static constexpr char name[] = "quantized4BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2DistanceGeneric<UInt8, L2Accumulate4Bit>, name>>();
}

REGISTER_FUNCTION(Quantized1BitL2Distance)
{
    static constexpr char name[] = "quantized1BitL2Distance";
    factory.registerFunction<FunctionQuantizedL2Distance<L2DistanceGeneric<UInt8, L2Accumulate1Bit>, name>>();
}

}
