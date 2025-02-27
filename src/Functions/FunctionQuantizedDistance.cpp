#include "FunctionQuantizedDistance.h"
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
    static constexpr std::array<float, 256 * 256> distance_lookup alignas(64) = []() constexpr
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
    static inline std::array<float, 256 * 256> distance_lookup alignas(64) = []() constexpr
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
    static inline std::array<float, 256 * 256> distance_lookup alignas(64) = []() constexpr
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
        if constexpr (std::is_same_v<AccumulatePolicy, L2Accumulate1Bit> || std::is_same_v<AccumulatePolicy, L2Accumulate4Bit>)
            return state.sum;
        else
            return std::sqrt(state.sum);
    }
};

struct CosineProduct16Bit
{
    static inline std::array<float, 65536> square_lookup alignas(64) = []() constexpr
    {
        std::array<float, 65536> table{};
        for (size_t i = 0; i < 65536; ++i)
        {
            float val = Lookup16Bit::dequantize_lookup[i];
            table[i] = val * val;
        }
        return table;
    }();

    static inline float product(UInt16 x, UInt16 y) { return Lookup16Bit::dequantize_lookup[x] * Lookup16Bit::dequantize_lookup[y]; }

    static inline float product(UInt16 x) { return square_lookup[x]; }
};

struct CosineProduct8Bit
{
    static constexpr std::array<float, 256 * 256> product_lookup alignas(64) = []() constexpr
    {
        std::array<float, 256 * 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            for (size_t j = 0; j < 256; ++j)
            {
                table[i * 256 + j] = Lookup8Bit::dequantize_lookup[i] * Lookup8Bit::dequantize_lookup[j];
            }
        }
        return table;
    }();

    static constexpr std::array<float, 256> square_lookup alignas(64) = []() constexpr
    {
        std::array<float, 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            float val = Lookup8Bit::dequantize_lookup[i];
            table[i] = val * val;
        }
        return table;
    }();

    static inline float product(UInt8 x, UInt8 y) { return product_lookup[(static_cast<size_t>(x) << 8) | y]; }

    static inline float product(UInt8 x) { return square_lookup[x]; }
};

struct CosineProduct4Bit
{
    static inline std::array<float, 256> square_lookup alignas(64) = []() constexpr
    {
        std::array<float, 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            auto first_i = Lookup4Bit::dequantize_lookup[i & 0x0F];
            auto second_i = Lookup4Bit::dequantize_lookup[i >> 4];

            float product1 = first_i * first_i;
            float product2 = second_i * second_i;
            table[i] = product1 + product2;
        }
        return table;
    }();

    static inline std::array<float, 256 * 256> product_lookup alignas(64) = []() constexpr
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

                float product1 = first_i * first_j;
                float product2 = second_i * second_j;
                table[i * 256 + j] = product1 + product2;
            }
        }
        return table;
    }();

    static inline float product(UInt8 x, UInt8 y) { return product_lookup[(static_cast<size_t>(x) << 8) | y]; }

    static inline float product(UInt8 x) { return square_lookup[x]; }
};

struct CosineProduct1Bit
{
    static inline std::array<float, 256 * 256> product_lookup alignas(64) = []() constexpr
    {
        std::array<float, 2> dequantize = {-1, 1};
        std::array<float, 256 * 256> table{};
        for (size_t i = 0; i < 256; ++i)
        {
            for (size_t j = 0; j < 256; ++j)
            {
                float product = 0.0f;
                for (size_t k = 0, x = i, y = j; k < 8; ++k) {
                    product += dequantize[x % 2] * dequantize[y % 2];
                    x >>= 1;
                    y >>= 1;
                }

                table[i * 256 + j] = product;
            }
        }
        return table;
    }();

    static inline float product(UInt8 x, UInt8 y) { return product_lookup[(static_cast<size_t>(x) << 8) | y]; }

    static inline float product(UInt8) { return 8; }
};

template <typename T, typename ProductPolicy>
struct CosineDistanceGeneric
{
    using ValueType = T;

    template <typename FloatType>
    struct State
    {
        FloatType dot_prod{};
        FloatType x_squared{};
        FloatType y_squared{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, T x, T y)
    {
        state.dot_prod += ProductPolicy::product(x, y);
        state.x_squared += ProductPolicy::product(x);
        state.y_squared += ProductPolicy::product(y);
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state)
    {
        state.dot_prod += other_state.dot_prod;
        state.x_squared += other_state.x_squared;
        state.y_squared += other_state.y_squared;
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state)
    {
        return 1.0f - state.dot_prod / sqrt(state.x_squared * state.y_squared);
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

REGISTER_FUNCTION(Quantized16BitCosineDistance)
{
    static constexpr char name[] = "quantized16BitCosineDistance";
    factory.registerFunction<FunctionQuantizedL2Distance<CosineDistanceGeneric<UInt16, CosineProduct16Bit>, name>>();
}

REGISTER_FUNCTION(Quantized8BitCosineDistance)
{
    static constexpr char name[] = "quantized8BitCosineDistance";
    factory.registerFunction<FunctionQuantizedL2Distance<CosineDistanceGeneric<UInt8, CosineProduct8Bit>, name>>();
}

REGISTER_FUNCTION(Quantized4BitCosineDistance)
{
    static constexpr char name[] = "quantized4BitCosineDistance";
    factory.registerFunction<FunctionQuantizedL2Distance<CosineDistanceGeneric<UInt8, CosineProduct4Bit>, name>>();
}

REGISTER_FUNCTION(Quantized1BitCosineDistance)
{
    static constexpr char name[] = "quantized1BitCosineDistance";
    factory.registerFunction<FunctionQuantizedL2Distance<CosineDistanceGeneric<UInt8, CosineProduct1Bit>, name>>();
}

}
