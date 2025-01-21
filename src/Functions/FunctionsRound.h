#pragma once

#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnVector.h>
#include <Interpreters/castColumn.h>
#include "IFunction.h"
#include <Common/intExp.h>
#include <Common/assert_cast.h>
#include <Core/Defines.h>
#include <cmath>
#include <type_traits>
#include <array>
#include <base/sort.h>
#include <algorithm>

#ifdef __SSE4_1__
    #include <smmintrin.h>
#else
    #include <fenv.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_SET_ROUNDING_MODE;
}


/// Rounding Functions:
/// - round(x, N) - rounding to nearest (N = 0 by default). Use banker's rounding for floating point numbers.
/// - roundBankers(x, N) - rounding to nearest (N = 0 by default). Use banker's rounding for all numbers.
/// - floor(x, N) is the largest number <= x (N = 0 by default).
/// - ceil(x, N) is the smallest number >= x (N = 0 by default).
/// - trunc(x, N) - is the largest by absolute value number that is not greater than x by absolute value (N = 0 by default).

/// The value of the parameter N (scale):
/// - N > 0: round to the number with N decimal places after the decimal point
/// - N < 0: round to an integer with N zero characters
/// - N = 0: round to an integer

/// Type of the result is the type of argument.
/// For integer arguments, when passing negative scale, overflow can occur. In that case, the behavior is undefined.

/// Controls the behavior of the rounding functions.
enum class ScaleMode : uint8_t
{
    Positive,   // round to a number with N decimal places after the decimal point
    Negative,   // round to an integer with N zero characters
    Zero,       // round to an integer
};

enum class RoundingMode : uint8_t
{
#ifdef __SSE4_1__
    Round   = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor   = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil    = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc   = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round   = 8,    /// Values correspond to above values, just in case.
    Floor   = 9,
    Ceil    = 10,
    Trunc   = 11,
#endif
};

enum class TieBreakingMode : uint8_t
{
    Auto,    /// banker's rounding for floating point numbers, round up otherwise
    Bankers, /// banker's rounding
};

enum class Vectorize : uint8_t
{
    No,
    Yes
};

/// For N, no more than the number of digits in the largest type.
using Scale = Int16;


/// Rounding functions for integer values.
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, TieBreakingMode tie_breaking_mode>
struct IntegerRoundingComputation
{
    static const size_t data_count = 1;

    static size_t prepare(size_t scale)
    {
        return scale;
    }

    /// Integer overflow is Ok.
    static ALWAYS_INLINE_NO_SANITIZE_UNDEFINED T computeImpl(T x, T scale)
    {
        switch (rounding_mode)
        {
            case RoundingMode::Trunc:
            {
                return x / scale * scale;
            }
            case RoundingMode::Floor:
            {
                if (x < 0)
                    x -= scale - 1;
                return x / scale * scale;
            }
            case RoundingMode::Ceil:
            {
                if (x >= 0)
                    x += scale - 1;
                return x / scale * scale;
            }
            case RoundingMode::Round:
            {
                if (x < 0)
                    x -= scale;
                switch (tie_breaking_mode)
                {
                    case TieBreakingMode::Auto:
                        x = (x + scale / 2) / scale * scale;
                        break;
                    case TieBreakingMode::Bankers:
                    {
                        T quotient = (x + scale / 2) / scale;
                        if (quotient * scale == x + scale / 2)
                            // round half to even
                            x = ((quotient + (x < 0)) & ~1) * scale;
                        else
                            // round the others as usual
                            x = quotient * scale;
                        break;
                    }
                }
                return x;
            }
        }

        std::unreachable();
    }

    static ALWAYS_INLINE T compute(T x, T scale)
    {
        switch (scale_mode)
        {
            case ScaleMode::Zero:
            case ScaleMode::Positive:
                return x;
            case ScaleMode::Negative:
                return computeImpl(x, scale);
        }

        std::unreachable();
    }

    static ALWAYS_INLINE void compute(const T * __restrict in, size_t scale, T * __restrict out)
    requires std::integral<T>
    {
        if constexpr (sizeof(T) <= sizeof(scale) && scale_mode == ScaleMode::Negative)
        {
            if (scale > size_t(std::numeric_limits<T>::max()))
            {
                *out = 0;
                return;
            }
        }
        *out = compute(*in, static_cast<T>(scale));
    }

    static ALWAYS_INLINE void compute(const T * __restrict in, T scale, T * __restrict out)
    requires(!std::integral<T>)
    {
        *out = compute(*in, scale);
    }
};


template <typename T, Vectorize vectorize>
class FloatRoundingComputationBase;

#ifdef __SSE4_1__

/// Vectorized implementation for x86.

template <>
class FloatRoundingComputationBase<Float32, Vectorize::Yes>
{
public:
    using ScalarType = Float32;
    using VectorType = __m128;
    static const size_t data_count = 4;

    static VectorType load(const ScalarType * in) { return _mm_loadu_ps(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_ps(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_ps(out, val);}
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_ps(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_ps(val, scale); }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return _mm_round_ps(val, int(mode)); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
};

template <>
class FloatRoundingComputationBase<Float64, Vectorize::Yes>
{
public:
    using ScalarType = Float64;
    using VectorType = __m128d;
    static const size_t data_count = 2;

    static VectorType load(const ScalarType * in) { return _mm_loadu_pd(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_pd(&in); }
    static void store(ScalarType * out, VectorType val) { _mm_storeu_pd(out, val);}
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_pd(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_pd(val, scale); }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return _mm_round_pd(val, int(mode)); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
};

#endif

/// Sequential implementation for ARM. Also used for scalar arguments.

inline float roundWithMode(float x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return nearbyintf(x);
        case RoundingMode::Floor: return floorf(x);
        case RoundingMode::Ceil: return ceilf(x);
        case RoundingMode::Trunc: return truncf(x);
    }

    std::unreachable();
}

inline double roundWithMode(double x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return nearbyint(x);
        case RoundingMode::Floor: return floor(x);
        case RoundingMode::Ceil: return ceil(x);
        case RoundingMode::Trunc: return trunc(x);
    }

    std::unreachable();
}

template <typename T>
class FloatRoundingComputationBase<T, Vectorize::No>
{
public:
    using ScalarType = T;
    using VectorType = T;
    static const size_t data_count = 1;

    static VectorType load(const ScalarType * in) { return *in; }
    static VectorType load1(const ScalarType in) { return in; }
    static VectorType store(ScalarType * out, ScalarType val) { return *out = val;}
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <RoundingMode mode> static VectorType apply(VectorType val) { return roundWithMode(val, mode); }

    static VectorType prepare(size_t scale)
    {
        return load1(scale);
    }
};


/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, Vectorize vectorize>
class FloatRoundingComputation : public FloatRoundingComputationBase<T, vectorize>
{
    using Base = FloatRoundingComputationBase<T, vectorize>;

public:
    static void compute(const T * __restrict in, const typename Base::VectorType & scale, T * __restrict out)
    {
        auto val = Base::load(in);

        if (scale_mode == ScaleMode::Positive)
            val = Base::multiply(val, scale);
        else if (scale_mode == ScaleMode::Negative)
            val = Base::divide(val, scale);

        val = Base::template apply<rounding_mode>(val);

        if (scale_mode == ScaleMode::Positive)
            val = Base::divide(val, scale);
        else if (scale_mode == ScaleMode::Negative)
            val = Base::multiply(val, scale);

        Base::store(out, val);
    }
};


/** Implementing high-level rounding functions.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FloatRoundingImpl
{
private:
    static_assert(!is_decimal<T>);

    template <Vectorize vectorize =
#ifdef __SSE4_1__
        Vectorize::Yes
#else
        Vectorize::No
#endif
    >
    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode, vectorize>;
    using Data = std::array<T, Op<>::data_count>;
    using ColumnType = ColumnVector<T>;
    using Container = typename ColumnType::Container;

public:
    static NO_INLINE void apply(const Container & in, size_t scale, Container & out)
    {
        auto mm_scale = Op<>::prepare(scale);

        const size_t data_count = std::tuple_size<Data>();

        const T* end_in = in.data() + in.size();
        const T* limit = in.data() + in.size() / data_count * data_count;

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

        while (p_in < limit)
        {
            Op<>::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp_src{{}};
            Data tmp_dst;

            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);

            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op<>::compute(reinterpret_cast<T *>(&tmp_src), mm_scale, reinterpret_cast<T *>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }
    }

    static void applyOne(T in, size_t scale, T& out)
    {
        using ScalarOp = Op<Vectorize::No>;
        auto s = ScalarOp::prepare(scale);
        ScalarOp::compute(&in, s, &out);
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, TieBreakingMode tie_breaking_mode>
struct IntegerRoundingImpl
{
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode, tie_breaking_mode>;
    using Container = typename ColumnVector<T>::Container;

public:
    template <size_t scale>
    static NO_INLINE void applyImpl(const Container & in, Container & out)
    {
        const T * end_in = in.data() + in.size();

        const T * __restrict p_in = in.data();
        T * __restrict p_out = out.data();

        while (p_in < end_in)
        {
            Op::compute(p_in, scale, p_out);
            ++p_in;
            ++p_out;
        }
    }

    static NO_INLINE void apply(const Container & in, size_t scale, Container & out)
    {
        /// Manual function cloning for compiler to generate integer division by constant.
        switch (scale)
        {
            case 1ULL: return applyImpl<1ULL>(in, out);
            case 10ULL: return applyImpl<10ULL>(in, out);
            case 100ULL: return applyImpl<100ULL>(in, out);
            case 1000ULL: return applyImpl<1000ULL>(in, out);
            case 10000ULL: return applyImpl<10000ULL>(in, out);
            case 100000ULL: return applyImpl<100000ULL>(in, out);
            case 1000000ULL: return applyImpl<1000000ULL>(in, out);
            case 10000000ULL: return applyImpl<10000000ULL>(in, out);
            case 100000000ULL: return applyImpl<100000000ULL>(in, out);
            case 1000000000ULL: return applyImpl<1000000000ULL>(in, out);
            case 10000000000ULL: return applyImpl<10000000000ULL>(in, out);
            case 100000000000ULL: return applyImpl<100000000000ULL>(in, out);
            case 1000000000000ULL: return applyImpl<1000000000000ULL>(in, out);
            case 10000000000000ULL: return applyImpl<10000000000000ULL>(in, out);
            case 100000000000000ULL: return applyImpl<100000000000000ULL>(in, out);
            case 1000000000000000ULL: return applyImpl<1000000000000000ULL>(in, out);
            case 10000000000000000ULL: return applyImpl<10000000000000000ULL>(in, out);
            case 100000000000000000ULL: return applyImpl<100000000000000000ULL>(in, out);
            case 1000000000000000000ULL: return applyImpl<1000000000000000000ULL>(in, out);
            case 10000000000000000000ULL: return applyImpl<10000000000000000000ULL>(in, out);
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected 'scale' parameter passed to function");
        }
    }

    static void applyOne(T in, size_t scale, T& out)
    {
        Op::compute(&in, scale, &out);
    }
};


template <is_decimal T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
class DecimalRoundingImpl
{
private:
    using NativeType = typename T::NativeType;
    using Op = IntegerRoundingComputation<NativeType, rounding_mode, ScaleMode::Negative, tie_breaking_mode>;
    using Container = typename ColumnDecimal<T>::Container;

public:
    static NO_INLINE void apply(const Container & in, UInt32 in_scale, Container & out, Scale scale_arg)
    {
        scale_arg = in_scale - scale_arg;
        if (scale_arg > 0)
        {
            auto scale = intExp10OfSize<NativeType>(scale_arg);

            const NativeType * __restrict p_in = reinterpret_cast<const NativeType *>(in.data());
            const NativeType * end_in = reinterpret_cast<const NativeType *>(in.data()) + in.size();
            NativeType * __restrict p_out = reinterpret_cast<NativeType *>(out.data());

            while (p_in < end_in)
            {
                Op::compute(p_in, scale, p_out);
                ++p_in;
                ++p_out;
            }
        }
        else
        {
            memcpy(out.data(), in.data(), in.size() * sizeof(T));
        }
    }

    static void applyOne(NativeType in, UInt32 in_scale, NativeType& out, Scale scale_arg)
    {
        scale_arg = in_scale - scale_arg;
        if (scale_arg > 0)
        {
            auto scale = intExp10OfSize<NativeType>(scale_arg);
            Op::compute(&in, scale, &out);
        }
        else
        {
            memcpy(&out, &in, sizeof(T));
        }
    }
};

/// Select the appropriate processing algorithm depending on the scale.
inline void validateScale(Int64 scale64)
{
    if (scale64 > std::numeric_limits<Scale>::max() || scale64 < std::numeric_limits<Scale>::min())
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale argument for rounding function is too large");
}

inline Scale getScaleArg(const ColumnConst* scale_col)
{
    const auto & scale_field = scale_col->getField();

    Int64 scale64 = scale_field.safeGet<Int64>();
    validateScale(scale64);

    return scale64;
}

/// Generic dispatcher
template <typename T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
struct Dispatcher
{
    template <ScaleMode scale_mode>
    using FunctionRoundingImpl = std::conditional_t<std::is_floating_point_v<T>,
        FloatRoundingImpl<T, rounding_mode, scale_mode>,
        IntegerRoundingImpl<T, rounding_mode, scale_mode, tie_breaking_mode>>;

    template <typename ScaleType>
    static ColumnPtr apply(const IColumn * value_col, const IColumn * scale_col = nullptr)
    {
        // Non-const value argument:
        const auto * value_col_typed = checkAndGetColumn<ColumnVector<T>>(value_col);
        if (value_col_typed)
        {
            auto col_res = ColumnVector<T>::create();

            typename ColumnVector<T>::Container & vec_res = col_res->getData();
            vec_res.resize(value_col_typed->getData().size());

            if (!vec_res.empty())
            {
                // Const scale argument:
                if (scale_col == nullptr || isColumnConst(*scale_col))
                {
                    auto scale_arg = (scale_col == nullptr) ? 0 : getScaleArg(checkAndGetColumnConst<ColumnVector<ScaleType>>(scale_col));
                    if (scale_arg == 0)
                    {
                        size_t scale = 1;
                        FunctionRoundingImpl<ScaleMode::Zero>::apply(value_col_typed->getData(), scale, vec_res);
                    }
                    else if (scale_arg > 0)
                    {
                        size_t scale = intExp10(scale_arg);
                        FunctionRoundingImpl<ScaleMode::Positive>::apply(value_col_typed->getData(), scale, vec_res);
                    }
                    else
                    {
                        size_t scale = intExp10(-scale_arg);
                        FunctionRoundingImpl<ScaleMode::Negative>::apply(value_col_typed->getData(), scale, vec_res);
                    }
                }
                /// Non-const scale argument:
                else if (const auto * scale_col_typed = checkAndGetColumn<ColumnVector<ScaleType>>(scale_col))
                {
                    const auto & value_data = value_col_typed->getData();
                    const auto & scale_data = scale_col_typed->getData();
                    const size_t rows = value_data.size();

                    for (size_t i = 0; i < rows; ++i)
                    {
                        Int64 scale64 = scale_data[i];
                        validateScale(scale64);
                        Scale raw_scale = scale64;

                        if (raw_scale == 0)
                        {
                            size_t scale = 1;
                            FunctionRoundingImpl<ScaleMode::Zero>::applyOne(value_data[i], scale, vec_res[i]);
                        }
                        else if (raw_scale > 0)
                        {
                            size_t scale = intExp10(raw_scale);
                            FunctionRoundingImpl<ScaleMode::Positive>::applyOne(value_data[i], scale, vec_res[i]);
                        }
                        else
                        {
                            size_t scale = intExp10(-raw_scale);
                            FunctionRoundingImpl<ScaleMode::Negative>::applyOne(value_data[i], scale, vec_res[i]);
                        }
                    }
                }
            }
            return col_res;
        }
        // Const value argument:
        const auto * value_col_typed_const = checkAndGetColumnConst<ColumnVector<T>>(value_col);
        if (value_col_typed_const)
        {
            auto value_col_full = value_col_typed_const->convertToFullColumn();
            return apply<ScaleType>(value_col_full.get(), scale_col);
        }
        return nullptr;
    }
};

/// Dispatcher for Decimal inputs
template <is_decimal T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
struct Dispatcher<T, rounding_mode, tie_breaking_mode>
{
public:
    template <typename ScaleType>
    static ColumnPtr apply(const IColumn * value_col, const IColumn * scale_col = nullptr)
    {
        // Non-const value argument:
        const auto * value_col_typed = checkAndGetColumn<ColumnDecimal<T>>(value_col);
        if (value_col_typed)
        {
            const typename ColumnDecimal<T>::Container & vec_src = value_col_typed->getData();

            auto col_res = ColumnDecimal<T>::create(vec_src.size(), value_col_typed->getScale());
            auto & vec_res = col_res->getData();
            vec_res.resize(vec_src.size());

            if (!vec_res.empty())
            {
                /// Const scale argument:
                if (scale_col == nullptr || isColumnConst(*scale_col))
                {
                    auto scale_arg = scale_col == nullptr ? 0 : getScaleArg(checkAndGetColumnConst<ColumnVector<ScaleType>>(scale_col));
                    DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>::apply(vec_src, value_col_typed->getScale(), vec_res, scale_arg);
                }
                /// Non-const scale argument:
                else if (const auto * scale_col_typed = checkAndGetColumn<ColumnVector<ScaleType>>(scale_col))
                {
                    const auto & scale = scale_col_typed->getData();
                    const size_t rows = vec_src.size();

                    for (size_t i = 0; i < rows; ++i)
                    {
                        Int64 scale64 = scale[i];
                        validateScale(scale64);
                        Scale raw_scale = scale64;

                        DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>::applyOne(value_col_typed->getElement(i), value_col_typed->getScale(),
                            reinterpret_cast<typename ColumnDecimal<T>::NativeT&>(col_res->getElement(i)), raw_scale);
                    }
                }
            }

            return col_res;
        }
        // Const value argument:
        const auto * value_col_typed_const = checkAndGetColumnConst<ColumnDecimal<T>>(value_col);
        if (value_col_typed_const)
        {
            auto value_col_full = value_col_typed_const->convertToFullColumn();
            return apply<ScaleType>(value_col_full.get(), scale_col);
        }
        return nullptr;
    }
};

/// Functions that round the value of an input parameter of type (U)Int8/16/32/64, Float32/64 or Decimal32/64/128.
/// Accept an additional optional parameter of type (U)Int8/16/32/64 (0 by default).
template <typename Name, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
class FunctionRounding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRounding>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"x", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "A number to round"},
        };
        FunctionArgumentDescriptors optional_args{
            {"N", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, "The number of decimal places to round to"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & value_arg = arguments[0];

        ColumnPtr res;
        auto call_data = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::RightType;

            if (arguments.size() > 1)
            {
                const ColumnWithTypeAndName & scale_column = arguments[1];

                auto call_scale = [&](const auto & scaleTypes) -> bool
                {
                    using ScaleTypes = std::decay_t<decltype(scaleTypes)>;
                    using ScaleType = typename ScaleTypes::RightType;

                    res = Dispatcher<DataType, rounding_mode, tie_breaking_mode>::template apply<ScaleType>(value_arg.column.get(), scale_column.column.get());
                    return true;
                };

                TypeIndex right_index = scale_column.type->getTypeId();
                if (!callOnBasicType<void, true, false, false, false>(right_index, call_scale))
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Scale argument for rounding functions must have integer type");
                return true;
            }
            res = Dispatcher<DataType, rounding_mode, tie_breaking_mode>::template apply<int>(value_arg.column.get());
            return true;
        };

#if !defined(__SSE4_1__)
        /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
        /// Actually it is by default. But we will set it just in case.

        if constexpr (rounding_mode == RoundingMode::Round)
            if (0 != fesetround(FE_TONEAREST))
                throw Exception(ErrorCodes::CANNOT_SET_ROUNDING_MODE, "Cannot set floating point rounding mode");
#endif

        TypeIndex left_index = value_arg.type->getTypeId();
        if (!callOnBasicType<void, true, true, true, false>(left_index, call_data))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", value_arg.name, getName());

        return res;
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }
};


/// Rounds down to a number within explicitly specified array.
/// If the value is less than the minimal bound - returns the minimal bound.
class FunctionRoundDown : public IFunction
{
public:
    static constexpr auto name = "roundDown";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRoundDown>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type_x = arguments[0];

        if (!isNumber(type_x))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Unsupported type {} of first argument of function {}, must be numeric type.",
                            type_x->getName(), getName());

        const DataTypeArray * type_arr = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!type_arr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Second argument of function {}, must be array of boundaries to round to.", getName());

        const auto type_arr_nested = type_arr->getNestedType();

        if (!isNumber(type_arr_nested))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Elements of array of second argument of function {} must be numeric type.", getName());
        }
        return getLeastSupertype(DataTypes{type_x, type_arr_nested});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        auto in_column = arguments[0].column;
        const auto & in_type = arguments[0].type;

        auto array_column = arguments[1].column;
        const auto & array_type = arguments[1].type;

        const auto & return_type = result_type;
        auto column_result = return_type->createColumn();
        auto * out = column_result.get();

        if (!in_type->equals(*return_type))
            in_column = castColumn(arguments[0], return_type);

        if (!array_type->equals(*return_type))
            array_column = castColumn(arguments[1], std::make_shared<DataTypeArray>(return_type));

        const auto * in = in_column.get();
        auto boundaries = typeid_cast<const ColumnConst &>(*array_column).getValue<Array>();
        size_t num_boundaries = boundaries.size();
        if (!num_boundaries)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty array is illegal for boundaries in {} function", getName());

        if (!executeNum<UInt8>(in, out, boundaries)
            && !executeNum<UInt16>(in, out, boundaries)
            && !executeNum<UInt32>(in, out, boundaries)
            && !executeNum<UInt64>(in, out, boundaries)
            && !executeNum<Int8>(in, out, boundaries)
            && !executeNum<Int16>(in, out, boundaries)
            && !executeNum<Int32>(in, out, boundaries)
            && !executeNum<Int64>(in, out, boundaries)
            && !executeNum<Float32>(in, out, boundaries)
            && !executeNum<Float64>(in, out, boundaries)
            && !executeDecimal<Decimal32>(in, out, boundaries)
            && !executeDecimal<Decimal64>(in, out, boundaries)
            && !executeDecimal<Decimal128>(in, out, boundaries)
            && !executeDecimal<Decimal256>(in, out, boundaries))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", in->getName(), getName());
        }

        return column_result;
    }

private:
    template <typename T>
    bool executeNum(const IColumn * in_untyped, IColumn * out_untyped, const Array & boundaries) const
    {
        const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped);
        auto out = typeid_cast<ColumnVector<T> *>(out_untyped);
        if (!in || !out)
            return false;

        executeImplNumToNum(in->getData(), out->getData(), boundaries);
        return true;
    }

    template <typename T>
    bool executeDecimal(const IColumn * in_untyped, IColumn * out_untyped, const Array & boundaries) const
    {
        const auto in = checkAndGetColumn<ColumnDecimal<T>>(in_untyped);
        auto out = typeid_cast<ColumnDecimal<T> *>(out_untyped);
        if (!in || !out)
            return false;

        executeImplNumToNum(in->getData(), out->getData(), boundaries);
        return true;
    }

    template <typename Container>
    void NO_INLINE executeImplNumToNum(const Container & src, Container & dst, const Array & boundaries) const
    {
        using ValueType = typename Container::value_type;
        std::vector<ValueType> boundary_values(boundaries.size());
        for (size_t i = 0; i < boundaries.size(); ++i)
            boundary_values[i] = static_cast<ValueType>(boundaries[i].safeGet<ValueType>());

        ::sort(boundary_values.begin(), boundary_values.end());
        boundary_values.erase(std::unique(boundary_values.begin(), boundary_values.end()), boundary_values.end());

        size_t size = src.size();
        dst.resize(size);

        if (boundary_values.size() < 32)    /// Just a guess
        {
            /// Linear search with value on previous iteration as a hint.
            /// Not optimal if the size of list is large and distribution of values is uniform random.

            auto begin = boundary_values.begin();
            auto end = boundary_values.end();
            auto it = begin + (end - begin) / 2;

            for (size_t i = 0; i < size; ++i)
            {
                auto value = src[i];

                if (*it < value)
                {
                    while (it != end && *it <= value)
                        ++it;
                    if (it != begin)
                        --it;
                }
                else
                {
                    while (*it > value && it != begin)
                        --it;
                }

                dst[i] = *it;
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
            {
                auto it = std::upper_bound(boundary_values.begin(), boundary_values.end(), src[i]);
                if (it == boundary_values.end())
                {
                    dst[i] = boundary_values.back();
                }
                else if (it == boundary_values.begin())
                {
                    dst[i] = boundary_values.front();
                }
                else
                {
                    dst[i] = *(it - 1);
                }
            }
        }
    }
};


struct NameRound { static constexpr auto name = "round"; };
struct NameRoundBankers { static constexpr auto name = "roundBankers"; };
struct NameCeil { static constexpr auto name = "ceil"; };
struct NameFloor { static constexpr auto name = "floor"; };
struct NameTrunc { static constexpr auto name = "trunc"; };

using FunctionRound = FunctionRounding<NameRound, RoundingMode::Round, TieBreakingMode::Auto>;
using FunctionRoundBankers = FunctionRounding<NameRoundBankers, RoundingMode::Round, TieBreakingMode::Bankers>;
using FunctionFloor = FunctionRounding<NameFloor, RoundingMode::Floor, TieBreakingMode::Auto>;
using FunctionCeil = FunctionRounding<NameCeil, RoundingMode::Ceil, TieBreakingMode::Auto>;
using FunctionTrunc = FunctionRounding<NameTrunc, RoundingMode::Trunc, TieBreakingMode::Auto>;

}
