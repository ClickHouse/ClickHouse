#pragma once

#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Interpreters/castColumn.h>
#include "IFunction.h"
#include <Common/intExp.h>
#include <cmath>
#include <type_traits>
#include <array>
#include <ext/bit_cast.h>
#include <algorithm>

#ifdef __SSE4_1__
    #include <smmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


/** Rounding Functions:
    * round(x, N) - rounding to nearest (N = 0 by default). Use banker's rounding for floating point numbers.
    * floor(x, N) is the largest number <= x (N = 0 by default).
    * ceil(x, N) is the smallest number >= x (N = 0 by default).
    * trunc(x, N) - is the largest by absolute value number that is not greater than x by absolute value (N = 0 by default).
    *
    * The value of the parameter N (scale):
    * - N > 0: round to the number with N decimal places after the decimal point
    * - N < 0: round to an integer with N zero characters
    * - N = 0: round to an integer
    *
    * Type of the result is the type of argument.
    * For integer arguments, when passing negative scale, overflow can occur.
    * In that case, the behavior is implementation specific.
    */


/** This parameter controls the behavior of the rounding functions.
  */
enum class ScaleMode
{
    Positive,   // round to a number with N decimal places after the decimal point
    Negative,   // round to an integer with N zero characters
    Zero,       // round to an integer
};

enum class RoundingMode
{
#ifdef __SSE4_1__
    Round   = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor   = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil    = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc   = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round   = 8,    /// Values are correspond to above just in case.
    Floor   = 9,
    Ceil    = 10,
    Trunc   = 11,
#endif
};


/** Rounding functions for integer values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingComputation
{
    static const size_t data_count = 1;

    static size_t prepare(size_t scale)
    {
        return scale;
    }

    static ALWAYS_INLINE T computeImpl(T x, T scale)
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
                bool negative = x < 0;
                if (negative)
                    x = -x;
                x = (x + scale / 2) / scale * scale;
                if (negative)
                    x = -x;
                return x;
            }
        }

        __builtin_unreachable();
    }

    static ALWAYS_INLINE T compute(T x, T scale)
    {
        switch (scale_mode)
        {
            case ScaleMode::Zero:
                return x;
            case ScaleMode::Positive:
                return x;
            case ScaleMode::Negative:
                return computeImpl(x, scale);
        }

        __builtin_unreachable();
    }

    static ALWAYS_INLINE void compute(const T * __restrict in, size_t scale, T * __restrict out)
    {
        if (sizeof(T) <= sizeof(scale) && scale > size_t(std::numeric_limits<T>::max()))
            *out = 0;
        else
            *out = compute(*in, scale);
    }

};


#ifdef __SSE4_1__

template <typename T>
class BaseFloatRoundingComputation;

template <>
class BaseFloatRoundingComputation<Float32>
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
class BaseFloatRoundingComputation<Float64>
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

#else

/// Implementation for ARM. Not vectorized.

inline float roundWithMode(float x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return roundf(x);
        case RoundingMode::Floor: return floorf(x);
        case RoundingMode::Ceil: return ceilf(x);
        case RoundingMode::Trunc: return truncf(x);
    }

    __builtin_unreachable();
}

inline double roundWithMode(double x, RoundingMode mode)
{
    switch (mode)
    {
        case RoundingMode::Round: return round(x);
        case RoundingMode::Floor: return floor(x);
        case RoundingMode::Ceil: return ceil(x);
        case RoundingMode::Trunc: return trunc(x);
    }

    __builtin_unreachable();
}

template <typename T>
class BaseFloatRoundingComputation
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

#endif


/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation : public BaseFloatRoundingComputation<T>
{
    using Base = BaseFloatRoundingComputation<T>;

public:
    static inline void compute(const T * __restrict in, const typename Base::VectorType & scale, T * __restrict out)
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
    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode>;
    using Data = std::array<T, Op::data_count>;

public:
    static NO_INLINE void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container & out)
    {
        auto mm_scale = Op::prepare(scale);

        const size_t data_count = std::tuple_size<Data>();

        const T* end_in = in.data() + in.size();
        const T* limit = in.data() + in.size() / data_count * data_count;

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

        while (p_in < limit)
        {
            Op::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp_src{{}};
            Data tmp_dst;

            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);

            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op::compute(reinterpret_cast<T *>(&tmp_src), mm_scale, reinterpret_cast<T *>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingImpl
{
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode>;

public:
    template <size_t scale>
    static NO_INLINE void applyImpl(const PaddedPODArray<T> & in, typename ColumnVector<T>::Container & out)
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

    static NO_INLINE void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container & out)
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
                throw Exception("Logical error: unexpected 'scale' parameter passed to function IntegerRoundingComputation::compute",
                    ErrorCodes::LOGICAL_ERROR);
        }
    }
};


template <typename T, RoundingMode rounding_mode>
class DecimalRounding
{
    using NativeType = typename T::NativeType;
    using Op = IntegerRoundingComputation<NativeType, rounding_mode, ScaleMode::Negative>;
    using Container = typename ColumnDecimal<T>::Container;

public:
    static NO_INLINE void apply(const Container & in, Container & out, Int64 scale_arg)
    {
        scale_arg = in.getScale() - scale_arg;
        if (scale_arg > 0)
        {
            size_t scale = intExp10(scale_arg);

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
            memcpy(out.data(), in.data(), in.size() * sizeof(T));
    }
};


/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, RoundingMode rounding_mode>
class Dispatcher
{
    template <ScaleMode scale_mode>
    using FunctionRoundingImpl = std::conditional_t<std::is_floating_point_v<T>,
        FloatRoundingImpl<T, rounding_mode, scale_mode>,
        IntegerRoundingImpl<T, rounding_mode, scale_mode>>;

    static void apply(Block & block, const ColumnVector<T> * col, Int64 scale_arg, size_t result)
    {
        auto col_res = ColumnVector<T>::create();

        typename ColumnVector<T>::Container & vec_res = col_res->getData();
        vec_res.resize(col->getData().size());

        if (!vec_res.empty())
        {
            if (scale_arg == 0)
            {
                size_t scale = 1;
                FunctionRoundingImpl<ScaleMode::Zero>::apply(col->getData(), scale, vec_res);
            }
            else if (scale_arg > 0)
            {
                size_t scale = intExp10(scale_arg);
                FunctionRoundingImpl<ScaleMode::Positive>::apply(col->getData(), scale, vec_res);
            }
            else
            {
                size_t scale = intExp10(-scale_arg);
                FunctionRoundingImpl<ScaleMode::Negative>::apply(col->getData(), scale, vec_res);
            }
        }

        block.getByPosition(result).column = std::move(col_res);
    }

    static void apply(Block & block, const ColumnDecimal<T> * col, Int64 scale_arg, size_t result)
    {
        const typename ColumnDecimal<T>::Container & vec_src = col->getData();

        auto col_res = ColumnDecimal<T>::create(vec_src.size(), vec_src.getScale());
        auto & vec_res = col_res->getData();

        if (!vec_res.empty())
            DecimalRounding<T, rounding_mode>::apply(col->getData(), vec_res, scale_arg);

        block.getByPosition(result).column = std::move(col_res);
    }

public:
    static void apply(Block & block, const IColumn * column, Int64 scale_arg, size_t result)
    {
        if constexpr (IsNumber<T>)
            apply(block, checkAndGetColumn<ColumnVector<T>>(column), scale_arg, result);
        else if constexpr (IsDecimalNumber<T>)
            apply(block, checkAndGetColumn<ColumnDecimal<T>>(column), scale_arg, result);
    }
};

/** A template for functions that round the value of an input parameter of type
  * (U)Int8/16/32/64, Float32/64 or Decimal32/64/128, and accept an additional optional parameter (default is 0).
  */
template <typename Name, RoundingMode rounding_mode>
class FunctionRounding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRounding>(); }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto & type : arguments)
            if (!isNumber(type) && !isDecimal(type))
                throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    static Int64 getScaleArg(Block & block, const ColumnNumbers & arguments)
    {
        if (arguments.size() == 2)
        {
            const IColumn & scale_column = *block.getByPosition(arguments[1]).column;
            if (!scale_column.isColumnConst())
                throw Exception("Scale argument for rounding functions must be constant.", ErrorCodes::ILLEGAL_COLUMN);

            Field scale_field = static_cast<const ColumnConst &>(scale_column).getField();
            if (scale_field.getType() != Field::Types::UInt64
                && scale_field.getType() != Field::Types::Int64)
                throw Exception("Scale argument for rounding functions must have integer type.", ErrorCodes::ILLEGAL_COLUMN);

            return scale_field.get<Int64>();
        }
        return 0;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnWithTypeAndName & column = block.getByPosition(arguments[0]);
        Int64 scale_arg = getScaleArg(block, arguments);

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>)
            {
                using FieldType = typename DataType::FieldType;
                Dispatcher<FieldType, rounding_mode>::apply(block, column.column.get(), scale_arg, result);
                return true;
            }
            return false;
        };

        if (!callOnIndexAndDataType<void>(column.type->getTypeId(), call))
        {
            throw Exception("Illegal column " + column.name + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { true, true, true };
    }
};


/** Rounds down to a number within explicitly specified array.
  * If the value is less than the minimal bound - returns the minimal bound.
  */
class FunctionRoundDown : public IFunction
{
public:
    static constexpr auto name = "roundDown";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRoundDown>(context); }
    FunctionRoundDown(const Context & context) : context(context) {}

public:
    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type_x = arguments[0];

        if (!(isNumber(type_x) || isDecimal(type_x)))
            throw Exception{"Unsupported type " + type_x->getName()
                            + " of first argument of function " + getName()
                            + ", must be numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const DataTypeArray * type_arr = checkAndGetDataType<DataTypeArray>(arguments[1].get());

        if (!type_arr)
            throw Exception{"Second argument of function " + getName()
                            + ", must be array of boundaries to round to.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        const auto type_arr_nested = type_arr->getNestedType();

        if (!(isNumber(type_arr_nested) || isDecimal(type_arr_nested)))
        {
            throw Exception{"Elements of array of second argument of function " + getName()
                            + " must be numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        return getLeastSupertype({type_x, type_arr_nested});
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        auto in_column = block.getByPosition(arguments[0]).column;
        const auto & in_type = block.getByPosition(arguments[0]).type;

        auto array_column = block.getByPosition(arguments[1]).column;
        const auto & array_type = block.getByPosition(arguments[1]).type;

        const auto & return_type = block.getByPosition(result).type;
        auto column_result = return_type->createColumn();
        auto out = column_result.get();

        if (!in_type->equals(*return_type))
            in_column = castColumn(block.getByPosition(arguments[0]), return_type, context);

        if (!array_type->equals(*return_type))
            array_column = castColumn(block.getByPosition(arguments[1]), std::make_shared<DataTypeArray>(return_type), context);

        const auto in = in_column.get();
        auto boundaries = typeid_cast<const ColumnConst &>(*array_column).getValue<Array>();
        size_t num_boundaries = boundaries.size();
        if (!num_boundaries)
            throw Exception("Empty array is illegal for boundaries in " + getName() + " function", ErrorCodes::BAD_ARGUMENTS);

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
            && !executeDecimal<Decimal128>(in, out, boundaries))
        {
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }

        block.getByPosition(result).column = std::move(column_result);
    }

private:
    template <typename T>
    bool executeNum(const IColumn * in_untyped, IColumn * out_untyped, const Array & boundaries)
    {
        const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped);
        auto out = typeid_cast<ColumnVector<T> *>(out_untyped);
        if (!in || !out)
            return false;

        executeImplNumToNum(in->getData(), out->getData(), boundaries);
        return true;
    }

    template <typename T>
    bool executeDecimal(const IColumn * in_untyped, IColumn * out_untyped, const Array & boundaries)
    {
        const auto in = checkAndGetColumn<ColumnDecimal<T>>(in_untyped);
        auto out = typeid_cast<ColumnDecimal<T> *>(out_untyped);
        if (!in || !out)
            return false;

        executeImplNumToNum(in->getData(), out->getData(), boundaries);
        return true;
    }

    template <typename Container>
    void executeImplNumToNum(const Container & src, Container & dst, const Array & boundaries)
    {
        using ValueType = typename Container::value_type;
        std::vector<ValueType> boundary_values(boundaries.size());
        for (size_t i = 0; i < boundaries.size(); ++i)
            boundary_values[i] = boundaries[i].get<ValueType>();

        std::sort(boundary_values.begin(), boundary_values.end());
        boundary_values.erase(std::unique(boundary_values.begin(), boundary_values.end()), boundary_values.end());

        size_t size = src.size();
        dst.resize(size);
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

private:
    const Context & context;
};


struct NameRound { static constexpr auto name = "round"; };
struct NameCeil { static constexpr auto name = "ceil"; };
struct NameFloor { static constexpr auto name = "floor"; };
struct NameTrunc { static constexpr auto name = "trunc"; };

using FunctionRound = FunctionRounding<NameRound, RoundingMode::Round>;
using FunctionFloor = FunctionRounding<NameFloor, RoundingMode::Floor>;
using FunctionCeil = FunctionRounding<NameCeil, RoundingMode::Ceil>;
using FunctionTrunc = FunctionRounding<NameTrunc, RoundingMode::Trunc>;

}
