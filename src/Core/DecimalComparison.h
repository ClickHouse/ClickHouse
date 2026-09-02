#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/TargetSpecific.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DECIMAL_OVERFLOW;
}


inline bool allowDecimalComparison(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    if (isColumnedAsDecimal(left_type))
    {
        if (isColumnedAsDecimal(right_type) || isNotDecimalButComparableToDecimal(right_type))
            return true;
    }
    else if (isNotDecimalButComparableToDecimal(left_type) && isColumnedAsDecimal(right_type))
    {
        return true;
    }
    return false;
}

template <size_t> struct ConstructDecInt;
template <> struct ConstructDecInt<1> { using Type = Int32; };
template <> struct ConstructDecInt<2> { using Type = Int32; };
template <> struct ConstructDecInt<4> { using Type = Int32; };
template <> struct ConstructDecInt<8> { using Type = Int64; };
template <> struct ConstructDecInt<16> { using Type = Int128; };
template <> struct ConstructDecInt<32> { using Type = Int256; };

template <typename T, typename U>
struct DecCompareInt
{
    using Type = typename ConstructDecInt<(!is_decimal<U> || sizeof(T) > sizeof(U)) ? sizeof(T) : sizeof(U)>::Type;
    using TypeA = Type;
    using TypeB = Type;
};

/// Helpers for comparison of a mixed-type pair (e.g. `Decimal32` vs `Int64`).
/// Instead of instantiating a fused convert-scale-compare kernel for every (A, B) combination
/// (which is quadratic in the number of types and dominates the code size of comparison
/// functions), each side is first brought to the common `CompareInt` representation by a small
/// conversion kernel, and then a single per-`CompareInt` kernel does the comparison.
/// These are free templates, so instantiations are shared between all type pairs.
namespace DecimalComparisonHelper
{

[[noreturn]] inline void throwWrongColumn()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column in Decimal comparison");
}

[[noreturn]] inline void throwOverflow()
{
    throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Can't compare decimal number due to overflow");
}

inline ColumnPtr NO_INLINE createConstResult(size_t size, UInt8 value)
{
    return DataTypeUInt8().createColumnConst(size, Field(value));
}

/// Performs exactly the same conversion, overflow checks and scale multiplication
/// as the former fused kernel did for each element.
template <typename To, bool check_overflow, bool apply_scale, typename From>
ALWAYS_INLINE To convertOneForCompare(const From & from, To scale [[maybe_unused]])
{
    To x{};
    if constexpr (is_decimal<From>)
        x = static_cast<To>(from.value);
    else
        x = static_cast<To>(from);

    if constexpr (check_overflow)
    {
        bool overflow = false;

        if constexpr (sizeof(From) > sizeof(To))
            overflow |= (static_cast<From>(x) != from);
        if constexpr (is_unsigned_v<From>)
            overflow |= (x < 0);

        if constexpr (apply_scale)
            overflow |= common::mulOverflow(x, scale, x);

        if (overflow)
            throwOverflow();
    }
    else
    {
        if constexpr (apply_scale)
            x = common::mulIgnoreOverflow(x, scale);
    }

    return x;
}

template <typename To, bool check_overflow, typename From>
To NO_INLINE convertForCompare(const From & from, To scale, bool apply_scale)
{
    if (apply_scale)
        return convertOneForCompare<To, check_overflow, true>(from, scale);
    return convertOneForCompare<To, check_overflow, false>(from, scale);
}

/// Returns a pointer to `size` values of type `To` representing the input data:
/// either a view over the original data (when the representation is bit-identical
/// and no scaling or checking is needed) or the `tmp` array filled by conversion.
template <typename To, bool check_overflow, typename From>
const To * NO_INLINE prepareForCompare(const PaddedPODArray<From> & data, PaddedPODArray<To> & tmp, To scale, bool apply_scale)
{
    /// A view is possible for a decimal over the same native type (`Decimal<To>` is a standard
    /// layout class with the value as its only field) and for builtin integers of the same size
    /// (a signed/unsigned pun is allowed by the aliasing rules; `wide::integer` is not).
    constexpr bool same_layout = std::is_same_v<From, To>
        || (is_decimal<From> && std::is_same_v<NativeType<From>, To>)
        || (is_integer<From> && !is_big_int_v<From> && sizeof(From) == sizeof(To));

    constexpr bool needs_check = check_overflow && (sizeof(From) > sizeof(To) || is_unsigned_v<From>);

    if constexpr (same_layout && !needs_check)
    {
        if (!apply_scale)
            return reinterpret_cast<const To *>(data.data());
    }

    size_t size = data.size();
    const From * from = data.data();
    tmp.resize(size);
    To * to = tmp.data();

    if (apply_scale)
    {
        for (size_t i = 0; i < size; ++i)
            to[i] = convertOneForCompare<To, check_overflow, true>(from[i], scale);
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
            to[i] = convertOneForCompare<To, check_overflow, false>(from[i], scale);
    }

    return to;
}

template <typename T, typename Op>
void NO_INLINE compareVectorVector(const T * a, const T * b, UInt8 * c, size_t size)
{
    for (size_t i = 0; i < size; ++i)
        c[i] = Op::apply(a[i], b[i]);
}

template <typename T, typename Op>
void NO_INLINE compareVectorConstant(const T * a, T b, UInt8 * c, size_t size)
{
    for (size_t i = 0; i < size; ++i)
        c[i] = Op::apply(a[i], b);
}

template <typename T, typename Op>
void NO_INLINE compareConstantVector(T a, const T * b, UInt8 * c, size_t size)
{
    for (size_t i = 0; i < size; ++i)
        c[i] = Op::apply(a, b[i]);
}

}

template <typename A, typename B, template <typename, typename> typename Operation>
requires is_decimal<A> || is_decimal<B>
class DecimalComparison
{
public:
    using CompareInt = typename DecCompareInt<A, B>::Type;
    using Op = Operation<CompareInt, CompareInt>;
    using ColVecA = ColumnVectorOrDecimal<A>;
    using ColVecB = ColumnVectorOrDecimal<B>;

    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;

    static ColumnPtr apply(const ColumnWithTypeAndName & col_left, const ColumnWithTypeAndName & col_right, bool check_overflow)
    {
        Shift shift = getScales<A, B>(col_left.type, col_right.type);

        if constexpr (std::is_same_v<A, B>)
        {
            /// Same-type pairs are the hot case (e.g. `DateTime64` vs `DateTime64`):
            /// keep the fused convert-and-compare kernels.
            if (check_overflow)
                return applyWithScale<true>(col_left.column, col_right.column, shift);
            return applyWithScale<false>(col_left.column, col_right.column, shift);
        }
        else
        {
            /// Mixed-type pairs: bring both sides to the `CompareInt` representation first
            /// (with exactly the conversions, overflow checks and scale multiplication the fused
            /// kernel would do), then compare same-type. This avoids instantiating a heavy fused
            /// kernel for every (A, B) combination - the helpers are shared between type pairs.
            if (check_overflow)
                return applyMixed<true>(col_left.column, col_right.column, shift);
            return applyMixed<false>(col_left.column, col_right.column, shift);
        }
    }

    static bool compare(A a, B b, UInt32 scale_a, UInt32 scale_b, bool check_overflow)
    {
        static const UInt32 max_scale = DecimalUtils::max_precision<Decimal256>;
        if (scale_a > max_scale || scale_b > max_scale)
            throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Bad scale of decimal field");

        Shift shift;
        if (scale_a < scale_b)
            shift.a = static_cast<CompareInt>(DecimalUtils::scaleMultiplier<B>(scale_b - scale_a));
        if (scale_a > scale_b)
            shift.b = static_cast<CompareInt>(DecimalUtils::scaleMultiplier<A>(scale_a - scale_b));

        if (check_overflow)
            return applyWithScale<true>(a, b, shift);
        return applyWithScale<false>(a, b, shift);
    }

private:
    struct Shift
    {
        CompareInt a = 1;
        CompareInt b = 1;

        bool none() const { return a == 1 && b == 1; }
        bool left() const { return a != 1; }
        bool right() const { return b != 1; }
    };

    template <bool check_overflow, typename T, typename U>
    static auto applyWithScale(T a, U b, const Shift & shift)
    {
        if (shift.left())
            return apply<check_overflow, true, false>(a, b, shift.a);
        if (shift.right())
            return apply<check_overflow, false, true>(a, b, shift.b);
        return apply<check_overflow, false, false>(a, b, 1);
    }

    template <typename T, typename U>
    requires is_decimal<T> && is_decimal<U>
    static Shift getScales(const DataTypePtr & left_type, const DataTypePtr & right_type)
    {
        const DataTypeDecimalBase<T> * decimal0 = checkDecimalBase<T>(*left_type);
        const DataTypeDecimalBase<U> * decimal1 = checkDecimalBase<U>(*right_type);

        Shift shift;
        if (decimal0 && decimal1)
        {
            auto result_type = DecimalUtils::binaryOpResult<false, false>(*decimal0, *decimal1);
            shift.a = static_cast<CompareInt>(result_type.scaleFactorFor(DecimalUtils::DataTypeDecimalTrait<T>{decimal0->getPrecision(), decimal0->getScale()}, false).value);
            shift.b = static_cast<CompareInt>(result_type.scaleFactorFor(DecimalUtils::DataTypeDecimalTrait<U>{decimal1->getPrecision(), decimal1->getScale()}, false).value);
        }
        else if (decimal0)
            shift.b = static_cast<CompareInt>(decimal0->getScaleMultiplier().value);
        else if (decimal1)
            shift.a = static_cast<CompareInt>(decimal1->getScaleMultiplier().value);

        return shift;
    }

    template <typename T, typename U>
    requires is_decimal<T> && (!is_decimal<U>)
    static Shift getScales(const DataTypePtr & left_type, const DataTypePtr &)
    {
        Shift shift;
        const DataTypeDecimalBase<T> * decimal0 = checkDecimalBase<T>(*left_type);
        if (decimal0)
            shift.b = static_cast<CompareInt>(decimal0->getScaleMultiplier().value);
        return shift;
    }

    template <typename T, typename U>
    requires (!is_decimal<T>) && is_decimal<U>
    static Shift getScales(const DataTypePtr &, const DataTypePtr & right_type)
    {
        Shift shift;
        const DataTypeDecimalBase<U> * decimal1 = checkDecimalBase<U>(*right_type);
        if (decimal1)
            shift.a = static_cast<CompareInt>(decimal1->getScaleMultiplier().value);
        return shift;
    }

    template <bool check_overflow, bool scale_left, bool scale_right>
    static ColumnPtr apply(const ColumnPtr & c0, const ColumnPtr & c1, CompareInt scale)
    {
        auto c_res = ColumnUInt8::create();

        bool c0_is_const = isColumnConst(*c0);
        bool c1_is_const = isColumnConst(*c1);

        if (c0_is_const && c1_is_const)
        {
            const ColumnConst & c0_const = checkAndGetColumnConst<ColVecA>(*c0);
            const ColumnConst & c1_const = checkAndGetColumnConst<ColVecB>(*c1);

            A a = c0_const.template getValue<A>();
            B b = c1_const.template getValue<B>();
            UInt8 res = apply<check_overflow, scale_left, scale_right>(a, b, scale);
            return DataTypeUInt8().createColumnConst(c0->size(), Field(res));
        }

        ColumnUInt8::Container & vec_res = c_res->getData();
        vec_res.resize(c0->size());

        if (c0_is_const)
        {
            const ColumnConst & c0_const = checkAndGetColumnConst<ColVecA>(*c0);
            A a = c0_const.template getValue<A>();
            if (const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get()))
                constantVector<check_overflow, scale_left, scale_right>(a, c1_vec->getData(), vec_res, scale);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column in Decimal comparison");
        }
        else if (c1_is_const)
        {
            const ColumnConst & c1_const = checkAndGetColumnConst<ColVecB>(*c1);
            B b = c1_const.template getValue<B>();
            if (const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get()))
                vectorConstant<check_overflow, scale_left, scale_right>(c0_vec->getData(), b, vec_res, scale);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column in Decimal comparison");
        }
        else
        {
            if (const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get()))
            {
                if (const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get()))
                    vectorVector<check_overflow, scale_left, scale_right>(c0_vec->getData(), c1_vec->getData(), vec_res, scale);
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column in Decimal comparison");
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong column in Decimal comparison");
        }

        return c_res;
    }

    /// Column-level comparison of a mixed-type pair. Converts each side to `CompareInt`
    /// with shared helpers and compares same-type; see `DecimalComparisonHelper`.
    template <bool check_overflow>
    static ColumnPtr applyMixed(const ColumnPtr & c0, const ColumnPtr & c1, const Shift & shift)
    {
        namespace Helper = DecimalComparisonHelper;

        /// As in `applyWithScale`: the left scale takes priority, at most one side is scaled.
        bool scale_a = shift.left();
        bool scale_b = !scale_a && shift.right();

        bool c0_is_const = isColumnConst(*c0);
        bool c1_is_const = isColumnConst(*c1);

        if (c0_is_const && c1_is_const)
        {
            const ColumnConst & c0_const = checkAndGetColumnConst<ColVecA>(*c0);
            const ColumnConst & c1_const = checkAndGetColumnConst<ColVecB>(*c1);

            CompareInt x = Helper::convertForCompare<CompareInt, check_overflow>(c0_const.template getValue<A>(), shift.a, scale_a);
            CompareInt y = Helper::convertForCompare<CompareInt, check_overflow>(c1_const.template getValue<B>(), shift.b, scale_b);

            return Helper::createConstResult(c0->size(), Op::apply(x, y));
        }

        auto c_res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = c_res->getData();
        size_t size = c0->size();
        vec_res.resize(size);

        /// Do not convert constants before checking the size: the former fused kernels
        /// did not perform any overflow checks for empty columns.
        if (size == 0)
            return c_res;

        UInt8 * res_data = vec_res.data();

        if (c0_is_const)
        {
            const ColumnConst & c0_const = checkAndGetColumnConst<ColVecA>(*c0);
            CompareInt x = Helper::convertForCompare<CompareInt, check_overflow>(c0_const.template getValue<A>(), shift.a, scale_a);

            const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get());
            if (!c1_vec)
                Helper::throwWrongColumn();

            PaddedPODArray<CompareInt> tmp;
            const CompareInt * b_data = Helper::prepareForCompare<CompareInt, check_overflow>(c1_vec->getData(), tmp, shift.b, scale_b);
            Helper::compareConstantVector<CompareInt, Op>(x, b_data, res_data, size);
        }
        else if (c1_is_const)
        {
            const ColumnConst & c1_const = checkAndGetColumnConst<ColVecB>(*c1);
            CompareInt y = Helper::convertForCompare<CompareInt, check_overflow>(c1_const.template getValue<B>(), shift.b, scale_b);

            const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get());
            if (!c0_vec)
                Helper::throwWrongColumn();

            PaddedPODArray<CompareInt> tmp;
            const CompareInt * a_data = Helper::prepareForCompare<CompareInt, check_overflow>(c0_vec->getData(), tmp, shift.a, scale_a);
            Helper::compareVectorConstant<CompareInt, Op>(a_data, y, res_data, size);
        }
        else
        {
            const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get());
            const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get());
            if (!c0_vec || !c1_vec)
                Helper::throwWrongColumn();

            PaddedPODArray<CompareInt> tmp_a;
            PaddedPODArray<CompareInt> tmp_b;
            const CompareInt * a_data = Helper::prepareForCompare<CompareInt, check_overflow>(c0_vec->getData(), tmp_a, shift.a, scale_a);
            const CompareInt * b_data = Helper::prepareForCompare<CompareInt, check_overflow>(c1_vec->getData(), tmp_b, shift.b, scale_b);
            Helper::compareVectorVector<CompareInt, Op>(a_data, b_data, res_data, size);
        }

        return c_res;
    }

    template <bool check_overflow, bool scale_left, bool scale_right>
    static ALWAYS_INLINE UInt8 apply(A a, B b, CompareInt scale [[maybe_unused]])
    {
        CompareInt x;
        if constexpr (is_decimal<A>)
            x = a.value;
        else
            x = a;

        CompareInt y;
        if constexpr (is_decimal<B>)
            y = b.value;
        else
            y = static_cast<CompareInt>(b);

        if constexpr (check_overflow)
        {
            bool overflow = false;

            if constexpr (sizeof(A) > sizeof(CompareInt))
                overflow |= (static_cast<A>(x) != a);
            if constexpr (sizeof(B) > sizeof(CompareInt))
                overflow |= (static_cast<B>(y) != b);
            if constexpr (is_unsigned_v<A>)
                overflow |= (x < 0);
            if constexpr (is_unsigned_v<B>)
                overflow |= (y < 0);

            if constexpr (scale_left)
                overflow |= common::mulOverflow(x, scale, x);
            if constexpr (scale_right)
                overflow |= common::mulOverflow(y, scale, y);

            if (overflow)
                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Can't compare decimal number due to overflow");
        }
        else
        {
            if constexpr (scale_left)
                x = common::mulIgnoreOverflow(x, scale);
            if constexpr (scale_right)
                y = common::mulIgnoreOverflow(y, scale);
        }

        return Op::apply(x, y);
    }

    template <bool check_overflow, bool scale_left, bool scale_right>
    static void NO_INLINE vectorVector(const ArrayA & a, const ArrayB & b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
        size_t size = a.size();
        const A * a_pos = a.data();
        const B * b_pos = b.data();
        UInt8 * c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = apply<check_overflow, scale_left, scale_right>(*a_pos, *b_pos, scale);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(
    template <bool check_overflow, bool scale_left, bool scale_right> static void NO_INLINE
    ), vectorConstantImpl, MULTITARGET_FUNCTION_BODY(( /// NOLINT
        const ArrayA & a, B b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
        size_t size = a.size();
        const A * __restrict a_pos = a.data();
        UInt8 * __restrict c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = apply<check_overflow, scale_left, scale_right>(*a_pos, b, scale);
            ++a_pos;
            ++c_pos;
        }
    }))

    template <bool check_overflow, bool scale_left, bool scale_right>
    static void NO_INLINE vectorConstant(const ArrayA & a, B b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            vectorConstantImpl_x86_64_v4<check_overflow, scale_left, scale_right>(a, b, c, scale);
            return;
        }
#endif

        vectorConstantImpl<check_overflow, scale_left, scale_right>(a, b, c, scale);
    }

    template <bool check_overflow, bool scale_left, bool scale_right>
    static void NO_INLINE constantVector(A a, const ArrayB & b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
        size_t size = b.size();
        const B * b_pos = b.data();
        UInt8 * c_pos = c.data();
        const B * b_end = b_pos + size;

        while (b_pos < b_end)
        {
            *c_pos = apply<check_overflow, scale_left, scale_right>(a, *b_pos, scale);
            ++b_pos;
            ++c_pos;
        }
    }
};

}
