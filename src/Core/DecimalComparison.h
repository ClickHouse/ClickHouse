#pragma once

#include <common/arithmeticOverflow.h>
#include <Core/Block.h>
#include <Core/AccurateComparison.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionHelpers.h>  /// TODO Core should not depend on Functions


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DECIMAL_OVERFLOW;
}

///
inline bool allowDecimalComparison(const DataTypePtr & left_type, const DataTypePtr & right_type)
{
    if (isColumnedAsDecimal(left_type))
    {
        if (isColumnedAsDecimal(right_type) || isNotDecimalButComparableToDecimal(right_type))
            return true;
    }
    else if (isNotDecimalButComparableToDecimal(left_type) && isColumnedAsDecimal(right_type))
        return true;
    return false;
}

template <size_t > struct ConstructDecInt { using Type = Int32; };
template <> struct ConstructDecInt<8> { using Type = Int64; };
template <> struct ConstructDecInt<16> { using Type = Int128; };
template <> struct ConstructDecInt<48> { using Type = Int256; };

template <typename T, typename U>
struct DecCompareInt
{
    using Type = typename ConstructDecInt<(!IsDecimalNumber<U> || sizeof(T) > sizeof(U)) ? sizeof(T) : sizeof(U)>::Type;
    using TypeA = Type;
    using TypeB = Type;
};

///
template <typename A, typename B, template <typename, typename> typename Operation, bool _check_overflow = true,
    bool _actual = IsDecimalNumber<A> || IsDecimalNumber<B>>
class DecimalComparison
{
public:
    using CompareInt = typename DecCompareInt<A, B>::Type;
    using Op = Operation<CompareInt, CompareInt>;
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimalNumber<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;

    DecimalComparison(ColumnsWithTypeAndName & data, size_t result, const ColumnWithTypeAndName & col_left, const ColumnWithTypeAndName & col_right)
    {
        if (!apply(data, result, col_left, col_right))
            throw Exception("Wrong decimal comparison with " + col_left.type->getName() + " and " + col_right.type->getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    static bool apply(ColumnsWithTypeAndName & data, size_t result [[maybe_unused]],
                      const ColumnWithTypeAndName & col_left, const ColumnWithTypeAndName & col_right)
    {
        if constexpr (_actual)
        {
            ColumnPtr c_res;
            Shift shift = getScales<A, B>(col_left.type, col_right.type);

            c_res = applyWithScale(col_left.column, col_right.column, shift);
            if (c_res)
                data[result].column = std::move(c_res);
            return true;
        }
        return false;
    }

    static bool compare(A a, B b, UInt32 scale_a, UInt32 scale_b)
    {
        static const UInt32 max_scale = DecimalUtils::maxPrecision<Decimal256>();
        if (scale_a > max_scale || scale_b > max_scale)
            throw Exception("Bad scale of decimal field", ErrorCodes::DECIMAL_OVERFLOW);

        Shift shift;
        if (scale_a < scale_b)
            shift.a = static_cast<CompareInt>(DecimalUtils::scaleMultiplier<B>(scale_b - scale_a));
        if (scale_a > scale_b)
            shift.b = static_cast<CompareInt>(DecimalUtils::scaleMultiplier<A>(scale_a - scale_b));

        return applyWithScale(a, b, shift);
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

    template <typename T, typename U>
    static auto applyWithScale(T a, U b, const Shift & shift)
    {
        if (shift.left())
            return apply<true, false>(a, b, shift.a);
        else if (shift.right())
            return apply<false, true>(a, b, shift.b);
        return apply<false, false>(a, b, 1);
    }

    template <typename T, typename U>
    static std::enable_if_t<IsDecimalNumber<T> && IsDecimalNumber<U>, Shift>
    getScales(const DataTypePtr & left_type, const DataTypePtr & right_type)
    {
        const DataTypeDecimal<T> * decimal0 = checkDecimal<T>(*left_type);
        const DataTypeDecimal<U> * decimal1 = checkDecimal<U>(*right_type);

        Shift shift;
        if (decimal0 && decimal1)
        {
            auto result_type = decimalResultType<false, false>(*decimal0, *decimal1);
            shift.a = static_cast<CompareInt>(result_type.scaleFactorFor(*decimal0, false).value);
            shift.b = static_cast<CompareInt>(result_type.scaleFactorFor(*decimal1, false).value);
        }
        else if (decimal0)
            shift.b = static_cast<CompareInt>(decimal0->getScaleMultiplier().value);
        else if (decimal1)
            shift.a = static_cast<CompareInt>(decimal1->getScaleMultiplier().value);

        return shift;
    }

    template <typename T, typename U>
    static std::enable_if_t<IsDecimalNumber<T> && !IsDecimalNumber<U>, Shift>
    getScales(const DataTypePtr & left_type, const DataTypePtr &)
    {
        Shift shift;
        const DataTypeDecimal<T> * decimal0 = checkDecimal<T>(*left_type);
        if (decimal0)
            shift.b = static_cast<CompareInt>(decimal0->getScaleMultiplier().value);
        return shift;
    }

    template <typename T, typename U>
    static std::enable_if_t<!IsDecimalNumber<T> && IsDecimalNumber<U>, Shift>
    getScales(const DataTypePtr &, const DataTypePtr & right_type)
    {
        Shift shift;
        const DataTypeDecimal<U> * decimal1 = checkDecimal<U>(*right_type);
        if (decimal1)
            shift.a = static_cast<CompareInt>(decimal1->getScaleMultiplier().value);
        return shift;
    }

    template <bool scale_left, bool scale_right>
    static ColumnPtr apply(const ColumnPtr & c0, const ColumnPtr & c1, CompareInt scale)
    {
        auto c_res = ColumnUInt8::create();

        if constexpr (_actual)
        {
            bool c0_is_const = isColumnConst(*c0);
            bool c1_is_const = isColumnConst(*c1);

            if (c0_is_const && c1_is_const)
            {
                const ColumnConst * c0_const = checkAndGetColumnConst<ColVecA>(c0.get());
                const ColumnConst * c1_const = checkAndGetColumnConst<ColVecB>(c1.get());

                A a = c0_const->template getValue<A>();
                B b = c1_const->template getValue<B>();
                UInt8 res = apply<scale_left, scale_right>(a, b, scale);
                return DataTypeUInt8().createColumnConst(c0->size(), toField(res));
            }

            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_is_const)
            {
                const ColumnConst * c0_const = checkAndGetColumnConst<ColVecA>(c0.get());
                A a = c0_const->template getValue<A>();
                if (const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get()))
                    constantVector<scale_left, scale_right>(a, c1_vec->getData(), vec_res, scale);
                else
                    throw Exception("Wrong column in Decimal comparison", ErrorCodes::LOGICAL_ERROR);
            }
            else if (c1_is_const)
            {
                const ColumnConst * c1_const = checkAndGetColumnConst<ColVecB>(c1.get());
                B b = c1_const->template getValue<B>();
                if (const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get()))
                    vectorConstant<scale_left, scale_right>(c0_vec->getData(), b, vec_res, scale);
                else
                    throw Exception("Wrong column in Decimal comparison", ErrorCodes::LOGICAL_ERROR);
            }
            else
            {
                if (const ColVecA * c0_vec = checkAndGetColumn<ColVecA>(c0.get()))
                {
                    if (const ColVecB * c1_vec = checkAndGetColumn<ColVecB>(c1.get()))
                        vectorVector<scale_left, scale_right>(c0_vec->getData(), c1_vec->getData(), vec_res, scale);
                    else
                        throw Exception("Wrong column in Decimal comparison", ErrorCodes::LOGICAL_ERROR);
                }
                else
                    throw Exception("Wrong column in Decimal comparison", ErrorCodes::LOGICAL_ERROR);
            }
        }

        return c_res;
    }

    template <bool scale_left, bool scale_right>
    static NO_INLINE UInt8 apply(A a, B b, CompareInt scale [[maybe_unused]])
    {
        CompareInt x;
        if constexpr (IsDecimalNumber<A>)
            x = a.value;
        else
            x = a;

        CompareInt y;
        if constexpr (IsDecimalNumber<B>)
            y = b.value;
        else
            y = b;

        if constexpr (_check_overflow)
        {
            bool overflow = false;

            if constexpr (sizeof(A) > sizeof(CompareInt))
                overflow |= (bigint_cast<A>(x) != a);
            if constexpr (sizeof(B) > sizeof(CompareInt))
                overflow |= (bigint_cast<B>(y) != b);
            if constexpr (is_unsigned_v<A>)
                overflow |= (x < 0);
            if constexpr (is_unsigned_v<B>)
                overflow |= (y < 0);

            if constexpr (scale_left)
                overflow |= common::mulOverflow(x, scale, x);
            if constexpr (scale_right)
                overflow |= common::mulOverflow(y, scale, y);

            if (overflow)
                throw Exception("Can't compare", ErrorCodes::DECIMAL_OVERFLOW);
        }
        else
        {
            if constexpr (scale_left)
                x *= scale;
            if constexpr (scale_right)
                y *= scale;
        }

        return Op::apply(x, y);
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE vectorVector(const ArrayA & a, const ArrayB & b, PaddedPODArray<UInt8> & c,
                                        CompareInt scale)
    {
        size_t size = a.size();
        const A * a_pos = a.data();
        const B * b_pos = b.data();
        UInt8 * c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = apply<scale_left, scale_right>(*a_pos, *b_pos, scale);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE vectorConstant(const ArrayA & a, B b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
        size_t size = a.size();
        const A * a_pos = a.data();
        UInt8 * c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = apply<scale_left, scale_right>(*a_pos, b, scale);
            ++a_pos;
            ++c_pos;
        }
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE constantVector(A a, const ArrayB & b, PaddedPODArray<UInt8> & c, CompareInt scale)
    {
        size_t size = b.size();
        const B * b_pos = b.data();
        UInt8 * c_pos = c.data();
        const B * b_end = b_pos + size;

        while (b_pos < b_end)
        {
            *c_pos = apply<scale_left, scale_right>(a, *b_pos, scale);
            ++b_pos;
            ++c_pos;
        }
    }
};

}
