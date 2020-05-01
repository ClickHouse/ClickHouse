#pragma once

#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/getLeastSupertype.h>

#include <Interpreters/castColumn.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionHelpers.h>

#include <Core/AccurateComparison.h>
#include <Core/DecimalComparison.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <limits>
#include <type_traits>

#if USE_EMBEDDED_COMPILER
#include <DataTypes/Native.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h>
#pragma GCC diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


/** Comparison functions: ==, !=, <, >, <=, >=.
  * The comparison functions always return 0 or 1 (UInt8).
  *
  * You can compare the following types:
  * - numbers and decimals;
  * - strings and fixed strings;
  * - dates;
  * - datetimes;
  *   within each group, but not from different groups;
  * - tuples (lexicographic comparison).
  *
  * Exception: You can compare the date and datetime with a constant string. Example: EventDate = '2015-01-01'.
  */


template <typename A, typename B, typename Op>
struct NumComparisonImpl
{
    /// If you don't specify NO_INLINE, the compiler will inline this function, but we don't need this as this function contains tight loop inside.
    static void NO_INLINE vectorVector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        /** GCC 4.8.2 vectorizes a loop only if it is written in this form.
          * In this case, if you loop through the array index (the code will look simpler),
          *  the loop will not be vectorized.
          */

        size_t size = a.size();
        const A * __restrict a_pos = a.data();
        const B * __restrict b_pos = b.data();
        UInt8 * __restrict c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, *b_pos);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vectorConstant(const PaddedPODArray<A> & a, B b, PaddedPODArray<UInt8> & c)
    {
        size_t size = a.size();
        const A * __restrict a_pos = a.data();
        UInt8 * __restrict c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, b);
            ++a_pos;
            ++c_pos;
        }
    }

    static void constantVector(A a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        NumComparisonImpl<B, A, typename Op::SymmetricOp>::vectorConstant(b, a, c);
    }

    static void constantConstant(A a, B b, UInt8 & c)
    {
        c = Op::apply(a, b);
    }
};


template <typename Op>
struct StringComparisonImpl
{
    static void NO_INLINE string_vector_string_vector(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            c[i] = Op::apply(memcmpSmallAllowOverflow15(
                a_data.data() + prev_a_offset, a_offsets[i] - prev_a_offset - 1,
                b_data.data() + prev_b_offset, b_offsets[i] - prev_b_offset - 1), 0);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            c[i] = Op::apply(memcmpSmallLikeZeroPaddedAllowOverflow15(
                a_data.data() + prev_a_offset, a_offsets[i] - prev_a_offset - 1,
                b_data.data() + i * b_n, b_n), 0);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, ColumnString::Offset b_size,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            c[i] = Op::apply(memcmpSmallAllowOverflow15(
                a_data.data() + prev_a_offset, a_offsets[i] - prev_a_offset - 1,
                b_data.data(), b_size), 0);

            prev_a_offset = a_offsets[i];
        }
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[i]), 0);
    }

    static void NO_INLINE fixed_string_vector_constant_16(
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[0]), 0);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        if (a_n == 16 && b_n == 16)
        {
            /** Specialization if both sizes are 16.
              * To more efficient comparison of IPv6 addresses stored in FixedString(16).
              */
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        }
        else if (a_n == b_n)
        {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + i, b_data.data() + i, a_n), 0);
        }
        else
        {
            size_t size = a_data.size() / a_n;

            for (size_t i = 0; i < size; ++i)
                c[i] = Op::apply(memcmpSmallLikeZeroPaddedAllowOverflow15(a_data.data() + i * a_n, a_n, b_data.data() + i * b_n, b_n), 0);
        }
    }

    static void NO_INLINE fixed_string_vector_constant(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, ColumnString::Offset b_size,
        PaddedPODArray<UInt8> & c)
    {
        if (a_n == 16 && b_size == 16)
        {
            fixed_string_vector_constant_16(a_data, b_data, c);
        }
        else if (a_n == b_size)
        {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + i, b_data.data(), a_n), 0);
        }
        else
        {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(0, memcmpSmallLikeZeroPaddedAllowOverflow15(a_data.data() + i, a_n, b_data.data(), b_size));
        }
    }

    static void constant_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }

    static void constant_fixed_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::fixed_string_vector_constant(b_data, b_n, a_data, a_size, c);
    }
};


/// Comparisons for equality/inequality are implemented slightly more efficient.
template <bool positive>
struct StringEqualsImpl
{
    static void NO_INLINE string_vector_string_vector(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            auto a_size = a_offsets[i] - prev_a_offset - 1;
            auto b_size = b_offsets[i] - prev_b_offset - 1;

            c[i] = positive == memequalSmallAllowOverflow15(
                a_data.data() + prev_a_offset, a_size,
                b_data.data() + prev_b_offset, b_size);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            auto a_size = a_offsets[i] - prev_a_offset - 1;

            c[i] = positive == memequalSmallLikeZeroPaddedAllowOverflow15(
                a_data.data() + prev_a_offset, a_size,
                b_data.data() + b_n * i, b_n);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, ColumnString::Offset b_size,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            auto a_size = a_offsets[i] - prev_a_offset - 1;

            c[i] = positive == memequalSmallAllowOverflow15(
                a_data.data() + prev_a_offset, a_size,
                b_data.data(), b_size);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size() / 16;

        for (size_t i = 0; i < size; ++i)
            c[i] = positive == memequal16(
                a_data.data() + i * 16,
                b_data.data() + i * 16);
    }

    static void NO_INLINE fixed_string_vector_constant_16(
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size() / 16;

        for (size_t i = 0; i < size; ++i)
            c[i] = positive == memequal16(
                a_data.data() + i * 16,
                b_data.data());
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        /** Specialization if both sizes are 16.
          * To more efficient comparison of IPv6 addresses stored in FixedString(16).
          */
        if (a_n == 16 && b_n == 16)
        {
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        }
        else if (a_n == b_n)
        {
            size_t size = a_data.size() / a_n;
            for (size_t i = 0; i < size; ++i)
                c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + i * a_n, a_n, b_data.data() + i * a_n, a_n);
        }
        else
        {
            size_t size = a_data.size() / a_n;
            for (size_t i = 0; i < size; ++i)
                c[i] = positive == memequalSmallLikeZeroPaddedAllowOverflow15(a_data.data() + i * a_n, a_n, b_data.data() + i * b_n, b_n);
        }
    }

    static void NO_INLINE fixed_string_vector_constant(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, ColumnString::Offset b_size,
        PaddedPODArray<UInt8> & c)
    {
        if (a_n == 16 && b_size == 16)
        {
            fixed_string_vector_constant_16(a_data, b_data, c);
        }
        else
        {
            size_t size = a_data.size() / a_n;
            for (size_t i = 0; i < size; ++i)
                c[i] = positive == memequalSmallLikeZeroPaddedAllowOverflow15(a_data.data() + i * a_n, a_n, b_data.data(), b_size);
        }
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void constant_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }

    static void constant_fixed_string_vector(
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        fixed_string_vector_constant(b_data, b_n, a_data, a_size, c);
    }
};


template <typename A, typename B>
struct StringComparisonImpl<EqualsOp<A, B>> : StringEqualsImpl<true> {};

template <typename A, typename B>
struct StringComparisonImpl<NotEqualsOp<A, B>> : StringEqualsImpl<false> {};


/// Generic version, implemented for columns of same type.
template <typename Op>
struct GenericComparisonImpl
{
    static void NO_INLINE vectorVector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, i, b, 1), 0);
    }

    static void NO_INLINE vectorConstant(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        auto b_materialized = b.cloneResized(1)->convertToFullColumnIfConst();
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, 0, *b_materialized, 1), 0);
    }

    static void constantVector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        GenericComparisonImpl<typename Op::SymmetricOp>::vectorConstant(b, a, c);
    }

    static void constantConstant(const IColumn & a, const IColumn & b, UInt8 & c)
    {
        c = Op::apply(a.compareAt(0, 0, b, 1), 0);
    }
};


#if USE_EMBEDDED_COMPILER

template <template <typename, typename> typename Op> struct CompileOp;

template <> struct CompileOp<EqualsOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool /*is_signed*/)
    {
        return x->getType()->isIntegerTy() ? b.CreateICmpEQ(x, y) : b.CreateFCmpOEQ(x, y); /// qNaNs always compare false
    }
};

template <> struct CompileOp<NotEqualsOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool /*is_signed*/)
    {
        return x->getType()->isIntegerTy() ? b.CreateICmpNE(x, y) : b.CreateFCmpONE(x, y);
    }
};

template <> struct CompileOp<LessOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool is_signed)
    {
        return x->getType()->isIntegerTy() ? (is_signed ? b.CreateICmpSLT(x, y) : b.CreateICmpULT(x, y)) : b.CreateFCmpOLT(x, y);
    }
};

template <> struct CompileOp<GreaterOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool is_signed)
    {
        return x->getType()->isIntegerTy() ? (is_signed ? b.CreateICmpSGT(x, y) : b.CreateICmpUGT(x, y)) : b.CreateFCmpOGT(x, y);
    }
};

template <> struct CompileOp<LessOrEqualsOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool is_signed)
    {
        return x->getType()->isIntegerTy() ? (is_signed ? b.CreateICmpSLE(x, y) : b.CreateICmpULE(x, y)) : b.CreateFCmpOLE(x, y);
    }
};

template <> struct CompileOp<GreaterOrEqualsOp>
{
    static llvm::Value * compile(llvm::IRBuilder<> & b, llvm::Value * x, llvm::Value * y, bool is_signed)
    {
        return x->getType()->isIntegerTy() ? (is_signed ? b.CreateICmpSGE(x, y) : b.CreateICmpUGE(x, y)) : b.CreateFCmpOGE(x, y);
    }
};

#endif


struct NameEquals          { static constexpr auto name = "equals"; };
struct NameNotEquals       { static constexpr auto name = "notEquals"; };
struct NameLess            { static constexpr auto name = "less"; };
struct NameGreater         { static constexpr auto name = "greater"; };
struct NameLessOrEquals    { static constexpr auto name = "lessOrEquals"; };
struct NameGreaterOrEquals { static constexpr auto name = "greaterOrEquals"; };


template <
    template <typename, typename> class Op,
    typename Name>
class FunctionComparison : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionComparison>(context); }

    FunctionComparison(const Context & context_)
    :   context(context_),
        check_decimal_overflow(decimalCheckComparisonOverflow(context))
    {}

private:
    const Context & context;
    bool check_decimal_overflow = true;

    template <typename T0, typename T1>
    bool executeNumRightType(Block & block, size_t result, const ColumnVector<T0> * col_left, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorVector(col_left->getData(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorConstant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    template <typename T0, typename T1>
    bool executeNumConstRightType(Block & block, size_t result, const ColumnConst * col_left, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantVector(col_left->template getValue<T0>(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantConstant(col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), res);

            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(col_left->size(), toField(res));
            return true;
        }

        return false;
    }

    template <typename T0>
    bool executeNumLeftType(Block & block, size_t result, const IColumn * col_left_untyped, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T0> * col_left = checkAndGetColumn<ColumnVector<T0>>(col_left_untyped))
        {
            if (   executeNumRightType<T0, UInt8>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt16>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt64>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, UInt128>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int8>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int16>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int64>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Int128>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Float32>(block, result, col_left, col_right_untyped)
                || executeNumRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (auto col_left_const = checkAndGetColumnConst<ColumnVector<T0>>(col_left_untyped))
        {
            if (   executeNumConstRightType<T0, UInt8>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, UInt16>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, UInt32>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, UInt64>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, UInt128>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Int8>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Int16>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Int32>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Int64>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Int128>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Float32>(block, result, col_left_const, col_right_untyped)
                || executeNumConstRightType<T0, Float64>(block, result, col_left_const, col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
    }

    void executeDecimal(Block & block, size_t result, const ColumnWithTypeAndName & col_left, const ColumnWithTypeAndName & col_right)
    {
        TypeIndex left_number = col_left.type->getTypeId();
        TypeIndex right_number = col_right.type->getTypeId();

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if (check_decimal_overflow)
                DecimalComparison<LeftDataType, RightDataType, Op, true>(block, result, col_left, col_right);
            else
                DecimalComparison<LeftDataType, RightDataType, Op, false>(block, result, col_left, col_right);
            return true;
        };

        if (!callOnBasicTypes<true, false, true, true>(left_number, right_number, call))
            throw Exception("Wrong call for " + getName() + " with " + col_left.type->getName() + " and " + col_right.type->getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    bool executeString(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0);
        const ColumnFixedString * c1_fixed_string = checkAndGetColumn<ColumnFixedString>(c1);

        const ColumnConst * c0_const = checkAndGetColumnConstStringOrFixedString(c0);
        const ColumnConst * c1_const = checkAndGetColumnConstStringOrFixedString(c1);

        if (!((c0_string || c0_fixed_string || c0_const) && (c1_string || c1_fixed_string || c1_const)))
            return false;

        const ColumnString::Chars * c0_const_chars = nullptr;
        const ColumnString::Chars * c1_const_chars = nullptr;
        ColumnString::Offset c0_const_size = 0;
        ColumnString::Offset c1_const_size = 0;

        if (c0_const)
        {
            const ColumnString * c0_const_string = checkAndGetColumn<ColumnString>(&c0_const->getDataColumn());
            const ColumnFixedString * c0_const_fixed_string = checkAndGetColumn<ColumnFixedString>(&c0_const->getDataColumn());

            if (c0_const_string)
            {
                c0_const_chars = &c0_const_string->getChars();
                c0_const_size = c0_const_string->getDataAt(0).size;
            }
            else if (c0_const_fixed_string)
            {
                c0_const_chars = &c0_const_fixed_string->getChars();
                c0_const_size = c0_const_fixed_string->getN();
            }
            else
                throw Exception("Logical error: ColumnConst contains not String nor FixedString column", ErrorCodes::ILLEGAL_COLUMN);
        }

        if (c1_const)
        {
            const ColumnString * c1_const_string = checkAndGetColumn<ColumnString>(&c1_const->getDataColumn());
            const ColumnFixedString * c1_const_fixed_string = checkAndGetColumn<ColumnFixedString>(&c1_const->getDataColumn());

            if (c1_const_string)
            {
                c1_const_chars = &c1_const_string->getChars();
                c1_const_size = c1_const_string->getDataAt(0).size;
            }
            else if (c1_const_fixed_string)
            {
                c1_const_chars = &c1_const_fixed_string->getChars();
                c1_const_size = c1_const_fixed_string->getN();
            }
            else
                throw Exception("Logical error: ColumnConst contains not String nor FixedString column", ErrorCodes::ILLEGAL_COLUMN);
        }

        using StringImpl = StringComparisonImpl<Op<int, int>>;

        if (c0_const && c1_const)
        {
            auto res = executeString(block, result, &c0_const->getDataColumn(), &c1_const->getDataColumn());
            if (!res)
                return false;

            block.getByPosition(result).column = ColumnConst::create(block.getByPosition(result).column, c0_const->size());
            return true;
        }
        else
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_string && c1_string)
                StringImpl::string_vector_string_vector(
                    c0_string->getChars(), c0_string->getOffsets(),
                    c1_string->getChars(), c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_string && c1_fixed_string)
                StringImpl::string_vector_fixed_string_vector(
                    c0_string->getChars(), c0_string->getOffsets(),
                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
                    c_res->getData());
            else if (c0_string && c1_const)
                StringImpl::string_vector_constant(
                    c0_string->getChars(), c0_string->getOffsets(),
                    *c1_const_chars, c1_const_size,
                    c_res->getData());
            else if (c0_fixed_string && c1_string)
                StringImpl::fixed_string_vector_string_vector(
                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
                    c1_string->getChars(), c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_fixed_string && c1_fixed_string)
                StringImpl::fixed_string_vector_fixed_string_vector(
                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
                    c_res->getData());
            else if (c0_fixed_string && c1_const)
                StringImpl::fixed_string_vector_constant(
                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
                    *c1_const_chars, c1_const_size,
                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constant_string_vector(
                    *c0_const_chars, c0_const_size,
                    c1_string->getChars(), c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_const && c1_fixed_string)
                StringImpl::constant_fixed_string_vector(
                    *c0_const_chars, c0_const_size,
                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
                    c_res->getData());
            else
                throw Exception("Illegal columns "
                    + c0->getName() + " and " + c1->getName()
                    + " of arguments of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);

            block.getByPosition(result).column = std::move(c_res);
            return true;
        }
    }

    bool executeDateOrDateTimeOrEnumOrUUIDWithConstString(
        Block & block, size_t result, const IColumn * col_left_untyped, const IColumn * col_right_untyped,
        const DataTypePtr & left_type, const DataTypePtr & right_type, bool left_is_num, size_t input_rows_count)
    {
        /// This is no longer very special case - comparing dates, datetimes, and enumerations with a string constant.
        const IColumn * column_string_untyped = !left_is_num ? col_left_untyped : col_right_untyped;
        const IColumn * column_number = left_is_num ? col_left_untyped : col_right_untyped;
        const IDataType * number_type = left_is_num ? left_type.get() : right_type.get();

        WhichDataType which(number_type);

        const bool legal_types = which.isDateOrDateTime() || which.isEnum() || which.isUUID();

        const auto column_string = checkAndGetColumnConst<ColumnString>(column_string_untyped);
        if (!column_string || !legal_types)
            return false;

        StringRef string_value = column_string->getDataAt(0);

        if (which.isDate())
        {
            DayNum date;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readDateText(date, in);
            if (!in.eof())
                throw Exception("String is too long for Date: " + string_value.toString(), ErrorCodes::TOO_LARGE_STRING_SIZE);

            ColumnPtr parsed_const_date_holder = DataTypeDate().createColumnConst(input_rows_count, date);
            const ColumnConst * parsed_const_date = assert_cast<const ColumnConst *>(parsed_const_date_holder.get());
            executeNumLeftType<DataTypeDate::FieldType>(block, result,
                left_is_num ? col_left_untyped : parsed_const_date,
                left_is_num ? parsed_const_date : col_right_untyped);
        }
        else if (which.isDateTime())
        {
            time_t date_time;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readDateTimeText(date_time, in, dynamic_cast<const DataTypeDateTime &>(*number_type).getTimeZone());
            if (!in.eof())
                throw Exception("String is too long for DateTime: " + string_value.toString(), ErrorCodes::TOO_LARGE_STRING_SIZE);

            ColumnPtr parsed_const_date_time_holder = DataTypeDateTime().createColumnConst(input_rows_count, UInt64(date_time));
            const ColumnConst * parsed_const_date_time = assert_cast<const ColumnConst *>(parsed_const_date_time_holder.get());
            executeNumLeftType<DataTypeDateTime::FieldType>(block, result,
                left_is_num ? col_left_untyped : parsed_const_date_time,
                left_is_num ? parsed_const_date_time : col_right_untyped);
        }
        else if (which.isUUID())
        {
            UUID uuid;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readText(uuid, in);
            if (!in.eof())
                throw Exception("String is too long for UUID: " + string_value.toString(), ErrorCodes::TOO_LARGE_STRING_SIZE);

            ColumnPtr parsed_const_uuid_holder = DataTypeUUID().createColumnConst(input_rows_count, uuid);
            const ColumnConst * parsed_const_uuid = assert_cast<const ColumnConst *>(parsed_const_uuid_holder.get());
            executeNumLeftType<DataTypeUUID::FieldType>(block, result,
                left_is_num ? col_left_untyped : parsed_const_uuid,
                left_is_num ? parsed_const_uuid : col_right_untyped);
        }

        else if (which.isEnum8())
            executeEnumWithConstString<DataTypeEnum8>(block, result, column_number, column_string,
                number_type, left_is_num, input_rows_count);
        else if (which.isEnum16())
            executeEnumWithConstString<DataTypeEnum16>(block, result, column_number, column_string,
                number_type, left_is_num, input_rows_count);

        return true;
    }

    /// Comparison between DataTypeEnum<T> and string constant containing the name of an enum element
    template <typename EnumType>
    void executeEnumWithConstString(
        Block & block, const size_t result, const IColumn * column_number, const ColumnConst * column_string,
        const IDataType * type_untyped, const bool left_is_num, size_t input_rows_count)
    {
        const auto type = static_cast<const EnumType *>(type_untyped);

        const Field x = castToNearestFieldType(type->getValue(column_string->getValue<String>()));
        const auto enum_col = type->createColumnConst(input_rows_count, x);

        executeNumLeftType<typename EnumType::FieldType>(block, result,
            left_is_num ? column_number : enum_col.get(),
            left_is_num ? enum_col.get() : column_number);
    }

    void executeTuple(Block & block, size_t result, const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1,
                          size_t input_rows_count)
    {
        /** We will lexicographically compare the tuples. This is done as follows:
          * x == y : x1 == y1 && x2 == y2 ...
          * x != y : x1 != y1 || x2 != y2 ...
          *
          * x < y:   x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn < yn))
          * x > y:   x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn > yn))
          * x <= y:  x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn <= yn))
          *
          * Recursive form:
          * x <= y:  x1 < y1 || (x1 == y1 && x_tail <= y_tail)
          *
          * x >= y:  x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn >= yn))
          */

        const size_t tuple_size = typeid_cast<const DataTypeTuple &>(*c0.type).getElements().size();

        if (0 == tuple_size)
            throw Exception("Comparison of zero-sized tuples is not implemented.", ErrorCodes::NOT_IMPLEMENTED);

        ColumnsWithTypeAndName x(tuple_size);
        ColumnsWithTypeAndName y(tuple_size);

        auto x_const = checkAndGetColumnConst<ColumnTuple>(c0.column.get());
        auto y_const = checkAndGetColumnConst<ColumnTuple>(c1.column.get());

        Columns x_columns;
        Columns y_columns;

        if (x_const)
            x_columns = convertConstTupleToConstantElements(*x_const);
        else
            x_columns = assert_cast<const ColumnTuple &>(*c0.column).getColumnsCopy();

        if (y_const)
            y_columns = convertConstTupleToConstantElements(*y_const);
        else
            y_columns = assert_cast<const ColumnTuple &>(*c1.column).getColumnsCopy();

        for (size_t i = 0; i < tuple_size; ++i)
        {
            x[i].type = static_cast<const DataTypeTuple &>(*c0.type).getElements()[i];
            y[i].type = static_cast<const DataTypeTuple &>(*c1.type).getElements()[i];

            x[i].column = x_columns[i];
            y[i].column = y_columns[i];
        }

        executeTupleImpl(block, result, x, y, tuple_size, input_rows_count);
    }

    void executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                              const ColumnsWithTypeAndName & y, size_t tuple_size,
                              size_t input_rows_count);

    void executeTupleEqualityImpl(
        std::shared_ptr<IFunctionOverloadResolver> func_compare,
        std::shared_ptr<IFunctionOverloadResolver> func_convolution,
        Block & block,
        size_t result,
        const ColumnsWithTypeAndName & x,
        const ColumnsWithTypeAndName & y,
        size_t tuple_size,
        size_t input_rows_count)
    {
        if (0 == tuple_size)
            throw Exception("Comparison of zero-sized tuples is not implemented.", ErrorCodes::NOT_IMPLEMENTED);

        Block tmp_block;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            auto impl = func_compare->build({x[i], y[i]});

            /// Comparison of the elements.
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            impl->execute(tmp_block, {i * 3, i * 3 + 1}, i * 3 + 2, input_rows_count);
        }

        if (tuple_size == 1)
        {
            /// Do not call AND for single-element tuple.
            block.getByPosition(result).column = tmp_block.getByPosition(2).column;
            return;
        }

        /// Logical convolution.
        tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

        ColumnNumbers convolution_args(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            convolution_args[i] = i * 3 + 2;

        ColumnsWithTypeAndName convolution_types(convolution_args.size(), { nullptr, std::make_shared<DataTypeUInt8>(), "" });
        auto impl = func_convolution->build(convolution_types);

        impl->execute(tmp_block, convolution_args, tuple_size * 3, input_rows_count);
        block.getByPosition(result).column = tmp_block.getByPosition(tuple_size * 3).column;
    }

    void executeTupleLessGreaterImpl(
        std::shared_ptr<IFunctionOverloadResolver> func_compare_head,
        std::shared_ptr<IFunctionOverloadResolver> func_compare_tail,
        std::shared_ptr<IFunctionOverloadResolver> func_and,
        std::shared_ptr<IFunctionOverloadResolver> func_or,
        std::shared_ptr<IFunctionOverloadResolver> func_equals,
        Block & block,
        size_t result,
        const ColumnsWithTypeAndName & x,
        const ColumnsWithTypeAndName & y,
        size_t tuple_size,
        size_t input_rows_count)
    {
        ColumnsWithTypeAndName bin_args = {{ nullptr, std::make_shared<DataTypeUInt8>(), "" },
                                           { nullptr, std::make_shared<DataTypeUInt8>(), "" }};

        auto func_and_adaptor = func_and->build(bin_args);
        auto func_or_adaptor = func_or->build(bin_args);

        Block tmp_block;

        /// Pairwise comparison of the inequality of all elements; on the equality of all elements except the last.
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

            if (i + 1 != tuple_size)
            {
                auto impl_head = func_compare_head->build({x[i], y[i]});
                impl_head->execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2, input_rows_count);

                tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

                auto impl_equals = func_equals->build({x[i], y[i]});
                impl_equals->execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 3, input_rows_count);

            }
            else
            {
                auto impl_tail = func_compare_tail->build({x[i], y[i]});
                impl_tail->execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2, input_rows_count);
            }
        }

        /// Combination. Complex code - make a drawing. It can be replaced by a recursive comparison of tuples.
        size_t i = tuple_size - 1;
        while (i > 0)
        {
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_and_adaptor->execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 3}, tmp_block.columns() - 1, input_rows_count);
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_or_adaptor->execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 2}, tmp_block.columns() - 1, input_rows_count);
            --i;
        }

        block.getByPosition(result).column = tmp_block.getByPosition(tmp_block.columns() - 1).column;
    }

    void executeGenericIdenticalTypes(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        bool c0_const = isColumnConst(*c0);
        bool c1_const = isColumnConst(*c1);

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constantConstant(*c0, *c1, res);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        }
        else
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constantVector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vectorConstant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vectorVector(*c0, *c1, vec_res);

            block.getByPosition(result).column = std::move(c_res);
        }
    }

    void executeGeneric(Block & block, size_t result, const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1)
    {
        DataTypePtr common_type = getLeastSupertype({c0.type, c1.type});

        ColumnPtr c0_converted = castColumn(c0, common_type);
        ColumnPtr c1_converted = castColumn(c1, common_type);

        executeGenericIdenticalTypes(block, result, c0_converted.get(), c1_converted.get());
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType left(arguments[0].get());
        WhichDataType right(arguments[1].get());

        const DataTypeTuple * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        const DataTypeTuple * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].get());

        bool both_represented_by_number = arguments[0]->isValueRepresentedByNumber() && arguments[1]->isValueRepresentedByNumber();
        bool has_date = left.isDate() || right.isDate();

        if (!((both_represented_by_number && !has_date)   /// Do not allow compare date and number.
            || (left.isStringOrFixedString() && right.isStringOrFixedString())
            /// You can compare the date, datetime, or datatime64 and an enumeration with a constant string.
            || (left.isString() && right.isDateOrDateTime())
            || (left.isDateOrDateTime() && right.isString())
            || (left.isDateOrDateTime() && right.isDateOrDateTime() && left.idx == right.idx) /// only date vs date, or datetime vs datetime
            || (left.isUUID() && right.isUUID())
            || (left.isUUID() && right.isString())
            || (left.isString() && right.isUUID())
            || (left.isEnum() && right.isEnum() && arguments[0]->getName() == arguments[1]->getName()) /// only equivalent enum type values can be compared against
            || (left.isEnum() && right.isString())
            || (left.isString() && right.isEnum())
            || (left_tuple && right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
            || (arguments[0]->equals(*arguments[1]))))
        {
            try
            {
                getLeastSupertype(arguments);
            }
            catch (const Exception &)
            {
                throw Exception("Illegal types of arguments (" + arguments[0]->getName() + ", " + arguments[1]->getName() + ")"
                    " of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        if (left_tuple && right_tuple)
        {
            auto adaptor = FunctionOverloadResolverAdaptor(std::make_unique<DefaultOverloadResolver>(FunctionComparison<Op, Name>::create(context)));

            size_t size = left_tuple->getElements().size();
            for (size_t i = 0; i < size; ++i)
            {
                ColumnsWithTypeAndName args = {{nullptr, left_tuple->getElements()[i], ""},
                                               {nullptr, right_tuple->getElements()[i], ""}};
                adaptor.build(args);
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto & col_with_type_and_name_left = block.getByPosition(arguments[0]);
        const auto & col_with_type_and_name_right = block.getByPosition(arguments[1]);
        const IColumn * col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn * col_right_untyped = col_with_type_and_name_right.column.get();

        const DataTypePtr & left_type = col_with_type_and_name_left.type;
        const DataTypePtr & right_type = col_with_type_and_name_right.type;

        /// The case when arguments are the same (tautological comparison). Return constant.
        /// NOTE: Nullable types are special case. (BTW, this function use default implementation for Nullable, so Nullable types cannot be here. Check just in case.)
        /// NOTE: We consider NaN comparison to be implementation specific (and in our implementation NaNs are sometimes equal sometimes not).
        if (left_type->equals(*right_type) && !left_type->isNullable() && col_left_untyped == col_right_untyped)
        {
            /// Always true: =, <=, >=
            if constexpr (std::is_same_v<Op<int, int>, EqualsOp<int, int>>
                || std::is_same_v<Op<int, int>, LessOrEqualsOp<int, int>>
                || std::is_same_v<Op<int, int>, GreaterOrEqualsOp<int, int>>)
            {
                block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, 1u);
                return;
            }
            else
            {
                block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, 0u);
                return;
            }
        }

        WhichDataType which_left{left_type};
        WhichDataType which_right{right_type};

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        bool date_and_datetime = (left_type != right_type) &&
            which_left.isDateOrDateTime() && which_right.isDateOrDateTime();

        if (left_is_num && right_is_num && !date_and_datetime)
        {
            if (!(executeNumLeftType<UInt8>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<UInt16>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<UInt32>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<UInt64>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<UInt128>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Int8>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Int16>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Int32>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Int64>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Int128>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Float32>(block, result, col_left_untyped, col_right_untyped)
                || executeNumLeftType<Float64>(block, result, col_left_untyped, col_right_untyped)))
                throw Exception("Illegal column " + col_left_untyped->getName()
                    + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (checkAndGetDataType<DataTypeTuple>(left_type.get()))
        {
            executeTuple(block, result, col_with_type_and_name_left, col_with_type_and_name_right, input_rows_count);
        }
        else if (which_left.idx != which_right.idx
                 && (which_left.isDateTime64() || which_right.isDateTime64())
                 && (which_left.isStringOrFixedString() || which_right.isStringOrFixedString()))
        {
            /** Special case of comparing DateTime64 against a string.
             *
             * Can't be moved to executeDateOrDateTimeOrEnumOrUUIDWithConstString()
             * since DateTime64 is basically a Decimal, but we do similar things, except type inference.
             * Outline:
             * - Extract string content
             * - Parse it as a ColumnDateTime64 value (same type as DateTime64, means same precision)
             * - Fabricate a column with type and name
             * - Compare left and right comlumns as DateTime64 columns.
             */

            const size_t datetime64_col_index = which_left.isDateTime64() ? 0 : 1;
            const size_t string_col_index = which_left.isStringOrFixedString() ? 0 : 1;

            const auto & datetime64_col_with_type_and_name = block.getByPosition(arguments[datetime64_col_index]);
            const auto & string_col_with_type_and_name = block.getByPosition(arguments[string_col_index]);

            if (!isColumnConst(*string_col_with_type_and_name.column))
                throw Exception(getName() + ", illegal column type of argument #" + std::to_string(string_col_index)
                        + " '" + string_col_with_type_and_name.name + "'"
                        " expected const String or const FixedString,"
                        " got " + string_col_with_type_and_name.type->getName(),
                        ErrorCodes::ILLEGAL_COLUMN);

            if (datetime64_col_with_type_and_name.column->size() == 0 || string_col_with_type_and_name.column->size() == 0)
            {
                // For some reason, when both left and right columns are empty (dry run while building a header block)
                // executeDecimal() fills result column with bogus value.
                block.getByPosition(result).column = ColumnUInt8::create();
                return;
            }

            auto parsed_tmp_column_holder = datetime64_col_with_type_and_name.type->createColumn();

            {
                const StringRef string_value = string_col_with_type_and_name.column->getDataAt(0);
                ReadBufferFromMemory in(string_value.data, string_value.size);
                datetime64_col_with_type_and_name.type->deserializeAsWholeText(*parsed_tmp_column_holder, in, FormatSettings{});

                if (!in.eof())
                    throw Exception(getName() + ": String is too long for " + datetime64_col_with_type_and_name.type->getName() + " : " + string_value.toString(), ErrorCodes::TOO_LARGE_STRING_SIZE);
            }

            // It is necessary to wrap tmp column in ColumnConst to avoid overflow when comparing.
            // (non-const columns are expected to have same number of rows as every other column in block).
            const ColumnWithTypeAndName parsed_tmp_col_with_type_and_name{
                    ColumnConst::create(std::move(parsed_tmp_column_holder), 1),
                    datetime64_col_with_type_and_name.type,
                    string_col_with_type_and_name.name};

            executeDecimal(block, result,
                which_left.isDateTime64() ? datetime64_col_with_type_and_name : parsed_tmp_col_with_type_and_name,
                which_right.isDateTime64() ? datetime64_col_with_type_and_name : parsed_tmp_col_with_type_and_name);

        }
        else if (isColumnedAsDecimal(left_type) || isColumnedAsDecimal(right_type))
        {
            // compare
            if (!allowDecimalComparison(left_type, right_type) && !date_and_datetime)
                throw Exception("No operation " + getName() + " between " + left_type->getName() + " and " + right_type->getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            executeDecimal(block, result, col_with_type_and_name_left, col_with_type_and_name_right);
        }
        else if (!left_is_num && !right_is_num && executeString(block, result, col_left_untyped, col_right_untyped))
        {
        }
        else if (left_type->equals(*right_type))
        {
            executeGenericIdenticalTypes(block, result, col_left_untyped, col_right_untyped);
        }
        else if (executeDateOrDateTimeOrEnumOrUUIDWithConstString(
                block, result, col_left_untyped, col_right_untyped,
                left_type, right_type,
                left_is_num, input_rows_count))
        {
        }
        else
        {
            executeGeneric(block, result, col_with_type_and_name_left, col_with_type_and_name_right);
        }
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes & types) const override
    {
        auto isBigInteger = &typeIsEither<DataTypeInt64, DataTypeUInt64, DataTypeUUID>;
        auto isFloatingPoint = &typeIsEither<DataTypeFloat32, DataTypeFloat64>;
        if ((isBigInteger(*types[0]) && isFloatingPoint(*types[1]))
            || (isBigInteger(*types[1]) && isFloatingPoint(*types[0]))
            || (WhichDataType(types[0]).isDate() && WhichDataType(types[1]).isDateTime())
            || (WhichDataType(types[1]).isDate() && WhichDataType(types[0]).isDateTime()))
            return false; /// TODO: implement (double, int_N where N > double's mantissa width)
        return isCompilableType(types[0]) && isCompilableType(types[1]);
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        auto * x = values[0]();
        auto * y = values[1]();
        if (!types[0]->equals(*types[1]))
        {
            llvm::Type * common;
            if (x->getType()->isIntegerTy() && y->getType()->isIntegerTy())
                common = b.getIntNTy(std::max(
                    /// if one integer has a sign bit, make sure the other does as well. llvm generates optimal code
                    /// (e.g. uses overflow flag on x86) for (word size + 1)-bit integer operations.
                    x->getType()->getIntegerBitWidth() + (!typeIsSigned(*types[0]) && typeIsSigned(*types[1])),
                    y->getType()->getIntegerBitWidth() + (!typeIsSigned(*types[1]) && typeIsSigned(*types[0]))));
            else
                /// (double, float) or (double, int_N where N <= double's mantissa width) -> double
                common = b.getDoubleTy();
            x = nativeCast(b, types[0], x, common);
            y = nativeCast(b, types[1], y, common);
        }
        auto * result = CompileOp<Op>::compile(b, x, y, typeIsSigned(*types[0]) || typeIsSigned(*types[1]));
        return b.CreateSelect(result, b.getInt8(1), b.getInt8(0));
    }
#endif
};

}
