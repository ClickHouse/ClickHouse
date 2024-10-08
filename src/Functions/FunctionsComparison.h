#pragma once

#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Common/TargetSpecific.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/castColumn.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IsOperation.h>

#include <Core/AccurateComparison.h>
#include <Core/DecimalComparison.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <limits>
#include <type_traits>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
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
    using ContainerA = PaddedPODArray<A>;
    using ContainerB = PaddedPODArray<B>;

    MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(static void), vectorVectorImpl, MULTITARGET_FUNCTION_BODY(( /// NOLINT
        const ContainerA & a, const ContainerB & b, PaddedPODArray<UInt8> & c)
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
    }))

    static void NO_INLINE vectorVector(const ContainerA & a, const ContainerB & b, PaddedPODArray<UInt8> & c)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            vectorVectorImplAVX512BW(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::AVX512F))
        {
            vectorVectorImplAVX512F(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            vectorVectorImplAVX2(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::SSE42))
        {
            vectorVectorImplSSE42(a, b, c);
            return;
        }
#endif

        vectorVectorImpl(a, b, c);
    }


    MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
    MULTITARGET_FUNCTION_HEADER(static void), vectorConstantImpl, MULTITARGET_FUNCTION_BODY(( /// NOLINT
        const ContainerA & a, B b, PaddedPODArray<UInt8> & c)
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
    }))

    static void NO_INLINE vectorConstant(const ContainerA & a, B b, PaddedPODArray<UInt8> & c)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            vectorConstantImplAVX512BW(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::AVX512F))
        {
            vectorConstantImplAVX512F(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            vectorConstantImplAVX2(a, b, c);
            return;
        }

        if (isArchSupported(TargetArch::SSE42))
        {
            vectorConstantImplSSE42(a, b, c);
            return;
        }
#endif

        vectorConstantImpl(a, b, c);
    }

    static void constantVector(A a, const ContainerB & b, PaddedPODArray<UInt8> & c)
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
    static void NO_INLINE string_vector_string_vector( /// NOLINT
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

    static void NO_INLINE string_vector_fixed_string_vector( /// NOLINT
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

    static void NO_INLINE string_vector_constant( /// NOLINT
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

    static void fixed_string_vector_string_vector( /// NOLINT
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16( /// NOLINT
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[i]), 0);
    }

    static void NO_INLINE fixed_string_vector_constant_16( /// NOLINT
        const ColumnString::Chars & a_data,
        const ColumnString::Chars & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[0]), 0); /// NOLINT(readability-container-data-pointer)
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector( /// NOLINT
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

    static void NO_INLINE fixed_string_vector_constant( /// NOLINT
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
                c[j] = Op::apply(memcmpSmallLikeZeroPaddedAllowOverflow15(a_data.data() + i, a_n, b_data.data(), b_size), 0);
        }
    }

    static void constant_string_vector( /// NOLINT
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }

    static void constant_fixed_string_vector( /// NOLINT
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
    static void NO_INLINE string_vector_string_vector( /// NOLINT
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

    static void NO_INLINE string_vector_fixed_string_vector( /// NOLINT
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

    static void NO_INLINE string_vector_constant( /// NOLINT
        const ColumnString::Chars & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars & b_data, ColumnString::Offset b_size,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        if (b_size == 0)
        {
            /*
             * Add the fast path of string comparison if the string constant is empty
             * and b_size is 0. If a_size is also 0, both of string a and b are empty
             * string. There is no need to call memequalSmallAllowOverflow15() for
             * string comparison.
             */
            for (size_t i = 0; i < size; ++i)
            {
                auto a_size = a_offsets[i] - prev_a_offset - 1;

                if (a_size == 0)
                    c[i] = positive;
                else
                    c[i] = !positive;

                prev_a_offset = a_offsets[i];
            }
        }
        else
        {
            for (size_t i = 0; i < size; ++i)
            {
                auto a_size = a_offsets[i] - prev_a_offset - 1;

                c[i] = positive == memequalSmallAllowOverflow15(
                    a_data.data() + prev_a_offset, a_size,
                    b_data.data(), b_size);

                prev_a_offset = a_offsets[i];
            }
        }
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16( /// NOLINT
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

    static void NO_INLINE fixed_string_vector_constant_16( /// NOLINT
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

    static void NO_INLINE fixed_string_vector_fixed_string_vector( /// NOLINT
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

    static void NO_INLINE fixed_string_vector_constant( /// NOLINT
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

    static void fixed_string_vector_string_vector( /// NOLINT
        const ColumnString::Chars & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void constant_string_vector( /// NOLINT
        const ColumnString::Chars & a_data, ColumnString::Offset a_size,
        const ColumnString::Chars & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }

    static void constant_fixed_string_vector( /// NOLINT
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
        return x->getType()->isIntegerTy() ? b.CreateICmpNE(x, y) : b.CreateFCmpUNE(x, y);
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


template <template <typename, typename> class Op, typename Name>
class FunctionComparison : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionComparison>(decimalCheckComparisonOverflow(context)); }

    explicit FunctionComparison(bool check_decimal_overflow_)
        : check_decimal_overflow(check_decimal_overflow_) {}

private:
    bool check_decimal_overflow = true;

    template <typename T0, typename T1>
    ColumnPtr executeNumRightType(const ColumnVector<T0> * col_left, const IColumn * col_right_untyped) const
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorVector(col_left->getData(), col_right->getData(), vec_res);

            return col_res;
        }
        if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vectorConstant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res);

            return col_res;
        }

        return nullptr;
    }

    template <typename T0, typename T1>
    ColumnPtr executeNumConstRightType(const ColumnConst * col_left, const IColumn * col_right_untyped) const
    {
        if (const ColumnVector<T1> * col_right = checkAndGetColumn<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantVector(col_left->template getValue<T0>(), col_right->getData(), vec_res);

            return col_res;
        }
        if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constantConstant(
                col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), res);

            return DataTypeUInt8().createColumnConst(col_left->size(), toField(res));
        }

        return nullptr;
    }

    template <typename T0>
    ColumnPtr executeNumLeftType(const IColumn * col_left_untyped, const IColumn * col_right_untyped) const
    {
        ColumnPtr res = nullptr;
        if (const ColumnVector<T0> * col_left = checkAndGetColumn<ColumnVector<T0>>(col_left_untyped))
        {
            if (   (res = executeNumRightType<T0, UInt8>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, UInt16>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, UInt32>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, UInt64>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, UInt128>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, UInt256>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int8>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int16>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int32>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int64>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int128>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Int256>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Float32>(col_left, col_right_untyped))
                || (res = executeNumRightType<T0, Float64>(col_left, col_right_untyped)))
                return res;
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", col_right_untyped->getName(), getName());
        }
        if (auto col_left_const = checkAndGetColumnConst<ColumnVector<T0>>(col_left_untyped))
        {
            if ((res = executeNumConstRightType<T0, UInt8>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, UInt16>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, UInt32>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, UInt64>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, UInt128>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, UInt256>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int8>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int16>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int32>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int64>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int128>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Int256>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Float32>(col_left_const, col_right_untyped))
                || (res = executeNumConstRightType<T0, Float64>(col_left_const, col_right_untyped)))
                return res;
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", col_right_untyped->getName(), getName());
        }

        return nullptr;
    }

    ColumnPtr executeDecimal(const ColumnWithTypeAndName & col_left, const ColumnWithTypeAndName & col_right) const
    {
        TypeIndex left_number = col_left.type->getTypeId();
        TypeIndex right_number = col_right.type->getTypeId();
        ColumnPtr res;

        auto call = [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if (check_decimal_overflow)
                return (res = DecimalComparison<LeftDataType, RightDataType, Op, true>::apply(col_left, col_right)) != nullptr;
            return (res = DecimalComparison<LeftDataType, RightDataType, Op, false>::apply(col_left, col_right)) != nullptr;
        };

        if (!callOnBasicTypes<true, false, true, true>(left_number, right_number, call))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong call for {} with {} and {}",
                            getName(), col_left.type->getName(), col_right.type->getName());

        return res;
    }

    ColumnPtr executeString(const IColumn * c0, const IColumn * c1) const
    {
        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0);
        const ColumnFixedString * c1_fixed_string = checkAndGetColumn<ColumnFixedString>(c1);

        const ColumnConst * c0_const = checkAndGetColumnConstStringOrFixedString(c0);
        const ColumnConst * c1_const = checkAndGetColumnConstStringOrFixedString(c1);

        if (!((c0_string || c0_fixed_string || c0_const) && (c1_string || c1_fixed_string || c1_const)))
            return nullptr;

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
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnConst contains not String nor FixedString column");
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
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnConst contains not String nor FixedString column");
        }

        using StringImpl = StringComparisonImpl<Op<int, int>>;

        if (c0_const && c1_const)
        {
            auto res = executeString(&c0_const->getDataColumn(), &c1_const->getDataColumn());
            if (!res)
                return nullptr;

            return ColumnConst::create(res, c0_const->size());
        }

        auto c_res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = c_res->getData();
        vec_res.resize(c0->size());

        if (c0_string && c1_string)
            StringImpl::string_vector_string_vector(
                c0_string->getChars(), c0_string->getOffsets(), c1_string->getChars(), c1_string->getOffsets(), c_res->getData());
        else if (c0_string && c1_fixed_string)
            StringImpl::string_vector_fixed_string_vector(
                c0_string->getChars(), c0_string->getOffsets(), c1_fixed_string->getChars(), c1_fixed_string->getN(), c_res->getData());
        else if (c0_string && c1_const)
            StringImpl::string_vector_constant(
                c0_string->getChars(), c0_string->getOffsets(), *c1_const_chars, c1_const_size, c_res->getData());
        else if (c0_fixed_string && c1_string)
            StringImpl::fixed_string_vector_string_vector(
                c0_fixed_string->getChars(), c0_fixed_string->getN(), c1_string->getChars(), c1_string->getOffsets(), c_res->getData());
        else if (c0_fixed_string && c1_fixed_string)
            StringImpl::fixed_string_vector_fixed_string_vector(
                c0_fixed_string->getChars(),
                c0_fixed_string->getN(),
                c1_fixed_string->getChars(),
                c1_fixed_string->getN(),
                c_res->getData());
        else if (c0_fixed_string && c1_const)
            StringImpl::fixed_string_vector_constant(
                c0_fixed_string->getChars(), c0_fixed_string->getN(), *c1_const_chars, c1_const_size, c_res->getData());
        else if (c0_const && c1_string)
            StringImpl::constant_string_vector(
                *c0_const_chars, c0_const_size, c1_string->getChars(), c1_string->getOffsets(), c_res->getData());
        else if (c0_const && c1_fixed_string)
            StringImpl::constant_fixed_string_vector(
                *c0_const_chars, c0_const_size, c1_fixed_string->getChars(), c1_fixed_string->getN(), c_res->getData());
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal columns {} and {} of arguments of function {}",
                c0->getName(),
                c1->getName(),
                getName());

        return c_res;
    }

    ColumnPtr executeWithConstString(
            const DataTypePtr & result_type, const IColumn * col_left_untyped, const IColumn * col_right_untyped,
            const DataTypePtr & left_type, const DataTypePtr & right_type, size_t input_rows_count) const
    {
        /// To compare something with const string, we cast constant to appropriate type and compare as usual.
        /// It is ok to throw exception if value is not convertible.
        /// We should deal with possible overflows, e.g. toUInt8(1) = '257' should return false.

        const ColumnConst * left_const = checkAndGetColumnConstStringOrFixedString(col_left_untyped);
        const ColumnConst * right_const = checkAndGetColumnConstStringOrFixedString(col_right_untyped);

        if (!left_const && !right_const)
            return nullptr;

        const IDataType * type_string = left_const ? left_type.get() : right_type.get();
        const DataTypePtr & type_to_compare = !left_const ? left_type : right_type;

        Field string_value = left_const ? left_const->getField() : right_const->getField();
        Field converted = convertFieldToType(string_value, *type_to_compare, type_string);

        /// If not possible to convert, comparison with =, <, >, <=, >= yields to false and comparison with != yields to true.
        if (converted.isNull())
        {
            return DataTypeUInt8().createColumnConst(input_rows_count, IsOperation<Op>::not_equals);
        }

        auto column_converted = type_to_compare->createColumnConst(input_rows_count, converted);

        ColumnsWithTypeAndName tmp_columns{
            {left_const ? column_converted : col_left_untyped->getPtr(), type_to_compare, ""},
            {!left_const ? column_converted : col_right_untyped->getPtr(), type_to_compare, ""},
        };

        return executeImpl(tmp_columns, result_type, input_rows_count);
    }

    ColumnPtr executeTuple(
        const DataTypePtr & result_type, const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1,
        size_t input_rows_count) const
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
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Comparison of zero-sized tuples is not implemented.");

        if (tuple_size != typeid_cast<const DataTypeTuple &>(*c1.type).getElements().size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot compare tuples of different sizes.");

        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnsWithTypeAndName x(tuple_size);
        ColumnsWithTypeAndName y(tuple_size);

        const auto * x_const = checkAndGetColumnConst<ColumnTuple>(c0.column.get());
        const auto * y_const = checkAndGetColumnConst<ColumnTuple>(c1.column.get());

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

        return executeTupleImpl(x, y, tuple_size, input_rows_count);
    }

    ColumnPtr executeTupleImpl(const ColumnsWithTypeAndName & x,
                          const ColumnsWithTypeAndName & y, size_t tuple_size,
                          size_t input_rows_count) const;

    ColumnPtr executeTupleEqualityImpl(
            std::shared_ptr<IFunctionOverloadResolver> func_compare,
            std::shared_ptr<IFunctionOverloadResolver> func_convolution,
            const ColumnsWithTypeAndName & x,
            const ColumnsWithTypeAndName & y,
            size_t tuple_size,
            size_t input_rows_count) const
    {
        if (0 == tuple_size)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Comparison of zero-sized tuples is not implemented.");

        ColumnsWithTypeAndName convolution_columns(tuple_size);
        ColumnsWithTypeAndName tmp_columns(2);

        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_columns[0] = x[i];
            tmp_columns[1] = y[i];

            auto impl = func_compare->build(tmp_columns);
            convolution_columns[i].type = impl->getResultType();

            /// Comparison of the elements.
            convolution_columns[i].column = impl->execute(tmp_columns, impl->getResultType(), input_rows_count);
        }

        if (tuple_size == 1)
        {
            /// Do not call AND for single-element tuple.
            return convolution_columns[0].column;
        }

        /// Logical convolution.
        auto impl = func_convolution->build(convolution_columns);
        return impl->execute(convolution_columns, impl->getResultType(), input_rows_count);
    }

    ColumnPtr executeTupleLessGreaterImpl(
            std::shared_ptr<IFunctionOverloadResolver> func_compare_head,
            std::shared_ptr<IFunctionOverloadResolver> func_compare_tail,
            std::shared_ptr<IFunctionOverloadResolver> func_and,
            std::shared_ptr<IFunctionOverloadResolver> func_or,
            std::shared_ptr<IFunctionOverloadResolver> func_equals,
            const ColumnsWithTypeAndName & x,
            const ColumnsWithTypeAndName & y,
            size_t tuple_size,
            size_t input_rows_count) const
    {
        ColumnsWithTypeAndName less_columns(tuple_size);
        ColumnsWithTypeAndName equal_columns(tuple_size - 1);
        ColumnsWithTypeAndName tmp_columns(2);

        /// Pairwise comparison of the inequality of all elements; on the equality of all elements except the last.
        /// (x[i], y[i], x[i] < y[i], x[i] == y[i])
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_columns[0] = x[i];
            tmp_columns[1] = y[i];

            if (i + 1 != tuple_size)
            {
                auto impl_head = func_compare_head->build(tmp_columns);
                less_columns[i].type = impl_head->getResultType();
                less_columns[i].column = impl_head->execute(tmp_columns, less_columns[i].type, input_rows_count);

                auto impl_equals = func_equals->build(tmp_columns);
                equal_columns[i].type = impl_equals->getResultType();
                equal_columns[i].column = impl_equals->execute(tmp_columns, equal_columns[i].type, input_rows_count);

            }
            else
            {
                auto impl_tail = func_compare_tail->build(tmp_columns);
                less_columns[i].type = impl_tail->getResultType();
                less_columns[i].column = impl_tail->execute(tmp_columns, less_columns[i].type, input_rows_count);
            }
        }

        /// Combination. Complex code - make a drawing. It can be replaced by a recursive comparison of tuples.
        /// Last column contains intermediate result.
        /// Code is generally equivalent to:
        ///   res = `x < y`[tuple_size - 1];
        ///   for (int i = tuple_size - 2; i >= 0; --i)
        ///       res = (res && `x == y`[i]) || `x < y`[i];
        size_t i = tuple_size - 1;
        tmp_columns[0] = less_columns[i];
        while (i > 0)
        {
            --i;

            tmp_columns[1] = equal_columns[i];
            auto func_and_adaptor = func_and->build(tmp_columns);

            tmp_columns[0].column = func_and_adaptor->execute(tmp_columns, func_and_adaptor->getResultType(), input_rows_count);
            tmp_columns[0].type = func_and_adaptor->getResultType();

            tmp_columns[1] = less_columns[i];
            auto func_or_adaptor = func_or->build(tmp_columns);

            tmp_columns[0].column = func_or_adaptor->execute(tmp_columns, func_or_adaptor->getResultType(), input_rows_count);
            tmp_columns[tmp_columns.size() - 1].type = func_or_adaptor->getResultType();
        }

        return tmp_columns[0].column;
    }

    ColumnPtr executeGenericIdenticalTypes(const IColumn * c0, const IColumn * c1) const
    {
        bool c0_const = isColumnConst(*c0);
        bool c1_const = isColumnConst(*c1);

        /// This is a paranoid check to protect from a broken query analysis.
        if (c0->isNullable() != c1->isNullable())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Columns are assumed to be of identical types, but they are different in Nullable");

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constantConstant(*c0, *c1, res);
            return DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        }

        auto c_res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = c_res->getData();
        vec_res.resize(c0->size());

        if (c0_const)
            GenericComparisonImpl<Op<int, int>>::constantVector(*c0, *c1, vec_res);
        else if (c1_const)
            GenericComparisonImpl<Op<int, int>>::vectorConstant(*c0, *c1, vec_res);
        else
            GenericComparisonImpl<Op<int, int>>::vectorVector(*c0, *c1, vec_res);

        return c_res;
    }

    ColumnPtr executeGeneric(const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1) const
    {
        DataTypePtr common_type = getLeastSupertype(DataTypes{c0.type, c1.type});

        ColumnPtr c0_converted = castColumn(c0, common_type);
        ColumnPtr c1_converted = castColumn(c1, common_type);

        return executeGenericIdenticalTypes(c0_converted.get(), c1_converted.get());
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        WhichDataType left(arguments[0].get());
        WhichDataType right(arguments[1].get());

        const DataTypeTuple * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        const DataTypeTuple * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].get());

        bool both_represented_by_number = arguments[0]->isValueRepresentedByNumber() && arguments[1]->isValueRepresentedByNumber();
        bool has_date = left.isDateOrDate32() || right.isDateOrDate32();

        if (!((both_represented_by_number && !has_date)   /// Do not allow to compare date and number.
            || (left.isStringOrFixedString() || right.isStringOrFixedString())  /// Everything can be compared with string by conversion.
            /// You can compare the date, datetime, or datatime64 and an enumeration with a constant string.
            || ((left.isDate() || left.isDate32() || left.isDateTime() || left.isDateTime64()) && (right.isDate() || right.isDate32() || right.isDateTime() || right.isDateTime64()) && left.idx == right.idx) /// only date vs date, or datetime vs datetime
            || (left.isUUID() && right.isUUID())
            || ((left.isIPv4() || left.isIPv6()) && (right.isIPv4() || right.isIPv6()))
            || (left.isEnum() && right.isEnum() && arguments[0]->getName() == arguments[1]->getName()) /// only equivalent enum type values can be compared against
            || (left_tuple && right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
            || (arguments[0]->equals(*arguments[1]))))
        {
            if (!tryGetLeastSupertype(arguments))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types of arguments ({}, {})"
                    " of function {}", arguments[0]->getName(), arguments[1]->getName(), getName());
        }

        if (left_tuple && right_tuple)
        {
            auto func = FunctionToOverloadResolverAdaptor(std::make_shared<FunctionComparison<Op, Name>>(check_decimal_overflow));

            bool has_nullable = false;
            bool has_null = false;

            size_t size = left_tuple->getElements().size();
            for (size_t i = 0; i < size; ++i)
            {
                ColumnsWithTypeAndName args = {{nullptr, left_tuple->getElements()[i], ""},
                                               {nullptr, right_tuple->getElements()[i], ""}};
                auto element_type = func.build(args)->getResultType();
                has_nullable = has_nullable || element_type->isNullable();
                has_null = has_null || element_type->onlyNull();
            }

            /// If any element comparison is nullable, return type will also be nullable.
            /// We useDefaultImplementationForNulls, but it doesn't work for tuples.
            if (has_null)
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
            if (has_nullable)
                return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & col_with_type_and_name_left = arguments[0];
        const auto & col_with_type_and_name_right = arguments[1];
        const IColumn * col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn * col_right_untyped = col_with_type_and_name_right.column.get();

        const DataTypePtr & left_type = col_with_type_and_name_left.type;
        const DataTypePtr & right_type = col_with_type_and_name_right.type;

        /// The case when arguments are the same (tautological comparison). Return constant.
        /// NOTE: Nullable types are special case.
        /// (BTW, this function use default implementation for Nullable, so Nullable types cannot be here. Check just in case.)
        if (left_type->equals(*right_type) &&
            !left_type->isNullable() &&
            !isTuple(left_type) &&
            !WhichDataType(left_type).isFloat() &&
            col_left_untyped == col_right_untyped)
        {
            ColumnPtr result_column;

            /// Always true: =, <=, >=
            if constexpr (IsOperation<Op>::equals
                || IsOperation<Op>::less_or_equals
                || IsOperation<Op>::greater_or_equals)
            {
                result_column = DataTypeUInt8().createColumnConst(input_rows_count, 1u);
            }
            else
            {
                result_column = DataTypeUInt8().createColumnConst(input_rows_count, 0u);
            }

            if (!isColumnConst(*col_left_untyped))
                result_column = result_column->convertToFullColumnIfConst();

            return result_column;
        }

        WhichDataType which_left{left_type};
        WhichDataType which_right{right_type};

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        const bool left_is_string = which_left.isStringOrFixedString();
        const bool right_is_string = which_right.isStringOrFixedString();

        const bool left_is_float = which_left.isFloat();
        const bool right_is_float = which_right.isFloat();

        const bool left_is_ipv4 = which_left.isIPv4();
        const bool right_is_ipv4 = which_right.isIPv4();
        const bool left_is_ipv6 = which_left.isIPv6();
        const bool right_is_ipv6 = which_right.isIPv6();
        const bool left_is_fixed_string = which_left.isFixedString();
        const bool right_is_fixed_string = which_right.isFixedString();
        size_t fixed_string_size =
            left_is_fixed_string ?
                assert_cast<const DataTypeFixedString &>(*left_type).getN() :
                (right_is_fixed_string ? assert_cast<const DataTypeFixedString &>(*right_type).getN() : 0);

        bool date_and_datetime = (which_left.idx != which_right.idx) && (which_left.isDate() || which_left.isDate32() || which_left.isDateTime() || which_left.isDateTime64())
            && (which_right.isDate() || which_right.isDate32() || which_right.isDateTime() || which_right.isDateTime64());

        /// Interval data types can be compared only when having equal units.
        bool left_is_interval = which_left.isInterval();
        bool right_is_interval = which_right.isInterval();

        bool types_equal = left_type->equals(*right_type);

        ColumnPtr res;
        if (left_is_num && right_is_num && !date_and_datetime
            && (!left_is_interval || !right_is_interval || types_equal))
        {
            if (!((res = executeNumLeftType<UInt8>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<UInt16>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<UInt32>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<UInt64>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<UInt128>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<UInt256>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int8>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int16>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int32>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int64>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int128>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Int256>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Float32>(col_left_untyped, col_right_untyped))
                || (res = executeNumLeftType<Float64>(col_left_untyped, col_right_untyped))))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    col_left_untyped->getName(), getName());

            return res;
        }
        if (checkAndGetDataType<DataTypeTuple>(left_type.get()) && checkAndGetDataType<DataTypeTuple>(right_type.get()))
        {
            return executeTuple(result_type, col_with_type_and_name_left, col_with_type_and_name_right, input_rows_count);
        }
        if (left_is_string && right_is_string && (res = executeString(col_left_untyped, col_right_untyped)))
        {
            return res;
        }
        if ((res = executeWithConstString(result_type, col_left_untyped, col_right_untyped, left_type, right_type, input_rows_count)))
        {
            return res;
        }
        if ((((left_is_ipv6 && right_is_fixed_string) || (right_is_ipv6 && left_is_fixed_string))
             && fixed_string_size == IPV6_BINARY_LENGTH)
            || ((left_is_ipv4 || left_is_ipv6) && (right_is_ipv4 || right_is_ipv6)))
        {
            /// Special treatment for FixedString(16) as a binary representation of IPv6 & for comparing IPv4 & IPv6 values -
            /// CAST is customized for this cases
            ColumnPtr left_column = left_is_ipv6 ? col_with_type_and_name_left.column : castColumn(col_with_type_and_name_left, right_type);
            ColumnPtr right_column
                = right_is_ipv6 ? col_with_type_and_name_right.column : castColumn(col_with_type_and_name_right, left_type);

            return executeGenericIdenticalTypes(left_column.get(), right_column.get());
        }
        if ((isColumnedAsDecimal(left_type) || isColumnedAsDecimal(right_type)))
        {
            // Comparing Date/Date32 and DateTime64 requires implicit conversion,
            if (date_and_datetime && (isDateOrDate32(left_type) || isDateOrDate32(right_type)))
            {
                DataTypePtr common_type = getLeastSupertype(DataTypes{left_type, right_type});
                ColumnPtr c0_converted = castColumn(col_with_type_and_name_left, common_type);
                ColumnPtr c1_converted = castColumn(col_with_type_and_name_right, common_type);
                return executeDecimal({c0_converted, common_type, "left"}, {c1_converted, common_type, "right"});
            }

            /// Check does another data type is comparable to Decimal, includes Int and Float.
            if (!allowDecimalComparison(left_type, right_type) && !date_and_datetime)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "No operation {} between {} and {}",
                    getName(),
                    left_type->getName(),
                    right_type->getName());
            /// When Decimal comparing to Float32/64, we convert both of them into Float64.
            /// Other systems like MySQL and Spark also do as this.
            if (left_is_float || right_is_float)
            {
                const auto converted_type = std::make_shared<DataTypeFloat64>();
                ColumnPtr c0_converted = castColumn(col_with_type_and_name_left, converted_type);
                ColumnPtr c1_converted = castColumn(col_with_type_and_name_right, converted_type);

                auto new_arguments
                    = ColumnsWithTypeAndName{{c0_converted, converted_type, "left"}, {c1_converted, converted_type, "right"}};
                return executeImpl(new_arguments, result_type, input_rows_count);
            }
            return executeDecimal(col_with_type_and_name_left, col_with_type_and_name_right);
        }
        if (date_and_datetime)
        {
            DataTypePtr common_type = getLeastSupertype(DataTypes{left_type, right_type});
            ColumnPtr c0_converted = castColumn(col_with_type_and_name_left, common_type);
            ColumnPtr c1_converted = castColumn(col_with_type_and_name_right, common_type);
            if (!((res = executeNumLeftType<UInt32>(c0_converted.get(), c1_converted.get()))
                  || (res = executeNumLeftType<UInt64>(c0_converted.get(), c1_converted.get()))
                  || (res = executeNumLeftType<Int32>(c0_converted.get(), c1_converted.get()))
                  || (res = executeDecimal({c0_converted, common_type, "left"}, {c1_converted, common_type, "right"}))))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Date related common types can only be UInt32/UInt64/Int32/Decimal");
            return res;
        }
        if (types_equal)
        {
            return executeGenericIdenticalTypes(col_left_untyped, col_right_untyped);
        }

        return executeGeneric(col_with_type_and_name_left, col_with_type_and_name_right);
    }
};

}
