#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getLeastSupertype.h>

#include <Interpreters/castColumn.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <Core/AccurateComparison.h>
#include <Core/DecimalComparison.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <limits>
#include <type_traits>


namespace DB
{


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
    static void NO_INLINE vector_vector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        /** GCC 4.8.2 vectorizes a loop only if it is written in this form.
          * In this case, if you loop through the array index (the code will look simpler),
          *  the loop will not be vectorized.
          */

        size_t size = a.size();
        const A * a_pos = a.data();
        const B * b_pos = b.data();
        UInt8 * c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, *b_pos);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vector_constant(const PaddedPODArray<A> & a, B b, PaddedPODArray<UInt8> & c)
    {
        size_t size = a.size();
        const A * a_pos = a.data();
        UInt8 * c_pos = c.data();
        const A * a_end = a_pos + size;

        while (a_pos < a_end)
        {
            *c_pos = Op::apply(*a_pos, b);
            ++a_pos;
            ++c_pos;
        }
    }

    static void constant_vector(A a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        NumComparisonImpl<B, A, typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(A a, B b, UInt8 & c)
    {
        c = Op::apply(a, b);
    }
};


inline int memcmp16(const void * a, const void * b)
{
    /// Assuming little endian.

    UInt64 a_hi = __builtin_bswap64(unalignedLoad<UInt64>(a));
    UInt64 b_hi = __builtin_bswap64(unalignedLoad<UInt64>(b));

    if (a_hi < b_hi)
        return -1;
    if (a_hi > b_hi)
        return 1;

    UInt64 a_lo = __builtin_bswap64(unalignedLoad<UInt64>(reinterpret_cast<const char *>(a) + 8));
    UInt64 b_lo = __builtin_bswap64(unalignedLoad<UInt64>(reinterpret_cast<const char *>(b) + 8));

    if (a_lo < b_lo)
        return -1;
    if (a_lo > b_lo)
        return 1;

    return 0;
}


template <typename Op>
struct StringComparisonImpl
{
    static void NO_INLINE string_vector_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            /// Trailing zero byte of the smaller string is included in the comparison.
            size_t a_size;
            size_t b_size;
            int res;
            if (i == 0)
            {
                a_size = a_offsets[0];
                b_size = b_offsets[0];
                res = memcmp(a_data.data(), b_data.data(), std::min(a_size, b_size));
            }
            else
            {
                a_size = a_offsets[i] - a_offsets[i - 1];
                b_size = b_offsets[i] - b_offsets[i - 1];
                res = memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], std::min(a_size, b_size));
            }

            c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_size, b_size));
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i == 0)
            {
                int res = memcmp(a_data.data(), b_data.data(), std::min(a_offsets[0] - 1, b_n));
                c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[0], b_n + 1));
            }
            else
            {
                int res = memcmp(&a_data[a_offsets[i - 1]], &b_data[i * b_n],
                    std::min(a_offsets[i] - a_offsets[i - 1] - 1, b_n));
                c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[i] - a_offsets[i - 1], b_n + 1));
            }
        }
    }

    static void NO_INLINE string_vector_constant(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset b_size = b.size() + 1;
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
        {
            /// Trailing zero byte of the smaller string is included in the comparison.
            if (i == 0)
            {
                int res = memcmp(a_data.data(), b_data, std::min(a_offsets[0], b_size));
                c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[0], b_size));
            }
            else
            {
                int res = memcmp(&a_data[a_offsets[i - 1]], b_data, std::min(a_offsets[i] - a_offsets[i - 1], b_size));
                c[i] = Op::apply(res, 0) || (res == 0 && Op::apply(a_offsets[i] - a_offsets[i - 1], b_size));
            }
        }
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Chars_t & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[i]), 0);
    }

    static void NO_INLINE fixed_string_vector_constant_16(
        const ColumnString::Chars_t & a_data,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], b.data()), 0);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        /** Specialization if both sizes are 16.
          * To more efficient comparison of IPv6 addresses stored in FixedString(16).
          */
        if (a_n == 16 && b_n == 16)
        {
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        }
        else
        {
            /// Generic implementation, less efficient.
            size_t size = a_data.size();

            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
            {
                int res = memcmp(&a_data[i], &b_data[i], std::min(a_n, b_n));
                c[j] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n, b_n));
            }
        }
    }

    static void NO_INLINE fixed_string_vector_constant(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        ColumnString::Offset b_n = b.size();
        if (a_n == 16 && b_n == 16)
        {
            fixed_string_vector_constant_16(a_data, b, c);
        }
        else
        {
            size_t size = a_data.size();
            const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
            {
                int res = memcmp(&a_data[i], b_data, std::min(a_n, b_n));
                c[j] = Op::apply(res, 0) || (res == 0 && Op::apply(a_n, b_n));
            }
        }
    }

    static void constant_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets, a, c);
    }

    static void constant_fixed_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::fixed_string_vector_constant(b_data, b_n, a, c);
    }

    static void constant_constant(
        const std::string & a,
        const std::string & b,
        UInt8 & c)
    {
        size_t a_n = a.size();
        size_t b_n = b.size();

        int res = memcmp(a.data(), b.data(), std::min(a_n, b_n));
        c = Op::apply(res, 0) || (res == 0 && Op::apply(a_n, b_n));
    }
};


/// Comparisons for equality/inequality are implemented slightly more efficient.
template <bool positive>
struct StringEqualsImpl
{
    static void NO_INLINE string_vector_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_offsets[0] && !memcmp(a_data.data(), b_data.data(), a_offsets[0] - 1))
                : (a_offsets[i] - a_offsets[i - 1] == b_offsets[i] - b_offsets[i - 1]
                    && !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], a_offsets[i] - a_offsets[i - 1] - 1)));
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_n + 1 && !memcmp(a_data.data(), b_data.data(), b_n))
                : (a_offsets[i] - a_offsets[i - 1] == b_n + 1
                    && !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_n * i], b_n)));
    }

    static void NO_INLINE string_vector_constant(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets & a_offsets,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset b_n = b.size();
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_n + 1 && !memcmp(a_data.data(), b_data, b_n))
                : (a_offsets[i] - a_offsets[i - 1] == b_n + 1
                    && !memcmp(&a_data[a_offsets[i - 1]], b_data, b_n)));
    }

#if __SSE2__
    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
        const ColumnString::Chars_t & a_data,
        const ColumnString::Chars_t & b_data,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = c.size();

        const __m128i * a_pos = reinterpret_cast<const __m128i *>(a_data.data());
        const __m128i * b_pos = reinterpret_cast<const __m128i *>(b_data.data());
        UInt8 * c_pos = c.data();
        UInt8 * c_end = c_pos + size;

        while (c_pos < c_end)
        {
            *c_pos = positive == (0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(a_pos),
                _mm_loadu_si128(b_pos))));

            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE fixed_string_vector_constant_16(
        const ColumnString::Chars_t & a_data,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = c.size();

        const __m128i * a_pos = reinterpret_cast<const __m128i *>(a_data.data());
        const __m128i b_value = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b.data()));
        UInt8 * c_pos = c.data();
        UInt8 * c_end = c_pos + size;

        while (c_pos < c_end)
        {
            *c_pos = positive == (0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_loadu_si128(a_pos),
                b_value)));

            ++a_pos;
            ++c_pos;
        }
    }
#endif

    static void NO_INLINE fixed_string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        /** Specialization if both sizes are 16.
          * To more efficient comparison of IPv6 addresses stored in FixedString(16).
          */
#if __SSE2__
        if (a_n == 16 && b_n == 16)
        {
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        }
        else
#endif
        {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = positive == (a_n == b_n && !memcmp(&a_data[i], &b_data[i], a_n));
        }
    }

    static void NO_INLINE fixed_string_vector_constant(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        ColumnString::Offset b_n = b.size();
#if __SSE2__
        if (a_n == 16 && b_n == 16)
        {
            fixed_string_vector_constant_16(a_data, b, c);
        }
        else
#endif
        {
            size_t size = a_data.size();
            const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = positive == (a_n == b_n && !memcmp(&a_data[i], b_data, a_n));
        }
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset a_n,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void constant_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_constant(b_data, b_offsets, a, c);
    }

    static void constant_fixed_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, ColumnString::Offset b_n,
        PaddedPODArray<UInt8> & c)
    {
        fixed_string_vector_constant(b_data, b_n, a, c);
    }

    static void constant_constant(
        const std::string & a,
        const std::string & b,
        UInt8 & c)
    {
        c = positive == (a == b);
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
    static void NO_INLINE vector_vector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, i, b, 1), 0);
    }

    static void NO_INLINE vector_constant(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        auto b_materialized = b.cloneResized(1)->convertToFullColumnIfConst();
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, 0, *b_materialized, 1), 0);
    }

    static void constant_vector(const IColumn & a, const IColumn & b, PaddedPODArray<UInt8> & c)
    {
        GenericComparisonImpl<typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(const IColumn & a, const IColumn & b, UInt8 & c)
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

    FunctionComparison(const Context & context)
    :   context(context),
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
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_constant(col_left->getData(), col_right_const->template getValue<T1>(), vec_res);

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
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_vector(col_left->template getValue<T0>(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        else if (auto col_right_const = checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped))
        {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_constant(col_left->template getValue<T0>(), col_right_const->template getValue<T1>(), res);

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

        if (!callOnBasicTypes(left_number, right_number, call))
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

        using StringImpl = StringComparisonImpl<Op<int, int>>;

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            StringImpl::constant_constant(c0_const->getValue<String>(), c1_const->getValue<String>(), res);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(c0_const->size(), toField(res));
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
                    c1_const->getValue<String>(),
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
                    c1_const->getValue<String>(),
                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constant_string_vector(
                    c0_const->getValue<String>(),
                    c1_string->getChars(), c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_const && c1_fixed_string)
                StringImpl::constant_fixed_string_vector(
                    c0_const->getValue<String>(),
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
                throw Exception("String is too long for Date: " + string_value.toString());

            ColumnPtr parsed_const_date_holder = DataTypeDate().createColumnConst(input_rows_count, UInt64(date));
            const ColumnConst * parsed_const_date = static_cast<const ColumnConst *>(parsed_const_date_holder.get());
            executeNumLeftType<DataTypeDate::FieldType>(block, result,
                left_is_num ? col_left_untyped : parsed_const_date,
                left_is_num ? parsed_const_date : col_right_untyped);
        }
        else if (which.isDateTime())
        {
            time_t date_time;
            ReadBufferFromMemory in(string_value.data, string_value.size);
            readDateTimeText(date_time, in);
            if (!in.eof())
                throw Exception("String is too long for DateTime: " + string_value.toString());

            ColumnPtr parsed_const_date_time_holder = DataTypeDateTime().createColumnConst(input_rows_count, UInt64(date_time));
            const ColumnConst * parsed_const_date_time = static_cast<const ColumnConst *>(parsed_const_date_time_holder.get());
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
                throw Exception("String is too long for UUID: " + string_value.toString());

            ColumnPtr parsed_const_uuid_holder = DataTypeUUID().createColumnConst(input_rows_count, UInt128(uuid));
            const ColumnConst * parsed_const_uuid = static_cast<const ColumnConst *>(parsed_const_uuid_holder.get());
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

        const Field x = nearestFieldType(type->getValue(column_string->getValue<String>()));
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
            x_columns = static_cast<const ColumnTuple &>(*c0.column).getColumns();

        if (y_const)
            y_columns = convertConstTupleToConstantElements(*y_const);
        else
            y_columns = static_cast<const ColumnTuple &>(*c1.column).getColumns();

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

    template <typename ComparisonFunction, typename ConvolutionFunction>
    void executeTupleEqualityImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x, const ColumnsWithTypeAndName & y,
                                      size_t tuple_size, size_t input_rows_count)
    {
        ComparisonFunction func_compare(context);
        ConvolutionFunction func_convolution;

        Block tmp_block;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            /// Comparison of the elements.
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_compare.execute(tmp_block, {i * 3, i * 3 + 1}, i * 3 + 2, input_rows_count);
        }

        /// Logical convolution.
        tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

        ColumnNumbers convolution_args(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            convolution_args[i] = i * 3 + 2;

        func_convolution.execute(tmp_block, convolution_args, tuple_size * 3, input_rows_count);
        block.getByPosition(result).column = tmp_block.getByPosition(tuple_size * 3).column;
    }

    template <typename HeadComparisonFunction, typename TailComparisonFunction>
    void executeTupleLessGreaterImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
                                         const ColumnsWithTypeAndName & y, size_t tuple_size, size_t input_rows_count)
    {
        HeadComparisonFunction func_compare_head(context);
        TailComparisonFunction func_compare_tail(context);
        FunctionAnd func_and;
        FunctionOr func_or;
        FunctionComparison<EqualsOp, NameEquals> func_equals(context);

        Block tmp_block;

        /// Pairwise comparison of the inequality of all elements; on the equality of all elements except the last.
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x[i]);
            tmp_block.insert(y[i]);

            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

            if (i + 1 != tuple_size)
            {
                func_compare_head.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2, input_rows_count);

                tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
                func_equals.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 3, input_rows_count);

            }
            else
                func_compare_tail.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2, input_rows_count);
        }

        /// Combination. Complex code - make a drawing. It can be replaced by a recursive comparison of tuples.
        size_t i = tuple_size - 1;
        while (i > 0)
        {
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_and.execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 3}, tmp_block.columns() - 1, input_rows_count);
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_or.execute(tmp_block, {tmp_block.columns() - 2, (i - 1) * 4 + 2}, tmp_block.columns() - 1, input_rows_count);
            --i;
        }

        block.getByPosition(result).column = tmp_block.getByPosition(tmp_block.columns() - 1).column;
    }

    void executeGenericIdenticalTypes(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        bool c0_const = c0->isColumnConst();
        bool c1_const = c1->isColumnConst();

        if (c0_const && c1_const)
        {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constant_constant(*c0, *c1, res);
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        }
        else
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constant_vector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vector_constant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vector_vector(*c0, *c1, vec_res);

            block.getByPosition(result).column = std::move(c_res);
        }
    }

    void executeGeneric(Block & block, size_t result, const ColumnWithTypeAndName & c0, const ColumnWithTypeAndName & c1)
    {
        DataTypePtr common_type = getLeastSupertype({c0.type, c1.type});

        ColumnPtr c0_converted = castColumn(c0, common_type, context);
        ColumnPtr c1_converted = castColumn(c1, common_type, context);

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

        if (!((arguments[0]->isValueRepresentedByNumber() && arguments[1]->isValueRepresentedByNumber())
            || (left.isStringOrFixedString() && right.isStringOrFixedString())
            || (left.isDate() && right.isDate())
            || (left.isDate() && right.isString())    /// You can compare the date, datetime and an enumeration with a constant string.
            || (left.isString() && right.isDate())
            || (left.isDateTime() && right.isDateTime())
            || (left.isDateTime() && right.isString())
            || (left.isString() && right.isDateTime())
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
            size_t size = left_tuple->getElements().size();
            for (size_t i = 0; i < size; ++i)
            {
                ColumnsWithTypeAndName args = {{nullptr, left_tuple->getElements()[i], ""},
                                               {nullptr, right_tuple->getElements()[i], ""}};
                getReturnType(args);
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

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        if (left_is_num && right_is_num)
        {
            if (!( executeNumLeftType<UInt8>(block, result, col_left_untyped, col_right_untyped)
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
        else if (isDecimal(left_type) || isDecimal(right_type))
        {
            if (!allowDecimalComparison(left_type, right_type))
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
        if ((isBigInteger(*types[0]) && isFloatingPoint(*types[1])) || (isBigInteger(*types[1]) && isFloatingPoint(*types[0])))
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
