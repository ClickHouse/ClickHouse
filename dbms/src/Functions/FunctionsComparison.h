#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/AccurateComparison.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <limits>
#include <type_traits>


namespace DB
{

/** Функции сравнения: ==, !=, <, >, <=, >=.
  * Функции сравнения возвращают всегда 0 или 1 (UInt8).
  *
  * Сравнивать можно следующие типы:
  * - числа;
  * - строки и фиксированные строки;
  * - даты;
  * - даты-с-временем;
  *   внутри каждой группы, но не из разных групп;
  * - кортежи (сравнение лексикографическое).
  *
  * Исключение: можно сравнивать дату и дату-с-временем с константной строкой. Пример: EventDate = '2015-01-01'.
  *
  * TODO Массивы.
  */

template <typename A, typename B> struct EqualsOp
{
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<B, A>;

    static UInt8 apply(A a, B b) { return accurate::equalsOp(a, b); }
};

template <typename A, typename B> struct NotEqualsOp
{
    using SymmetricOp = NotEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::notEqualsOp(a, b); }
};

template <typename A, typename B> struct GreaterOp;

template <typename A, typename B> struct LessOp
{
    using SymmetricOp = GreaterOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOp(a, b); }
};

template <typename A, typename B> struct GreaterOp
{
    using SymmetricOp = LessOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOp(a, b); }
};

template <typename A, typename B> struct GreaterOrEqualsOp;

template <typename A, typename B> struct LessOrEqualsOp
{
    using SymmetricOp = GreaterOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOrEqualsOp(a, b); }
};

template <typename A, typename B> struct GreaterOrEqualsOp
{
    using SymmetricOp = LessOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOrEqualsOp(a, b); }
};


template<typename A, typename B, typename Op>
struct NumComparisonImpl
{
    /// If you don't specify NO_INLINE, the compiler will inline this function, but we don't need this as this function contains tight loop inside.
    static void NO_INLINE vector_vector(const PaddedPODArray<A> & a, const PaddedPODArray<B> & b, PaddedPODArray<UInt8> & c)
    {
        /** GCC 4.8.2 векторизует цикл только если его записать в такой форме.
          * В данном случае, если сделать цикл по индексу массива (код будет выглядеть проще),
          *  цикл не будет векторизовываться.
          */

        size_t size = a.size();
        const A * a_pos = &a[0];
        const B * b_pos = &b[0];
        UInt8 * c_pos = &c[0];
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
        const A * a_pos = &a[0];
        UInt8 * c_pos = &c[0];
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
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i == 0)
            {
                /// Завершающий ноль в меньшей по длине строке входит в сравнение.
                c[i] = Op::apply(memcmp(&a_data[0], &b_data[0], std::min(a_offsets[0], b_offsets[0])), 0);
            }
            else
            {
                c[i] = Op::apply(memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]],
                    std::min(a_offsets[i] - a_offsets[i - 1], b_offsets[i] - b_offsets[i - 1])), 0);
            }
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i == 0)
            {
                int res = memcmp(&a_data[0], &b_data[0], std::min(a_offsets[0] - 1, b_n));
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
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset_t b_n = b.size();
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
        {
            if (i == 0)
            {
                c[i] = Op::apply(memcmp(&a_data[0], b_data, std::min(a_offsets[0], b_n + 1)), 0);
            }
            else
            {
                c[i] = Op::apply(memcmp(&a_data[a_offsets[i - 1]], b_data,
                    std::min(a_offsets[i] - a_offsets[i - 1], b_n + 1)), 0);
            }
        }
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
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
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
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
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        ColumnString::Offset_t b_n = b.size();
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
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets, a, c);
    }

    static void constant_fixed_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
        PaddedPODArray<UInt8> & c)
    {
        StringComparisonImpl<typename Op::SymmetricOp>::fixed_string_vector_constant(b_data, b_n, a, c);
    }

    static void constant_constant(
        const std::string & a,
        const std::string & b,
        UInt8 & c)
    {
        c = Op::apply(memcmp(a.data(), b.data(), std::min(a.size(), b.size()) + 1), 0);
    }
};


/// Comparisons for equality/inequality are implemented slightly more efficient.
template <bool positive>
struct StringEqualsImpl
{
    static void NO_INLINE string_vector_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_offsets[0] && !memcmp(&a_data[0], &b_data[0], a_offsets[0] - 1))
                : (a_offsets[i] - a_offsets[i - 1] == b_offsets[i] - b_offsets[i - 1]
                    && !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_offsets[i - 1]], a_offsets[i] - a_offsets[i - 1] - 1)));
    }

    static void NO_INLINE string_vector_fixed_string_vector(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], &b_data[0], b_n))
                : (a_offsets[i] - a_offsets[i - 1] == b_n + 1
                    && !memcmp(&a_data[a_offsets[i - 1]], &b_data[b_n * i], b_n)));
    }

    static void NO_INLINE string_vector_constant(
        const ColumnString::Chars_t & a_data, const ColumnString::Offsets_t & a_offsets,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        size_t size = a_offsets.size();
        ColumnString::Offset_t b_n = b.size();
        const UInt8 * b_data = reinterpret_cast<const UInt8 *>(b.data());
        for (size_t i = 0; i < size; ++i)
            c[i] = positive == ((i == 0)
                ? (a_offsets[0] == b_n + 1 && !memcmp(&a_data[0], b_data, b_n))
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
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
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
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const std::string & b,
        PaddedPODArray<UInt8> & c)
    {
        ColumnString::Offset_t b_n = b.size();
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

    static void constant_constant(
        const std::string & a,
        const std::string & b,
        UInt8 & c)
    {
        c = positive == (a == b);
    }

    static void fixed_string_vector_string_vector(
        const ColumnString::Chars_t & a_data, ColumnString::Offset_t a_n,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void constant_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, const ColumnString::Offsets_t & b_offsets,
        PaddedPODArray<UInt8> & c)
    {
        string_vector_constant(b_data, b_offsets, a, c);
    }

    static void constant_fixed_string_vector(
        const std::string & a,
        const ColumnString::Chars_t & b_data, ColumnString::Offset_t b_n,
        PaddedPODArray<UInt8> & c)
    {
        fixed_string_vector_constant(b_data, b_n, a, c);
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


struct NameEquals             { static constexpr auto name = "equals"; };
struct NameNotEquals         { static constexpr auto name = "notEquals"; };
struct NameLess             { static constexpr auto name = "less"; };
struct NameGreater             { static constexpr auto name = "greater"; };
struct NameLessOrEquals     { static constexpr auto name = "lessOrEquals"; };
struct NameGreaterOrEquals     { static constexpr auto name = "greaterOrEquals"; };


template <
    template <typename, typename> class Op,
    typename Name>
class FunctionComparison : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionComparison>(); };

private:
    template <typename T0, typename T1>
    bool executeNumRightType(Block & block, size_t result, const ColumnVector<T0> * col_left, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T1> * col_right = typeid_cast<const ColumnVector<T1> *>(col_right_untyped))
        {
            std::shared_ptr<ColumnUInt8> col_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = col_res;

            ColumnUInt8::Container_t & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }
        else if (const ColumnConst<T1> * col_right = typeid_cast<const ColumnConst<T1> *>(col_right_untyped))
        {
            std::shared_ptr<ColumnUInt8> col_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = col_res;

            ColumnUInt8::Container_t & vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_constant(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }

        return false;
    }

    template <typename T0, typename T1>
    bool executeNumConstRightType(Block & block, size_t result, const ColumnConst<T0> * col_left, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T1> * col_right = typeid_cast<const ColumnVector<T1> *>(col_right_untyped))
        {
            std::shared_ptr<ColumnUInt8> col_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = col_res;

            ColumnUInt8::Container_t & vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_vector(col_left->getData(), col_right->getData(), vec_res);

            return true;
        }
        else if (const ColumnConst<T1> * col_right = typeid_cast<const ColumnConst<T1> *>(col_right_untyped))
        {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_constant(col_left->getData(), col_right->getData(), res);

            auto col_res = std::make_shared<ColumnConstUInt8>(col_left->size(), res);
            block.safeGetByPosition(result).column = col_res;

            return true;
        }

        return false;
    }

    template <typename T0>
    bool executeNumLeftType(Block & block, size_t result, const IColumn * col_left_untyped, const IColumn * col_right_untyped)
    {
        if (const ColumnVector<T0> * col_left = typeid_cast<const ColumnVector<T0> *>(col_left_untyped))
        {
            if (    executeNumRightType<T0, UInt8>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, UInt16>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, UInt32>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, UInt64>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Int8>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Int16>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Int32>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Int64>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Float32>(block, result, col_left, col_right_untyped)
                ||    executeNumRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const ColumnConst<T0> * col_left = typeid_cast<const ColumnConst<T0> *>(col_left_untyped))
        {
            if (    executeNumConstRightType<T0, UInt8>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, UInt16>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, UInt32>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, UInt64>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Int8>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Int16>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Int32>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Int64>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Float32>(block, result, col_left, col_right_untyped)
                ||    executeNumConstRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName()
                    + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
    }

    bool executeString(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        const ColumnString * c0_string = typeid_cast<const ColumnString *>(c0);
        const ColumnString * c1_string = typeid_cast<const ColumnString *>(c1);
        const ColumnFixedString * c0_fixed_string = typeid_cast<const ColumnFixedString *>(c0);
        const ColumnFixedString * c1_fixed_string = typeid_cast<const ColumnFixedString *>(c1);
        const ColumnConstString * c0_const = typeid_cast<const ColumnConstString *>(c0);
        const ColumnConstString * c1_const = typeid_cast<const ColumnConstString *>(c1);

        if (!((c0_string || c0_fixed_string || c0_const) && (c1_string || c1_fixed_string || c1_const)))
            return false;

        using StringImpl = StringComparisonImpl<Op<int, int>>;

        if (c0_const && c1_const)
        {
            auto c_res = std::make_shared<ColumnConstUInt8>(c0_const->size(), 0);
            block.safeGetByPosition(result).column = c_res;
            StringImpl::constant_constant(c0_const->getData(), c1_const->getData(), c_res->getData());
        }
        else
        {
            auto c_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = c_res;
            ColumnUInt8::Container_t & vec_res = c_res->getData();
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
                    c1_const->getData(),
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
                    c1_const->getData(),
                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constant_string_vector(
                    c0_const->getData(),
                    c1_string->getChars(), c1_string->getOffsets(),
                    c_res->getData());
            else if (c0_const && c1_fixed_string)
                StringImpl::constant_fixed_string_vector(
                    c0_const->getData(),
                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
                    c_res->getData());
            else
                throw Exception("Illegal columns "
                    + c0->getName() + " and " + c1->getName()
                    + " of arguments of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        return true;
    }

    void executeDateOrDateTimeOrEnumWithConstString(
        Block & block, size_t result, const IColumn * col_left_untyped, const IColumn * col_right_untyped,
        const DataTypePtr & left_type, const DataTypePtr & right_type, bool left_is_num, bool right_is_num)
    {
        /// Уже не такой и особый случай - сравнение дат, дат-с-временем и перечислений со строковой константой.
        const IColumn * column_string_untyped = !left_is_num ? col_left_untyped : col_right_untyped;
        const IColumn * column_number = left_is_num ? col_left_untyped : col_right_untyped;
        const IDataType * number_type = left_is_num ? left_type.get() : right_type.get();

        bool is_date = false;
        bool is_date_time = false;
        bool is_enum8 = false;
        bool is_enum16 = false;

        const auto legal_types = (is_date = typeid_cast<const DataTypeDate *>(number_type))
            || (is_date_time = typeid_cast<const DataTypeDateTime *>(number_type))
            || (is_enum8 = typeid_cast<const DataTypeEnum8 *>(number_type))
            || (is_enum16 = typeid_cast<const DataTypeEnum16 *>(number_type));

        const auto column_string = typeid_cast<const ColumnConstString *>(column_string_untyped);
        if (!column_string || !legal_types)
            throw Exception{
                "Illegal columns " + col_left_untyped->getName() + " and " + col_right_untyped->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN
            };

        if (is_date)
        {
            DayNum_t date;
            ReadBufferFromString in(column_string->getData());
            readDateText(date, in);
            if (!in.eof())
                throw Exception("String is too long for Date: " + column_string->getData());

            ColumnConst<DataTypeDate::FieldType> parsed_const_date(block.rows(), date);
            executeNumLeftType<DataTypeDate::FieldType>(block, result,
                left_is_num ? col_left_untyped : &parsed_const_date,
                left_is_num ? &parsed_const_date : col_right_untyped);
        }
        else if (is_date_time)
        {
            time_t date_time;
            ReadBufferFromString in(column_string->getData());
            readDateTimeText(date_time, in);
            if (!in.eof())
                throw Exception("String is too long for DateTime: " + column_string->getData());

            ColumnConst<DataTypeDateTime::FieldType> parsed_const_date_time(block.rows(), date_time);
            executeNumLeftType<DataTypeDateTime::FieldType>(block, result,
                left_is_num ? col_left_untyped : &parsed_const_date_time,
                left_is_num ? &parsed_const_date_time : col_right_untyped);
        }
        else if (is_enum8)
            executeEnumWithConstString<DataTypeEnum8>(block, result, column_number, column_string,
                number_type, left_is_num);
        else if (is_enum16)
            executeEnumWithConstString<DataTypeEnum16>(block, result, column_number, column_string,
                number_type, left_is_num);
    }

    /// Comparison between DataTypeEnum<T> and string constant containing the name of an enum element
    template <typename EnumType>
    void executeEnumWithConstString(
        Block & block, const size_t result, const IColumn * column_number, const ColumnConstString * column_string,
        const IDataType * type_untyped, const bool left_is_num)
    {
        const auto type = static_cast<const EnumType *>(type_untyped);

        const Field x = nearestFieldType(type->getValue(column_string->getData()));
        const auto enum_col = type->createConstColumn(block.rows(), x);

        executeNumLeftType<typename EnumType::FieldType>(block, result,
            left_is_num ? column_number : enum_col.get(),
            left_is_num ? enum_col.get() : column_number);
    }

    void executeTuple(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        /** Сравнивать кортежи будем лексикографически. Это делается следующим образом:
          * x == y : x1 == y1 && x2 == y2 ...
          * x != y : x1 != y1 || x2 != y2 ...
          *
          * x < y:   x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn < yn))
          * x > y:   x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn > yn))
          * x <= y:  x1 < y1 || (x1 == y1 && (x2 < y2 || (x2 == y2 ... && xn <= yn))
          *
           * Рекурсивная запись:
          * x <= y:  x1 < y1 || (x1 == y1 && x_tail <= y_tail)
          *
          * x >= y:  x1 > y1 || (x1 == y1 && (x2 > y2 || (x2 == y2 ... && xn >= yn))
          */

        auto x_const = typeid_cast<const ColumnConstTuple *>(c0);
        auto y_const = typeid_cast<const ColumnConstTuple *>(c1);

        ColumnPtr x_tuple_of_consts;
        ColumnPtr y_tuple_of_consts;

        auto x = static_cast<const ColumnTuple *>(c0);
        auto y = static_cast<const ColumnTuple *>(c1);

        if (x_const)
        {
            x_tuple_of_consts = x_const->convertToTupleOfConstants();
            x = static_cast<const ColumnTuple *>(x_tuple_of_consts.get());
        }

        if (y_const)
        {
            y_tuple_of_consts = y_const->convertToTupleOfConstants();
            y = static_cast<const ColumnTuple *>(y_tuple_of_consts.get());
        }

        const size_t tuple_size = x->getData().columns();

        if (0 == tuple_size)
            throw Exception("Comparison of zero-sized tuples is not implemented.", ErrorCodes::NOT_IMPLEMENTED);

        executeTupleImpl(block, result, x, y, tuple_size);
    }

    void executeTupleImpl(Block & block, size_t result, const ColumnTuple * x, const ColumnTuple * y, size_t tuple_size);

    template <typename ComparisonFunction, typename ConvolutionFunction>
    void executeTupleEqualityImpl(Block & block, size_t result, const ColumnTuple * x, const ColumnTuple * y, size_t tuple_size)
    {
        ComparisonFunction func_compare;
        ConvolutionFunction func_convolution;

        Block tmp_block;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x->getData().safeGetByPosition(i));
            tmp_block.insert(y->getData().safeGetByPosition(i));

            /// Сравнение элементов.
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_compare.execute(tmp_block, {i * 3, i * 3 + 1}, i * 3 + 2);
        }

        /// Логическая свёртка.
        tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

        ColumnNumbers convolution_args(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            convolution_args[i] = i * 3 + 2;

        func_convolution.execute(tmp_block, convolution_args, tuple_size * 3);
        block.safeGetByPosition(result).column = tmp_block.safeGetByPosition(tuple_size * 3).column;
    }

    template <typename HeadComparisonFunction, typename TailComparisonFunction>
    void executeTupleLessGreaterImpl(Block & block, size_t result, const ColumnTuple * x, const ColumnTuple * y, size_t tuple_size)
    {
        HeadComparisonFunction func_compare_head;
        TailComparisonFunction func_compare_tail;
        FunctionAnd func_and;
        FunctionOr func_or;
        FunctionComparison<EqualsOp, NameEquals> func_equals;

        Block tmp_block;

        /// Попарное сравнение на неравенство всех элементов; на равенство всех элементов кроме последнего.
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tmp_block.insert(x->getData().safeGetByPosition(i));
            tmp_block.insert(y->getData().safeGetByPosition(i));

            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });

            if (i + 1 != tuple_size)
            {
                func_compare_head.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2);

                tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
                func_equals.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 3);

            }
            else
                func_compare_tail.execute(tmp_block, {i * 4, i * 4 + 1}, i * 4 + 2);
        }

        /// Комбинирование. Сложный код - сделайте рисунок. Можно заменить на рекурсивное сравнение кортежей.
        size_t i = tuple_size - 1;
        while (i > 0)
        {
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_and.execute(tmp_block, { tmp_block.columns() - 2, (i - 1) * 4 + 3 },  tmp_block.columns() - 1);
            tmp_block.insert({ nullptr, std::make_shared<DataTypeUInt8>(), "" });
            func_or.execute(tmp_block, { tmp_block.columns() - 2, (i - 1) * 4 + 2 },  tmp_block.columns() - 1);
            --i;
        }

        block.safeGetByPosition(result).column = tmp_block.safeGetByPosition(tmp_block.columns() - 1).column;
    }

    void executeGeneric(Block & block, size_t result, const IColumn * c0, const IColumn * c1)
    {
        bool c0_const = c0->isConst();
        bool c1_const = c1->isConst();

        if (c0_const && c1_const)
        {
            auto c_res = std::make_shared<ColumnConstUInt8>(c0->size(), 0);
            block.safeGetByPosition(result).column = c_res;
            GenericComparisonImpl<Op<int, int>>::constant_constant(*c0, *c1, c_res->getData());
        }
        else
        {
            auto c_res = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = c_res;
            ColumnUInt8::Container_t & vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constant_vector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vector_constant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vector_vector(*c0, *c1, vec_res);
        }
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    /// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        bool left_is_date = false;
        bool left_is_date_time = false;
        bool left_is_enum8 = false;
        bool left_is_enum16 = false;
        bool left_is_string = false;
        bool left_is_fixed_string = false;
        const DataTypeTuple * left_tuple = nullptr;

        false
            || (left_is_date         = typeid_cast<const DataTypeDate *>(arguments[0].get()))
            || (left_is_date_time     = typeid_cast<const DataTypeDateTime *>(arguments[0].get()))
            || (left_is_enum8         = typeid_cast<const DataTypeEnum8 *>(arguments[0].get()))
            || (left_is_enum16         = typeid_cast<const DataTypeEnum16 *>(arguments[0].get()))
            || (left_is_string         = typeid_cast<const DataTypeString *>(arguments[0].get()))
            || (left_is_fixed_string = typeid_cast<const DataTypeFixedString *>(arguments[0].get()))
            || (left_tuple             = typeid_cast<const DataTypeTuple *>(arguments[0].get()));

        const bool left_is_enum = left_is_enum8 || left_is_enum16;

        bool right_is_date = false;
        bool right_is_date_time = false;
        bool right_is_enum8 = false;
        bool right_is_enum16 = false;
        bool right_is_string = false;
        bool right_is_fixed_string = false;
        const DataTypeTuple * right_tuple = nullptr;

        false
            || (right_is_date         = typeid_cast<const DataTypeDate *>(arguments[1].get()))
            || (right_is_date_time     = typeid_cast<const DataTypeDateTime *>(arguments[1].get()))
            || (right_is_enum8         = typeid_cast<const DataTypeEnum8 *>(arguments[1].get()))
            || (right_is_enum16     = typeid_cast<const DataTypeEnum16 *>(arguments[1].get()))
            || (right_is_string     = typeid_cast<const DataTypeString *>(arguments[1].get()))
            || (right_is_fixed_string = typeid_cast<const DataTypeFixedString *>(arguments[1].get()))
            || (right_tuple         = typeid_cast<const DataTypeTuple *>(arguments[1].get()));

        const bool right_is_enum = right_is_enum8 || right_is_enum16;

        if (!(    (arguments[0]->behavesAsNumber() && arguments[1]->behavesAsNumber() && !(left_is_enum ^ right_is_enum))
            ||    ((left_is_string || left_is_fixed_string) && (right_is_string || right_is_fixed_string))
            ||    (left_is_date && right_is_date)
            ||    (left_is_date && right_is_string)    /// Можно сравнивать дату, дату-с-временем и перечисление с константной строкой.
            ||    (left_is_string && right_is_date)
            ||    (left_is_date_time && right_is_date_time)
            ||    (left_is_date_time && right_is_string)
            ||    (left_is_string && right_is_date_time)
            ||    (left_is_date_time && right_is_date_time)
            ||    (left_is_date_time && right_is_string)
            ||    (left_is_string && right_is_date_time)
            ||    (left_is_enum && right_is_enum && arguments[0]->getName() == arguments[1]->getName()) /// only equivalent enum type values can be compared against
            ||    (left_is_enum && right_is_string)
            ||    (left_is_string && right_is_enum)
            ||    (left_tuple && right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
            ||  (arguments[0]->equals(*arguments[1]))))
            throw Exception("Illegal types of arguments (" + arguments[0]->getName() + ", " + arguments[1]->getName() + ")"
                " of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (left_tuple && right_tuple)
        {
            size_t size = left_tuple->getElements().size();
            for (size_t i = 0; i < size; ++i)
                getReturnType({ left_tuple->getElements()[i], right_tuple->getElements()[i] });
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const auto & col_with_type_and_name_left = block.safeGetByPosition(arguments[0]);
        const auto & col_with_type_and_name_right = block.safeGetByPosition(arguments[1]);
        const IColumn * col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn * col_right_untyped = col_with_type_and_name_right.column.get();

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        if (left_is_num && right_is_num)
        {
            if (!(    executeNumLeftType<UInt8>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<UInt16>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<UInt32>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<UInt64>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Int8>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Int16>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Int32>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Int64>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Float32>(block, result, col_left_untyped, col_right_untyped)
                ||    executeNumLeftType<Float64>(block, result, col_left_untyped, col_right_untyped)))
                throw Exception("Illegal column " + col_left_untyped->getName()
                    + " of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (typeid_cast<const DataTypeTuple *>(col_with_type_and_name_left.type.get()))
            executeTuple(block, result, col_left_untyped, col_right_untyped);
        else if (!left_is_num && !right_is_num && executeString(block, result, col_left_untyped, col_right_untyped))
            ;
        else if (col_with_type_and_name_left.type->equals(*col_with_type_and_name_right.type))
            executeGeneric(block, result, col_left_untyped, col_right_untyped);
        else
            executeDateOrDateTimeOrEnumWithConstString(
                block, result, col_left_untyped, col_right_untyped,
                col_with_type_and_name_left.type, col_with_type_and_name_right.type,
                left_is_num, right_is_num);
}
};


using FunctionEquals = FunctionComparison<EqualsOp,             NameEquals>            ;
using FunctionNotEquals = FunctionComparison<NotEqualsOp,         NameNotEquals>        ;
using FunctionLess = FunctionComparison<LessOp,                 NameLess>            ;
using FunctionGreater = FunctionComparison<GreaterOp,             NameGreater>        ;
using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp,         NameLessOrEquals>    ;
using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp,    NameGreaterOrEquals>;

}
