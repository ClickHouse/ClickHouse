#include <Functions/FunctionsString.h>

#include <ext/range.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <IO/WriteHelpers.h>
#include <Common/UTF8Helpers.h>


#if __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
    extern const int LOGICAL_ERROR;
}

using namespace GatherUtils;

template <bool negative = false>
struct EmptyImpl
{
    /// If the function will return constant value for FixedString data type.
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 1;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i] + 1;
        }
    }

    /// Only make sense if is_fixed_to_constant.
    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt8 & /*res*/)
    {
        throw Exception("Logical error: 'vector_fixed_to_constant method' is called", ErrorCodes::LOGICAL_ERROR);
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt8> & res)
    {
        std::vector<char> empty_chars(n);
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            res[i] = negative ^ (0 == memcmp(&data[i * size], empty_chars.data(), n));
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt8> & res)
    {
        size_t size = offsets.size();
        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = negative ^ (offsets[i] == prev_offset);
            prev_offset = offsets[i];
        }
    }
};


/** Calculates the length of a string in bytes.
  */
struct LengthImpl
{
    static constexpr auto is_fixed_to_constant = true;

    static void vector(const ColumnString::Chars_t & /*data*/, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i] - 1) : (offsets[i] - 1 - offsets[i - 1]);
    }

    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t n, UInt64 & res)
    {
        res = n;
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & /*data*/, size_t /*n*/, PaddedPODArray<UInt64> & /*res*/)
    {
    }

    static void array(const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();
        for (size_t i = 0; i < size; ++i)
            res[i] = i == 0 ? (offsets[i]) : (offsets[i] - offsets[i - 1]);
    }
};


/** If the string is UTF-8 encoded text, it returns the length of the text in code points.
  * (not in characters: the length of the text "ё" can be either 1 or 2, depending on the normalization)
 * (not in characters: the length of the text "" can be either 1 or 2, depending on the normalization)
  * Otherwise, the behavior is undefined.
  */
struct LengthUTF8Impl
{
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed_to_constant(const ColumnString::Chars_t & /*data*/, size_t /*n*/, UInt64 & /*res*/)
    {
    }

    static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[i * n], n);
        }
    }

    static void array(const ColumnString::Offsets &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function lengthUTF8 to Array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vector_fixed(const ColumnString::Chars_t & data, size_t /*n*/, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        const auto flip_case_mask = 'A' ^ 'a';

#if __SSE2__
        const auto bytes_sse = sizeof(__m128i);
        const auto src_end_sse = src_end - (src_end - src) % bytes_sse;

        const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
        const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
        const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

        for (; src < src_end_sse; src += bytes_sse, dst += bytes_sse)
        {
            /// load 16 sequential 8-bit characters
            const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

            /// find which 8-bit sequences belong to range [case_lower_bound, case_upper_bound]
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));

            /// keep `flip_case_mask` only where necessary, zero out elsewhere
            const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

            /// flip case by applying calculated mask
            const auto cased_chars = _mm_xor_si128(chars, xor_mask);

            /// store result back to destination
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
        }
#endif

        for (; src < src_end; ++src, ++dst)
            if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
                *dst = *src ^ flip_case_mask;
            else
                *dst = *src;
    }
};

/** Expands the string in bytes.
  */
struct ReverseImpl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (size_t j = prev_offset; j < offsets[i] - 1; ++j)
                res_data[j] = data[offsets[i] + prev_offset - 2 - j];
            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data)
    {
        res_data.resize(data.size());
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
            for (size_t j = i * n; j < (i + 1) * n; ++j)
                res_data[j] = data[(i * 2 + 1) * n - j - 1];
    }
};


/** Expands the sequence of code points in a UTF-8 encoded string.
  * The result may not match the expected result, because modifying code points (for example, diacritics) may be applied to another symbols.
  * If the string is not encoded in UTF-8, then the behavior is undefined.
  */
struct ReverseUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            while (j < offsets[i] - 1)
            {
                if (data[j] < 0xBF)
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
                else if (data[j] < 0xE0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 1], &data[j], 2);
                    j += 2;
                }
                else if (data[j] < 0xF0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 2], &data[j], 3);
                    j += 3;
                }
                else
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
            }

            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    static void vector_fixed(const ColumnString::Chars_t &, size_t, ColumnString::Chars_t &)
    {
        throw Exception("Cannot apply function reverseUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector(const ColumnString::Chars_t & data,
    const IColumn::Offsets & offsets,
    ColumnString::Chars_t & res_data,
    IColumn::Offsets & res_offsets)
{
    res_data.resize(data.size());
    res_offsets.assign(offsets);
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::vector_fixed(
    const ColumnString::Chars_t & data, size_t /*n*/, ColumnString::Chars_t & res_data)
{
    res_data.resize(data.size());
    array(data.data(), data.data() + data.size(), res_data.data());
}

template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::constant(
    const std::string & data, std::string & res_data)
{
    res_data.resize(data.size());
    array(reinterpret_cast<const UInt8 *>(data.data()),
        reinterpret_cast<const UInt8 *>(data.data() + data.size()),
        reinterpret_cast<UInt8 *>(&res_data[0]));
}

template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::toCase(
    const UInt8 *& src, const UInt8 * src_end, UInt8 *& dst)
{
    if (src[0] <= ascii_upper_bound)
    {
        if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
            *dst++ = *src++ ^ flip_case_mask;
        else
            *dst++ = *src++;
    }
    else if (src + 1 < src_end
        && ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) || (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
    {
        cyrillic_to_case(src, dst);
    }
    else if (src + 1 < src_end && src[0] == 0xC2u)
    {
        /// Punctuation U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else if (src + 2 < src_end && src[0] == 0xE2u)
    {
        /// Characters U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
         *dst++ = *src++;
        *dst++ = *src++;
        *dst++ = *src++;
    }
    else
    {
        static const Poco::UTF8Encoding utf8;

        if (const auto chars = utf8.convert(to_case(utf8.convert(src)), dst, src_end - src))
            src += chars, dst += chars;
        else
            ++src, ++dst;
    }
}

template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, UInt8 *&)>
void LowerUpperUTF8Impl<not_case_lower_bound, not_case_upper_bound, to_case, cyrillic_to_case>::array(
    const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
{
#if __SSE2__
    const auto bytes_sse = sizeof(__m128i);
    auto src_end_sse = src + (src_end - src) / bytes_sse * bytes_sse;

    /// SSE2 packed comparison operate on signed types, hence compare (c < 0) instead of (c > 0x7f)
    const auto v_zero = _mm_setzero_si128();
    const auto v_not_case_lower_bound = _mm_set1_epi8(not_case_lower_bound - 1);
    const auto v_not_case_upper_bound = _mm_set1_epi8(not_case_upper_bound + 1);
    const auto v_flip_case_mask = _mm_set1_epi8(flip_case_mask);

    while (src < src_end_sse)
    {
        const auto chars = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

        /// check for ASCII
        const auto is_not_ascii = _mm_cmplt_epi8(chars, v_zero);
        const auto mask_is_not_ascii = _mm_movemask_epi8(is_not_ascii);

        /// ASCII
        if (mask_is_not_ascii == 0)
        {
            const auto is_not_case
                = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound), _mm_cmplt_epi8(chars, v_not_case_upper_bound));
            const auto mask_is_not_case = _mm_movemask_epi8(is_not_case);

            /// everything in correct case ASCII
            if (mask_is_not_case == 0)
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), chars);
            else
            {
                /// ASCII in mixed case
                /// keep `flip_case_mask` only where necessary, zero out elsewhere
                const auto xor_mask = _mm_and_si128(v_flip_case_mask, is_not_case);

                /// flip case by applying calculated mask
                const auto cased_chars = _mm_xor_si128(chars, xor_mask);

                /// store result back to destination
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), cased_chars);
            }

            src += bytes_sse, dst += bytes_sse;
        }
        else
        {
            /// UTF-8
            const auto expected_end = src + bytes_sse;

            while (src < expected_end)
                toCase(src, src_end, dst);

            /// adjust src_end_sse by pushing it forward or backward
            const auto diff = src - expected_end;
            if (diff != 0)
            {
                if (src_end_sse + diff < src_end)
                    src_end_sse += diff;
                else
                    src_end_sse -= bytes_sse - diff;
            }
        }
    }
#endif
    /// handle remaining symbols
    while (src < src_end)
        toCase(src, src_end, dst);
}


/** If the string is encoded in UTF-8, then it selects a substring of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct SubstringUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        size_t start,
        size_t length,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            ColumnString::Offset pos = 1;
            ColumnString::Offset bytes_start = 0;
            ColumnString::Offset bytes_length = 0;
            while (j < offsets[i] - 1)
            {
                if (pos == start)
                    bytes_start = j - prev_offset + 1;

                if (data[j] < 0xBF)
                    j += 1;
                else if (data[j] < 0xE0)
                    j += 2;
                else if (data[j] < 0xF0)
                    j += 3;
                else
                    j += 1;

                if (pos >= start && pos < start + length)
                    bytes_length = j - prev_offset + 1 - bytes_start;
                else if (pos >= start + length)
                    break;

                ++pos;
            }

            if (bytes_start == 0)
            {
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = 0;
                ++res_offset;
            }
            else
            {
                size_t bytes_to_copy = std::min(offsets[i] - prev_offset - bytes_start, bytes_length);
                res_data.resize(res_data.size() + bytes_to_copy + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + bytes_start - 1], bytes_to_copy);
                res_offset += bytes_to_copy + 1;
                res_data[res_offset - 1] = 0;
            }
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};


template <typename Impl, typename Name, typename ResultType>
class FunctionStringOrArrayToT : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringOrArrayToT>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (Impl::is_fixed_to_constant)
            {
                ResultType res = 0;
                Impl::vector_fixed_to_constant(col->getChars(), col->getN(), res);

                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(col->size(), toField(res));
            }
            else
            {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->size());
                Impl::vector_fixed_to_vector(col->getChars(), col->getN(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
            }
        }
        else if (const ColumnArray * col = checkAndGetColumn<ColumnArray>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::array(col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Also works with arrays.
class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString()
            && !checkDataType<DataTypeArray>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col->getN());
            ReverseImpl::vector_fixed(col->getChars(), col->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else if (checkColumn<ColumnArray>(column.get()))
        {
            FunctionArrayReverse().execute(block, arguments, result, input_rows_count);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    ConcatImpl(const Context & context) : context(context) {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<ConcatImpl>(context);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) override
    {
        return is_injective;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(arguments[0].get()))
            return FunctionArrayConcat(context).getReturnTypeImpl(arguments);

        if (arguments.size() < 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!arg->isStringOrFixedString())
                throw Exception{"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        if (!is_injective && !arguments.empty() && checkDataType<DataTypeArray>(block.getByPosition(arguments[0]).type.get()))
            return FunctionArrayConcat(context).executeImpl(block, arguments, result, input_rows_count);

        if (arguments.size() == 2)
            executeBinary(block, arguments, result, input_rows_count);
        else
            executeNAry(block, arguments, result, input_rows_count);
    }

private:
    const Context & context;

    void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result, size_t input_rows_count)
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else
        {
            /// Fallback: use generic implementation for not very important cases.
            executeNAry(block, arguments, result, input_rows_count);
            return;
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeNAry(Block & block, const ColumnNumbers & arguments, const size_t result, size_t input_rows_count)
    {
        size_t num_sources = arguments.size();
        StringSources sources(num_sources);

        for (size_t i = 0; i < num_sources; ++i)
            sources[i] = createDynamicStringSource(*block.getByPosition(arguments[i]).column);

        auto c_res = ColumnString::create();
        concat(sources, StringSink(*c_res, input_rows_count));
        block.getByPosition(result).column = std::move(c_res);
    }
};


class FunctionSubstring : public IFunction
{
public:
    static constexpr auto name = "substring";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstring>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(number_of_arguments) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber())
            throw Exception("Illegal type " + arguments[1]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (number_of_arguments == 3 && !arguments[2]->isNumber())
            throw Exception("Illegal type " + arguments[2]->getName()
                    + " of second argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    template <typename Source>
    void executeForSource(const ColumnPtr & column_start, const ColumnPtr & column_length,
                              const ColumnConst * column_start_const, const ColumnConst * column_length_const,
                              Int64 start_value, Int64 length_value, Block & block, size_t result, Source && source,
                              size_t input_rows_count)
    {
       auto col_res = ColumnString::create();

        if (!column_length)
        {
            if (column_start_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), start_value - 1);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetUnbounded(source, StringSink(*col_res, input_rows_count), -start_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetUnbounded(source, StringSink(*col_res, input_rows_count), *column_start);
        }
        else
        {
            if (column_start_const && column_length_const)
            {
                if (start_value > 0)
                    sliceFromLeftConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), start_value - 1, length_value);
                else if (start_value < 0)
                    sliceFromRightConstantOffsetBounded(source, StringSink(*col_res, input_rows_count), -start_value, length_value);
                else
                    throw Exception("Indices in strings are 1-based", ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX);
            }
            else
                sliceDynamicOffsetBounded(source, StringSink(*col_res, input_rows_count), *column_start, *column_length);
        }

        block.getByPosition(result).column = std::move(col_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t number_of_arguments = arguments.size();

        ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        ColumnPtr column_length;

        if (number_of_arguments == 3)
            column_length = block.getByPosition(arguments[2]).column;

        const ColumnConst * column_start_const = checkAndGetColumn<ColumnConst>(column_start.get());
        const ColumnConst * column_length_const = nullptr;

        if (number_of_arguments == 3)
            column_length_const = checkAndGetColumn<ColumnConst>(column_length.get());

        Int64 start_value = 0;
        Int64 length_value = 0;

        if (column_start_const)
        {
            start_value = column_start_const->getInt(0);
        }
        if (column_length_const)
        {
            length_value = column_length_const->getInt(0);
            if (length_value < 0)
                throw Exception("Third argument provided for function substring could not be negative.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, StringSource(*col), input_rows_count);
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, FixedStringSource(*col), input_rows_count);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, ConstSource<StringSource>(*col), input_rows_count);
        else if (const ColumnConst * col = checkAndGetColumnConst<ColumnFixedString>(column_string.get()))
            executeForSource(column_start, column_length, column_start_const, column_length_const, start_value,
                             length_value, block, result, ConstSource<FixedStringSource>(*col), input_rows_count);
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionSubstringUTF8 : public IFunction
{
public:
    static constexpr auto name = "substringUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstringUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isNumber() || !arguments[2]->isNumber())
            throw Exception("Illegal type " + (arguments[1]->isNumber() ? arguments[2]->getName() : arguments[1]->getName())
                    + " of argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        const ColumnPtr column_length = block.getByPosition(arguments[2]).column;

        if (!column_start->isColumnConst() || !column_length->isColumnConst())
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.");

        Field start_field = (*block.getByPosition(arguments[1]).column)[0];
        Field length_field = (*block.getByPosition(arguments[2]).column)[0];

        if (start_field.getType() != Field::Types::UInt64 || length_field.getType() != Field::Types::UInt64)
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be non-negative and must have UInt type.");

        UInt64 start = start_field.get<UInt64>();
        UInt64 length = length_field.get<UInt64>();

        if (start == 0)
            throw Exception("Second argument of function substring must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        /// Otherwise may lead to overflow and pass bounds check inside inner loop.
        if (start >= 0x8000000000000000ULL || length >= 0x8000000000000000ULL)
            throw Exception("Too large values of 2nd or 3rd argument provided for function substring.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
        {
            auto col_res = ColumnString::create();
            SubstringUTF8Impl::vector(col->getChars(), col->getOffsets(), start, length, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
    static constexpr auto name = "appendTrailingCharIfAbsent";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionAppendTrailingCharIfAbsent>();
    }

    String getName() const override
    {
        return name;
    }


private:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception{"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[1]->isString())
            throw Exception{"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto & column = block.getByPosition(arguments[0]).column;
        const auto & column_char = block.getByPosition(arguments[1]).column;

        if (!checkColumnConst<ColumnString>(column_char.get()))
            throw Exception{"Second argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_COLUMN};

        String trailing_char_str = static_cast<const ColumnConst &>(*column_char).getValue<String>();

        if (trailing_char_str.size() != 1)
            throw Exception{"Second argument of function " + getName() + " must be a one-character string", ErrorCodes::BAD_ARGUMENTS};

        if (const auto col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            const auto & src_data = col->getChars();
            const auto & src_offsets = col->getOffsets();

            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();

            const auto size = src_offsets.size();
            dst_data.resize(src_data.size() + size);
            dst_offsets.resize(size);

            ColumnString::Offset src_offset{};
            ColumnString::Offset dst_offset{};

            for (const auto i : ext::range(0, size))
            {
                const auto src_length = src_offsets[i] - src_offset;
                memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
                src_offset = src_offsets[i];
                dst_offset += src_length;

                if (src_length > 1 && dst_data[dst_offset - 2] != trailing_char_str.front())
                {
                    dst_data[dst_offset - 1] = trailing_char_str.front();
                    dst_data[dst_offset] = 0;
                    ++dst_offset;
                }

                dst_offsets[i] = dst_offset;
            }

            dst_data.resize_assume_reserved(dst_offset);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};


struct NameStartsWith
{
    static constexpr auto name = "startsWith";
};
struct NameEndsWith
{
    static constexpr auto name = "endsWith";
};

template <typename Name>
class FunctionStartsEndsWith : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStartsEndsWith>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isStringOrFixedString())
            throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * haystack_column = block.getByPosition(arguments[0]).column.get();
        const IColumn * needle_column = block.getByPosition(arguments[1]).column.get();

        auto col_res = ColumnVector<UInt8>::create();
        typename ColumnVector<UInt8>::Container & vec_res = col_res->getData();

        vec_res.resize(input_rows_count);

        if (const ColumnString * haystack = checkAndGetColumn<ColumnString>(haystack_column))
            dispatch<StringSource>(StringSource(*haystack), needle_column, vec_res);
        else if (const ColumnFixedString * haystack = checkAndGetColumn<ColumnFixedString>(haystack_column))
            dispatch<FixedStringSource>(FixedStringSource(*haystack), needle_column, vec_res);
        else if (const ColumnConst * haystack = checkAndGetColumnConst<ColumnString>(haystack_column))
            dispatch<ConstSource<StringSource>>(ConstSource<StringSource>(*haystack), needle_column, vec_res);
        else if (const ColumnConst * haystack = checkAndGetColumnConst<ColumnFixedString>(haystack_column))
            dispatch<ConstSource<FixedStringSource>>(ConstSource<FixedStringSource>(*haystack), needle_column, vec_res);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    template <typename HaystackSource>
    void dispatch(HaystackSource haystack_source, const IColumn * needle_column, PaddedPODArray<UInt8> & res_data) const
    {
        if (const ColumnString * needle = checkAndGetColumn<ColumnString>(needle_column))
            execute<HaystackSource, StringSource>(haystack_source, StringSource(*needle), res_data);
        else if (const ColumnFixedString * needle = checkAndGetColumn<ColumnFixedString>(needle_column))
            execute<HaystackSource, FixedStringSource>(haystack_source, FixedStringSource(*needle), res_data);
        else if (const ColumnConst * needle = checkAndGetColumnConst<ColumnString>(needle_column))
            execute<HaystackSource, ConstSource<StringSource>>(haystack_source, ConstSource<StringSource>(*needle), res_data);
        else if (const ColumnConst * needle = checkAndGetColumnConst<ColumnFixedString>(needle_column))
            execute<HaystackSource, ConstSource<FixedStringSource>>(haystack_source, ConstSource<FixedStringSource>(*needle), res_data);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename HaystackSource, typename NeedleSource>
    static void execute(HaystackSource haystack_source, NeedleSource needle_source, PaddedPODArray<UInt8> & res_data)
    {
        size_t row_num = 0;

        while (!haystack_source.isEnd())
        {
            auto haystack = haystack_source.getWhole();
            auto needle = needle_source.getWhole();

            if (needle.size > haystack.size)
            {
                res_data[row_num] = false;
            }
            else
            {
                if constexpr (std::is_same_v<Name, NameStartsWith>)
                {
                    res_data[row_num] = StringRef(haystack.data, needle.size) == StringRef(needle.data, needle.size);
                }
                else    /// endsWith
                {
                    res_data[row_num] = StringRef(haystack.data + haystack.size - needle.size, needle.size) == StringRef(needle.data, needle.size);
                }
            }

            haystack_source.next();
            needle_source.next();
            ++row_num;
        }
    }
};


struct NameEmpty
{
    static constexpr auto name = "empty";
};
struct NameNotEmpty
{
    static constexpr auto name = "notEmpty";
};
struct NameLength
{
    static constexpr auto name = "length";
};
struct NameLengthUTF8
{
    static constexpr auto name = "lengthUTF8";
};
struct NameLower
{
    static constexpr auto name = "lower";
};
struct NameUpper
{
    static constexpr auto name = "upper";
};
struct NameReverseUTF8
{
    static constexpr auto name = "reverseUTF8";
};
struct NameConcat
{
    static constexpr auto name = "concat";
};
struct NameConcatAssumeInjective
{
    static constexpr auto name = "concatAssumeInjective";
};


using FunctionEmpty = FunctionStringOrArrayToT<EmptyImpl<false>, NameEmpty, UInt8>;
using FunctionNotEmpty = FunctionStringOrArrayToT<EmptyImpl<true>, NameNotEmpty, UInt8>;
using FunctionLength = FunctionStringOrArrayToT<LengthImpl, NameLength, UInt64>;
using FunctionLengthUTF8 = FunctionStringOrArrayToT<LengthUTF8Impl, NameLengthUTF8, UInt64>;
using FunctionLower = FunctionStringToString<LowerUpperImpl<'A', 'Z'>, NameLower>;
using FunctionUpper = FunctionStringToString<LowerUpperImpl<'a', 'z'>, NameUpper>;
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;
using FunctionConcat = ConcatImpl<NameConcat, false>;
using FunctionConcatAssumeInjective = ConcatImpl<NameConcatAssumeInjective, true>;
using FunctionStartsWith = FunctionStartsEndsWith<NameStartsWith>;
using FunctionEndsWith = FunctionStartsEndsWith<NameEndsWith>;


void registerFunctionsString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEmpty>();
    factory.registerFunction<FunctionNotEmpty>();
    factory.registerFunction<FunctionLength>();
    factory.registerFunction<FunctionLengthUTF8>();
    factory.registerFunction<FunctionLower>();
    factory.registerFunction<FunctionUpper>();
    factory.registerFunction<FunctionLowerUTF8>();
    factory.registerFunction<FunctionUpperUTF8>();
    factory.registerFunction<FunctionReverse>();
    factory.registerFunction<FunctionReverseUTF8>();
    factory.registerFunction<FunctionConcat>();
    factory.registerFunction<FunctionConcatAssumeInjective>();
    factory.registerFunction<FunctionSubstring>();
    factory.registerFunction<FunctionSubstringUTF8>();
    factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
    factory.registerFunction<FunctionStartsWith>();
    factory.registerFunction<FunctionEndsWith>();
}
}
