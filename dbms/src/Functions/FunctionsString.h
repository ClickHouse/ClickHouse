#pragma once

#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>


namespace DB
{
/** Функции работы со строками:
  *
  * length, empty, notEmpty,
  * concat, substring, lower, upper, reverse
  * lengthUTF8, substringUTF8, lowerUTF8, upperUTF8, reverseUTF8
  *
  * s                -> UInt8:    empty, notEmpty
  * s                -> UInt64:   length, lengthUTF8
  * s                -> s:        lower, upper, lowerUTF8, upperUTF8, reverse, reverseUTF8
  * s, s             -> s:        concat
  * s, c1, c2        -> s:        substring, substringUTF8
  * s, c1, c2, s2    -> s:        replace, replaceUTF8
  *
  * Функции поиска строк и регулярных выражений расположены отдельно.
  * Функции работы с URL расположены отдельно.
  * Функции кодирования строк, конвертации в другие типы расположены отдельно.
  *
  * Функции length, empty, notEmpty, reverse также работают с массивами.
  */


/// xor or do nothing
template <bool>
UInt8 xor_or_identity(const UInt8 c, const int mask)
{
    return c ^ mask;
};
template <>
inline UInt8 xor_or_identity<false>(const UInt8 c, const int)
{
    return c;
}

/// It is caller's responsibility to ensure the presence of a valid cyrillic sequence in array
template <bool to_lower>
inline void UTF8CyrillicToCase(const UInt8 *& src, const UInt8 * const src_end, UInt8 *& dst)
{
    if (src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
    {
        /// ЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏ
        *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<to_lower>(*src++, 0x10);
    }
    else if (src[0] == 0xD1u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
    {
        /// ѐёђѓєѕіїјљњћќѝўџ
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x10);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0x90u && src[1] <= 0x9Fu))
    {
        /// А-П
        *dst++ = *src++;
        *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0xB0u && src[1] <= 0xBFu))
    {
        /// а-п
        *dst++ = *src++;
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD0u && (src[1] >= 0xA0u && src[1] <= 0xAFu))
    {
        /// Р-Я
        *dst++ = xor_or_identity<to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<to_lower>(*src++, 0x20);
    }
    else if (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
    {
        /// р-я
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
        *dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
    }
}


/** Если строка содержит текст в кодировке UTF-8 - перевести его в нижний (верхний) регистр.
  * Замечание: предполагается, что после перевода символа в другой регистр,
  *  длина его мультибайтовой последовательности в UTF-8 не меняется.
  * Иначе - поведение не определено.
  */
template <char not_case_lower_bound,
    char not_case_upper_bound,
    int to_case(int),
    void cyrillic_to_case(const UInt8 *&, const UInt8 *, UInt8 *&)>
struct LowerUpperUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets_t & offsets,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets_t & res_offsets);

    static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

    static void constant(const std::string & data, std::string & res_data);

    /** Converts a single code point starting at `src` to desired case, storing result starting at `dst`.
     *    `src` and `dst` are incremented by corresponding sequence lengths. */
    static void toCase(const UInt8 *& src, const UInt8 * const src_end, UInt8 *& dst);

private:
    static constexpr auto ascii_upper_bound = '\x7f';
    static constexpr auto flip_case_mask = 'A' ^ 'a';

    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst);
};


template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionStringToString>();
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
        return is_injective;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0]->clone();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
        if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
        {
            std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = col_res;
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
        }
        else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(&*column))
        {
            auto col_res = std::make_shared<ColumnFixedString>(col->getN());
            block.safeGetByPosition(result).column = col_res;
            Impl::vector_fixed(col->getChars(), col->getN(), col_res->getChars());
        }
        else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
        {
            String res;
            Impl::constant(col->getData(), res);
            auto col_res = std::make_shared<ColumnConstString>(col->size(), res);
            block.safeGetByPosition(result).column = col_res;
        }
        else
            throw Exception(
                "Illegal column " + block.safeGetByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct NameLowerUTF8
{
    static constexpr auto name = "lowerUTF8";
};
struct NameUpperUTF8
{
    static constexpr auto name = "upperUTF8";
};


typedef FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>, NameLowerUTF8>
    FunctionLowerUTF8;
typedef FunctionStringToString<LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>, NameUpperUTF8>
    FunctionUpperUTF8;
}
