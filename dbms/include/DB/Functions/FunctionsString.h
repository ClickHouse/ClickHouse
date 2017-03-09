#pragma once

#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>

#include <DB/Core/FieldVisitors.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>
#include <ext/range.hpp>

#if __SSE2__
	#include <emmintrin.h>
#endif


namespace DB
{

/** Функции работы со строками:
  *
  * length, empty, notEmpty,
  * concat, substring, lower, upper, reverse
  * lengthUTF8, substringUTF8, lowerUTF8, upperUTF8, reverseUTF8
  *
  * s				-> UInt8:	empty, notEmpty
  * s 				-> UInt64: 	length, lengthUTF8
  * s 				-> s:		lower, upper, lowerUTF8, upperUTF8, reverse, reverseUTF8
  * s, s 			-> s: 		concat
  * s, c1, c2 		-> s:		substring, substringUTF8
  * s, c1, c2, s2	-> s:		replace, replaceUTF8
  *
  * Функции поиска строк и регулярных выражений расположены отдельно.
  * Функции работы с URL расположены отдельно.
  * Функции кодирования строк, конвертации в другие типы расположены отдельно.
  *
  * Функции length, empty, notEmpty, reverse также работают с массивами.
  */


/// xor or do nothing
template <bool> UInt8 xor_or_identity(const UInt8 c, const int mask) { return c ^ mask; };
template <> inline UInt8 xor_or_identity<false>(const UInt8 c, const int) { return c; }

/// It is caller's responsibility to ensure the presence of a valid cyrillic sequence in array
template <bool to_lower>
inline void UTF8CyrillicToCase(const UInt8 * & src, const UInt8 * const src_end, UInt8 * & dst)
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
		///	Р-Я
		*dst++ = xor_or_identity<to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<to_lower>(*src++, 0x20);
	}
	else if (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x8Fu))
	{
		/// р-я
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x1);
		*dst++ = xor_or_identity<!to_lower>(*src++, 0x20);
	}
};


/** Если строка содержит текст в кодировке UTF-8 - перевести его в нижний (верхний) регистр.
  * Замечание: предполагается, что после перевода символа в другой регистр,
  *  длина его мультибайтовой последовательности в UTF-8 не меняется.
  * Иначе - поведение не определено.
  */
template <char not_case_lower_bound, char not_case_upper_bound,
	int to_case(int), void cyrillic_to_case(const UInt8 * &, const UInt8 *, UInt8 * &)>
struct LowerUpperUTF8Impl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
					   ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		res_data.resize(data.size());
		res_offsets.assign(offsets);
		array(data.data(), data.data() + data.size(), res_data.data());
	}

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n,
							 ColumnString::Chars_t & res_data)
	{
		res_data.resize(data.size());
		array(data.data(), data.data() + data.size(), res_data.data());
	}

	static void constant(const std::string & data, std::string & res_data)
	{
		res_data.resize(data.size());
		array(reinterpret_cast<const UInt8 *>(data.data()), reinterpret_cast<const UInt8 *>(data.data() + data.size()),
			  reinterpret_cast<UInt8 *>(&res_data[0]));
	}

	/** Converts a single code point starting at `src` to desired case, storing result starting at `dst`.
	 *	`src` and `dst` are incremented by corresponding sequence lengths. */
	static void toCase(const UInt8 * & src, const UInt8 * const src_end, UInt8 * & dst)
	{
		if (src[0] <= ascii_upper_bound)
		{
			if (*src >= not_case_lower_bound && *src <= not_case_upper_bound)
				*dst++ = *src++ ^ flip_case_mask;
			else
				*dst++ = *src++;
		}
		else if (src + 1 < src_end &&
				 ((src[0] == 0xD0u && (src[1] >= 0x80u && src[1] <= 0xBFu)) ||
				  (src[0] == 0xD1u && (src[1] >= 0x80u && src[1] <= 0x9Fu))))
		{
			cyrillic_to_case(src, src_end, dst);
		}
		else if (src + 1 < src_end && src[0] == 0xC2u)
		{
			/// Пунктуация U+0080 - U+00BF, UTF-8: C2 80 - C2 BF
			*dst++ = *src++;
			*dst++ = *src++;
		}
		else if (src + 2 < src_end && src[0] == 0xE2u)
		{
			/// Символы U+2000 - U+2FFF, UTF-8: E2 80 80 - E2 BF BF
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

private:
	static constexpr auto ascii_upper_bound = '\x7f';
	static constexpr auto flip_case_mask = 'A' ^ 'a';

	static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
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
				const auto is_not_case = _mm_and_si128(_mm_cmpgt_epi8(chars, v_not_case_lower_bound),
													   _mm_cmplt_epi8(chars, v_not_case_upper_bound));
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
};


template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionStringToString>(); }

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }
	bool isInjective(const Block &) override { return is_injective; }

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0]->clone();
	}

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr column = block.safeGetByPosition(arguments[0]).column;
		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
			block.safeGetByPosition(result).column = col_res;
			Impl::vector(col->getChars(), col->getOffsets(),
				col_res->getChars(), col_res->getOffsets());
		}
		else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(&*column))
		{
			auto col_res = std::make_shared<ColumnFixedString>(col->getN());
			block.safeGetByPosition(result).column = col_res;
			Impl::vector_fixed(col->getChars(), col->getN(),
				col_res->getChars());
		}
		else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
		{
			String res;
			Impl::constant(col->getData(), res);
			auto col_res = std::make_shared<ColumnConstString>(col->size(), res);
			block.safeGetByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};



struct NameLowerUTF8		{ static constexpr auto name = "lowerUTF8"; };
struct NameUpperUTF8		{ static constexpr auto name = "upperUTF8"; };


typedef FunctionStringToString<
	LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>,
	NameLowerUTF8>	FunctionLowerUTF8;
typedef FunctionStringToString<
	LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>,
	NameUpperUTF8>	FunctionUpperUTF8;


}
