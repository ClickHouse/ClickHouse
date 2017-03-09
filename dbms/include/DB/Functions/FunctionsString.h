#pragma once

#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Functions/IFunction.h>

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


template <bool negative = false>
struct EmptyImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt8> & res);

	static void vector_fixed_to_constant(const ColumnString::Chars_t & data, size_t n, UInt8 & res);

	static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt8> & res);

	static void constant(const std::string & data, UInt8 & res);

	static void array(const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt8> & res);

	static void constant_array(const Array & data, UInt8 & res);
};


/** Вычисляет длину строки в байтах.
  */
struct LengthImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt64> & res);

	static void vector_fixed_to_constant(const ColumnString::Chars_t & data, size_t n, UInt64 & res)
	{
		res = n;
	}

	static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res);

	static void constant(const std::string & data, UInt64 & res);

	static void array(const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt64> & res);

	static void constant_array(const Array & data, UInt64 & res);
};


/** Если строка представляет собой текст в кодировке UTF-8, то возвращает длину текста в кодовых точках.
  * (не в символах: длина текста "ё" может быть как 1, так и 2, в зависимости от нормализации)
  * Иначе - поведение не определено.
  */
struct LengthUTF8Impl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt64> & res);

	static void vector_fixed_to_constant(const ColumnString::Chars_t & data, size_t n, UInt64 & res);

	static void vector_fixed_to_vector(const ColumnString::Chars_t & data, size_t n, PaddedPODArray<UInt64> & res);

	static void constant(const std::string & data, UInt64 & res);

	static void array(const ColumnString::Offsets_t & offsets, PaddedPODArray<UInt64> & res);

	static void constant_array(const Array & data, UInt64 & res);
};


template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
	static void vector(const ColumnString::Chars_t & data,
		const ColumnString::Offsets_t & offsets,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

	static void constant(const std::string & data, std::string & res_data);

private:
	static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst);
};


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
	 *	`src` and `dst` are incremented by corresponding sequence lengths. */
	static void toCase(const UInt8 *& src, const UInt8 * const src_end, UInt8 *& dst);

private:
	static constexpr auto ascii_upper_bound = '\x7f';
	static constexpr auto flip_case_mask = 'A' ^ 'a';

	static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst);
};


/** Разворачивает строку в байтах.
  */
struct ReverseImpl
{
	static void vector(const ColumnString::Chars_t & data,
		const ColumnString::Offsets_t & offsets,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

	static void constant(const std::string & data, std::string & res_data);
};


/** Разворачивает последовательность кодовых точек в строке в кодировке UTF-8.
  * Результат может не соответствовать ожидаемому, так как модифицирующие кодовые точки (например, диакритика) могут примениться не к тем символам.
  * Если строка не в кодировке UTF-8, то поведение не определено.
  */
struct ReverseUTF8Impl
{
	static void vector(const ColumnString::Chars_t & data,
		const ColumnString::Offsets_t & offsets,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n, ColumnString::Chars_t & res_data);

	static void constant(const std::string & data, std::string & res_data);
};


/** Выделяет подстроку в строке, как последовательности байт.
  */
struct SubstringImpl
{
	static void vector(const ColumnString::Chars_t & data,
		const ColumnString::Offsets_t & offsets,
		size_t start,
		size_t length,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void vector_fixed(const ColumnString::Chars_t & data,
		size_t n,
		size_t start,
		size_t length,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void constant(const std::string & data, size_t start, size_t length, std::string & res_data);
};


/** Если строка в кодировке UTF-8, то выделяет в ней подстроку кодовых точек.
  * Иначе - поведение не определено.
  */
struct SubstringUTF8Impl
{
	static void vector(const ColumnString::Chars_t & data,
		const ColumnString::Offsets_t & offsets,
		size_t start,
		size_t length,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void vector_fixed(const ColumnString::Chars_t & data,
		ColumnString::Offset_t n,
		size_t start,
		size_t length,
		ColumnString::Chars_t & res_data,
		ColumnString::Offsets_t & res_offsets);

	static void constant(const std::string & data, size_t start, size_t length, std::string & res_data);
};


template <typename Impl, typename Name, typename ResultType>
class FunctionStringOrArrayToT : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override
	{
		return 1;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
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


/// Также работает над массивами.
class FunctionReverse : public IFunction
{
public:
	static constexpr auto name = "reverse";
	static FunctionPtr create(const Context & context);

	String getName() const override;

	size_t getNumberOfArguments() const override
	{
		return 1;
	}
	bool isInjective(const Block &) override
	{
		return true;
	}
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context);

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

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override;

private:
	enum class InstructionType : UInt8
	{
		COPY_STRING,
		COPY_FIXED_STRING,
		COPY_CONST_STRING
	};

	/// column pointer augmented with offset (current offset String/FixedString, unused for Const<String>)
	using ColumnAndOffset = std::pair<const IColumn *, IColumn::Offset_t>;
	/// InstructionType is being stored to allow using static_cast safely
	using Instruction = std::pair<InstructionType, ColumnAndOffset>;
	using Instructions = std::vector<Instruction>;

	/** calculate total length of resulting strings (without terminating nulls), determine whether all input
	  *	strings are constant, assemble instructions
	  */
	Instructions getInstructions(const Block & block, const ColumnNumbers & arguments, size_t & out_length, bool & out_const);

	void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result);

	void executeNAry(Block & block, const ColumnNumbers & arguments, const size_t result);

	static void vector_vector(const ColumnString::Chars_t & a_data,
		const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data,
		const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void vector_fixed_vector(const ColumnString::Chars_t & a_data,
		const ColumnString::Offsets_t & a_offsets,
		const ColumnString::Chars_t & b_data,
		ColumnString::Offset_t b_n,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void vector_constant(const ColumnString::Chars_t & a_data,
		const ColumnString::Offsets_t & a_offsets,
		const std::string & b,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void fixed_vector_vector(const ColumnString::Chars_t & a_data,
		ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data,
		const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void fixed_vector_fixed_vector(const ColumnString::Chars_t & a_data,
		ColumnString::Offset_t a_n,
		const ColumnString::Chars_t & b_data,
		ColumnString::Offset_t b_n,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void fixed_vector_constant(const ColumnString::Chars_t & a_data,
		ColumnString::Offset_t a_n,
		const std::string & b,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void constant_vector(const std::string & a,
		const ColumnString::Chars_t & b_data,
		const ColumnString::Offsets_t & b_offsets,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void constant_fixed_vector(const std::string & a,
		const ColumnString::Chars_t & b_data,
		ColumnString::Offset_t b_n,
		ColumnString::Chars_t & c_data,
		ColumnString::Offsets_t & c_offsets);

	static void constant_constant(const std::string & a, const std::string & b, std::string & c);
};


template <typename Impl, typename Name>
class FunctionStringNumNumToString : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context);

	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override
	{
		return 3;
	}

	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};


class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
	static constexpr auto name = "appendTrailingCharIfAbsent";
	static FunctionPtr create(const Context & context);

	String getName() const override;

private:
	size_t getNumberOfArguments() const override
	{
		return 2;
	}
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
	void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override;
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
struct NameLowerUTF8
{
	static constexpr auto name = "lowerUTF8";
};
struct NameUpperUTF8
{
	static constexpr auto name = "upperUTF8";
};
struct NameReverseUTF8
{
	static constexpr auto name = "reverseUTF8";
};
struct NameSubstring
{
	static constexpr auto name = "substring";
};
struct NameSubstringUTF8
{
	static constexpr auto name = "substringUTF8";
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
typedef FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', Poco::Unicode::toLower, UTF8CyrillicToCase<true>>, NameLowerUTF8>
	FunctionLowerUTF8;
typedef FunctionStringToString<LowerUpperUTF8Impl<'a', 'z', Poco::Unicode::toUpper, UTF8CyrillicToCase<false>>, NameUpperUTF8>
	FunctionUpperUTF8;
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;
using FunctionSubstring = FunctionStringNumNumToString<SubstringImpl, NameSubstring>;
using FunctionSubstringUTF8 = FunctionStringNumNumToString<SubstringUTF8Impl, NameSubstringUTF8>;
using FunctionConcat = ConcatImpl<NameConcat, false>;
using FunctionConcatAssumeInjective = ConcatImpl<NameConcatAssumeInjective, true>;
}
