#pragma once

#include <Poco/NumberFormatter.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypesNumberVariable.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции работы со строками:
  *
  * length, concat, substring, left, right, insert, replace, lower, upper, repeat, reverse,
  * escape, quote, strcmp, trim, trimLeft, trimRight, pad, padLeft
  * lengthUTF8, substringUTF8, leftUTF8, rightUTF8, insertUTF8, replaceUTF8, lowerUTF8, upperUTF8, reverseUTF8,
  * padUTF8, padLeftUTF8.
  *
  * s 				-> UInt64: 	length, lengthUTF8
  * s 				-> s:		lower, upper, lowerUTF8, upperUTF8, reverse, reverseUTF8, escape, quote, trim, trimLeft, trimRight
  * s, s			-> Int8: 	strcmp
  * s, s 			-> s: 		concat
  * s, c1, c2 		-> s:		substring, substringUTF8, pad, padLeft, padUTF8, padLeftUTF8
  * s, c1 			-> s:		substring, substringUTF8, left, right, leftUTF8, rightUTF8, repeat
  * s, c1, s2		-> s:		insert, insertUTF8
  * s, c1, c2, s2	-> s:		replace, replaceUTF8
  * 
  * Функции поиска строк и регулярных выражений расположены отдельно.
  * Функции работы с URL расположены отдельно.
  * Функции кодирования строк, конвертации в другие типы расположены отдельно.
  */


/** Вычисляет длину строки в байтах.
  */
struct LengthImpl
{
	static void vector(const std::vector<UInt8> & data, const std::vector<size_t> & offsets,
		std::vector<UInt64> & res)
	{
		size_t size = offsets.size();
		for (size_t i = 0; i < size; ++i)
			res[i] = i == 0
				? (offsets[i] - 1)
				: (offsets[i] - 1 - offsets[i - 1]);
	}

	static void vector_fixed_to_constant(const std::vector<UInt8> & data, size_t n,
		UInt64 & res)
	{
		res = n;
	}

	static void vector_fixed_to_vector(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt64> & res)
	{
	}

	static void constant(const std::string & data, UInt64 & res)
	{
		res = data.size();
	}
};


/** Если строка представляет собой текст в кодировке UTF-8, то возвращает длину текста в кодовых точках.
  * (не в символах: длина текста "ё" может быть как 1, так и 2, в зависимости от нормализации)
  * Иначе - поведение не определено.
  */
struct LengthUTF8Impl
{
	static void vector(const std::vector<UInt8> & data, const std::vector<size_t> & offsets,
		std::vector<UInt64> & res)
	{
		size_t size = offsets.size();

		size_t prev_offset = 0;
		for (size_t i = 0; i < size; ++i)
		{
			res[i] = 0;
			for (const UInt8 * c = &data[prev_offset]; c + 1 < &data[offsets[i]]; ++c)
				if (*c <= 0x7F || *c >= 0xC0)
					++res[i];
			prev_offset = offsets[i];
		}
	}

	static void vector_fixed_to_constant(const std::vector<UInt8> & data, size_t n,
		UInt64 & res)
	{
	}

	static void vector_fixed_to_vector(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt64> & res)
	{
		size_t size = data.size() / n;

		for (size_t i = 0; i < size; ++i)
		{
			res[i] = 0;
			for (const UInt8 * c = &data[i * n]; c < &data[(i + 1) * n]; ++c)
				if (*c <= 0x7F || *c >= 0xC0)
					++res[i];
		}
	}

	static void constant(const std::string & data, UInt64 & res)
	{
		res = 0;
		for (const UInt8 * c = reinterpret_cast<const UInt8 *>(data.data()); c < reinterpret_cast<const UInt8 *>(data.data() + data.size()); ++c)
			if (*c <= 0x7F || *c >= 0xC0)
				++res;
	}
};


/** Переводит строку в нижний (верхний) регистр, в текущей локали, в однобайтовой кодировке.
  */
template <int F(int)>
struct LowerUpperImpl
{
	static void vector(const std::vector<UInt8> & data, const std::vector<size_t> & offsets,
		std::vector<UInt8> & res_data, std::vector<size_t> & res_offsets)
	{
		res_data.resize(data.size());
		res_offsets = offsets;
		array(&*data.begin(), &*data.end(), &*res_data.begin());
	}

	static void vector_fixed(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt8> & res_data)
	{
		res_data.resize(data.size());
		array(&*data.begin(), &*data.end(), &*res_data.begin());
	}

	static void constant(const std::string & data, std::string & res_data)
	{
		res_data.resize(data.size());
		array(reinterpret_cast<const UInt8 *>(&*data.begin()), reinterpret_cast<const UInt8 *>(&*data.end()),
			reinterpret_cast<UInt8 *>(&*res_data.begin()));
	}

private:
	static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
	{
		for (; src < src_end; ++src, ++dst)
			*dst = F(*src);
	}
};


/** Если строка содержит текст в кодировке UTF-8 - перевести его в нижний (верхний) регистр.
  * Замечание: предполагается, что после перевода символа в другой регистр,
  *  длина его мультибайтовой последовательности в UTF-8 не меняется.
  * Иначе - поведение не определено.
  */
template <int F(int)>
struct LowerUpperUTF8Impl
{
	static void vector(const std::vector<UInt8> & data, const std::vector<size_t> & offsets,
		std::vector<UInt8> & res_data, std::vector<size_t> & res_offsets)
	{
		res_data.resize(data.size());
		res_offsets = offsets;
		array(&*data.begin(), &*data.end(), &*res_data.begin());
	}

	static void vector_fixed(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt8> & res_data)
	{
		res_data.resize(data.size());
		array(&*data.begin(), &*data.end(), &*res_data.begin());
	}

	static void constant(const std::string & data, std::string & res_data)
	{
		res_data.resize(data.size());
		array(reinterpret_cast<const UInt8 *>(&*data.begin()), reinterpret_cast<const UInt8 *>(&*data.end()),
			reinterpret_cast<UInt8 *>(&*res_data.begin()));
	}

private:
	static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
	{
		static Poco::UTF8Encoding utf8;
		
		while (src < src_end)
		{
			int chars = utf8.convert(F(utf8.convert(src)), dst, src_end - src);
			if (chars)
			{
				src += chars;
				dst += chars;
			}
			else
			{
				++src;
				++dst;
			}
		}
	}
};


template <typename Impl, typename Name>
class FunctionStringToUInt64 : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]) && !dynamic_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef UInt64 ResultType;
		
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			Impl::vector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getOffsets(), vec_res);
		}
		else if (const ColumnFixedString * col = dynamic_cast<const ColumnFixedString *>(&*column))
		{
			/// Для фиксированной строки, функция length возвращает константу, а функция lengthUTF8 - не константу.
			if ("length" == getName())
			{
				ResultType res = 0;
				Impl::vector_fixed_to_constant(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getN(), res);

				ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col->size(), res);
				block.getByPosition(result).column = col_res;
			}
			else
			{
				ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
				block.getByPosition(result).column = col_res;

				ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
				vec_res.resize(col->size());
				Impl::vector_fixed_to_vector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getN(), vec_res);
			}
		}
		else if (const ColumnConst<String> * col = dynamic_cast<const ColumnConst<String> *>(&*column))
		{
			ResultType res = 0;
			Impl::constant(col->getData(), res);

			ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename Impl, typename Name>
class FunctionStringToString : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]) && !dynamic_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;
			Impl::vector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getOffsets(),
				dynamic_cast<ColumnUInt8 &>(col_res->getData()).getData(), col_res->getOffsets());
		}
		else if (const ColumnFixedString * col = dynamic_cast<const ColumnFixedString *>(&*column))
		{
			ColumnFixedString * col_res = new ColumnFixedString(col->getN());
			block.getByPosition(result).column = col_res;
			Impl::vector_fixed(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getN(),
				dynamic_cast<ColumnUInt8 &>(col_res->getData()).getData());
		}
		else if (const ColumnConst<String> * col = dynamic_cast<const ColumnConst<String> *>(&*column))
		{
			String res;
			Impl::constant(col->getData(), res);
			ColumnConstString * col_res = new ColumnConstString(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameLength 			{ static const char * get() { return "length"; } };
struct NameLengthUTF8 		{ static const char * get() { return "lengthUTF8"; } };
struct NameLower 			{ static const char * get() { return "lower"; } };
struct NameUpper 			{ static const char * get() { return "upper"; } };
struct NameLowerUTF8		{ static const char * get() { return "lowerUTF8"; } };
struct NameUpperUTF8		{ static const char * get() { return "upperUTF8"; } };

typedef FunctionStringToUInt64<LengthImpl, 				NameLength> 			FunctionLength;
typedef FunctionStringToUInt64<LengthUTF8Impl, 			NameLengthUTF8> 		FunctionLengthUTF8;
typedef FunctionStringToString<LowerUpperImpl<tolower>, NameLower>	 			FunctionLower;
typedef FunctionStringToString<LowerUpperImpl<toupper>, NameUpper>	 			FunctionUpper;
typedef FunctionStringToString<LowerUpperUTF8Impl<Poco::Unicode::toLower>,	NameLowerUTF8>	FunctionLowerUTF8;
typedef FunctionStringToString<LowerUpperUTF8Impl<Poco::Unicode::toUpper>,	NameUpperUTF8>	FunctionUpperUTF8;


}
