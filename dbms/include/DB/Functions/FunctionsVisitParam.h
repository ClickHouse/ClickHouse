#pragma once

#include <Poco/NumberFormatter.h>
#include <Poco/UTF8Encoding.h>
#include <Poco/Unicode.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/FunctionsStringSearch.h>

/** Функции для извлечения параметров визитов.
 *  Реализованы через шаблоны из FunctionsStringSearch.h.
 * 
 * Проверить есть ли параметр
 * 		visitParamHas
 * 
 * Извлечь числовое значение параметра
 * 		visitParamExtractUInt
 * 		visitParamExtractInt
 * 		visitParamExtractFloat
 * 		visitParamExtractBool
 * 
 * Извлечь строкое значение параметра
 * 		visitParamExtractString - значение разэскейпливается
 * 		visitParamExtractRaw
 */

namespace DB
{
	
/// Прочитать беззнаковое целое в простом формате из не-0-terminated строки.
static UInt64 readUIntText(const UInt8 * buf, const UInt8 * end)
{
	UInt64 x = 0;

	if (buf == end)
		return x;

	while (buf != end)
	{
		switch (*buf)
		{
			case '+':
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				x *= 10;
				x += *buf - '0';
				break;
			default:
			    return x;
		}
		++buf;
	}

	return x;
}


/// Прочитать знаковое целое в простом формате из не-0-terminated строки.
static Int64 readIntText(const UInt8 * buf, const UInt8 * end)
{
	bool negative = false;
	Int64 x = 0;

	if (buf == end)
		return x;

	while (buf != end)
	{
		switch (*buf)
		{
			case '+':
				break;
			case '-':
				negative = true;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				x *= 10;
				x += *buf - '0';
				break;
			default:
			    return x;
		}
		++buf;
	}
	if (negative)
		x = -x;

	return x;
}


/// Прочитать число с плавающей запятой в простом формате, с грубым округлением, из не-0-terminated строки.
static double readFloatText(const UInt8 * buf, const UInt8 * end)
{
	bool negative = false;
	double x = 0;
	bool after_point = false;
	double power_of_ten = 1;

	if (buf == end)
		return x;

	while (buf != end)
	{
		switch (*buf)
		{
			case '+':
				break;
			case '-':
				negative = true;
				break;
			case '.':
				after_point = true;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				if (after_point)
				{
					power_of_ten /= 10;
					x += (*buf - '0') * power_of_ten;
				}
				else
				{
					x *= 10;
					x += *buf - '0';
				}
				break;
			case 'e':
			case 'E':
			{
				++buf;
				Int32 exponent = readIntText(buf, end);
				if (exponent == 0)
				{
					if (negative)
						x = -x;
					return x;
				}
				else if (exponent > 0)
				{
					for (Int32 i = 0; i < exponent; ++i)
						x *= 10;
					if (negative)
						x = -x;
					return x;
				}
				else
				{
					for (Int32 i = 0; i < exponent; ++i)
						x /= 10;
					if (negative)
						x = -x;
					return x;
				}
			}
			default:
			    return x;
		}
		++buf;
	}
	if (negative)
		x = -x;

	return x;
}


struct HasParam
{
	typedef UInt8 ResultType;
	
	static UInt8 extract(const UInt8 * pos, const UInt8 * end)
	{
		return true;
	}
};


struct ExtractUInt
{
	typedef UInt64 ResultType;
	
	static UInt64 extract(const UInt8 * pos, const UInt8 * end)
	{
		return readUIntText(pos, end);
	}
};


struct ExtractInt
{
	typedef Int64 ResultType;
	
	static Int64 extract(const UInt8 * pos, const UInt8 * end)
	{
		return readIntText(pos, end);
	}
};


struct ExtractFloat
{
	typedef Float64 ResultType;
	
	static Float64 extract(const UInt8 * pos, const UInt8 * end)
	{
		return readFloatText(pos, end);
	}
};


struct ExtractBool
{
	typedef UInt8 ResultType;
	
	static UInt8 extract(const UInt8 * pos, const UInt8 * end)
	{
		return pos + 4 <= end && 0 == strncmp(reinterpret_cast<const char *>(pos), "true", 4);
	}
};


/** Ищет вхождения поля в параметре визитов и вызывает ParamExtractor
 * на каждое вхождение поля, передавая ему указатель на часть строки,
 * где начинается вхождение значения поля.
 * ParamExtractor должен распарсить и вернуть значение нужного типа.
 * 
 * Если поле не было найдено или полю соответствует некорректное значение,
 * то используется значение по умолчанию - 0.
 */
template<typename ParamExtractor>
struct ExtractParamImpl
{
	typedef typename ParamExtractor::ResultType ResultType;

	/// Предполагается, что res нужного размера и инициализирован нулями.
	static void vector(const std::vector<UInt8> & data, const ColumnString::Offsets_t & offsets,
		std::string needle,
		std::vector<ResultType> & res)
	{
		/// Ищем параметр просто как подстроку вида "name":
		needle = "\"" + needle + "\":";
		
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end && NULL != (pos = reinterpret_cast<UInt8 *>(memmem(pos, end - pos, needle.data(), needle.size()))))
		{
			/// Определим, к какому индексу оно относится.
			while (begin + offsets[i] < pos)
				++i;

			/// Проверяем, что вхождение не переходит через границы строк.
			if (pos + needle.size() < begin + offsets[i])
				res[i] = ParamExtractor::extract(pos + needle.size(), begin + offsets[i]);

			pos = begin + offsets[i];
			++i;
		}
	}

	static void constant(const std::string & data, std::string needle, ResultType & res)
	{
		needle = "\"" + needle + "\":";
		size_t pos = data.find(needle);
		if (pos == std::string::npos)
			res = 0;
		else
			res = ParamExtractor::extract(
				reinterpret_cast<const UInt8 *>(data.data() + pos + needle.size()),
				reinterpret_cast<const UInt8 *>(data.data() + data.size())
			);
	}
};


/** Для случая когда тип поля, которое нужно извлечь - строка.
 */
template<typename ParamExtractor>
struct ExtractParamToStringImpl
{
};


struct NameVisitParamHas			{ static const char * get() { return "visitParamHas"; } };
struct NameVisitParamExtractUInt	{ static const char * get() { return "visitParamExtractUInt"; } };
struct NameVisitParamExtractInt	{ static const char * get() { return "visitParamExtractInt"; } };
struct NameVisitParamExtractFloat	{ static const char * get() { return "visitParamExtractFloat"; } };
struct NameVisitParamExtractBool	{ static const char * get() { return "visitParamExtractBool"; } };


typedef FunctionsStringSearch<ExtractParamImpl<HasParam>, NameVisitParamHas> FunctionVisitParamHas;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractUInt>, NameVisitParamExtractUInt> FunctionVisitParamExtractUInt;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractInt>, NameVisitParamExtractInt> FunctionVisitParamExtractInt;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractFloat>, NameVisitParamExtractFloat> FunctionVisitParamExtractFloat;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractBool>, NameVisitParamExtractBool> FunctionVisitParamExtractBool;

}