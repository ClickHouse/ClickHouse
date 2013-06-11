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

struct HasParam
{
	typedef UInt8 ResultType;
	
	static UInt8 extract(const UInt8 * pos, const UInt8 * end)
	{
		return true;
	}
};

template<typename NumericType>
struct ExtractNumericType
{
	typedef NumericType ResultType;
	
	static ResultType extract(const UInt8 * pos, const UInt8 * end)
	{
		ReadBuffer in(const_cast<char *>(reinterpret_cast<const char *>(pos)), end - pos, 0);
		
		/// Учимся читать числа в двойных кавычках
		if (!in.eof() && *in.position() == '"')
			++in.position();
		
		ResultType x = 0;
		if (!in.eof())
			readText(x, in);
		return x;
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
typedef FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<UInt64> >, NameVisitParamExtractUInt> FunctionVisitParamExtractUInt;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Int64> >, NameVisitParamExtractInt> FunctionVisitParamExtractInt;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractNumericType<Float64> >, NameVisitParamExtractFloat> FunctionVisitParamExtractFloat;
typedef FunctionsStringSearch<ExtractParamImpl<ExtractBool>, NameVisitParamExtractBool> FunctionVisitParamExtractBool;

}