#pragma once

#include <statdaemons/OptimizedRegularExpression.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции поиска и замены в строках:
  *
  * position(haystack, needle)	- обычный поиск подстроки в строке, возвращает позицию (в байтах) найденной подстроки, начиная с 1, или 0, если подстрока не найдена.
  * positionUTF8(haystack, needle) - то же самое, но позиция вычисляется в кодовых точках, при условии, что строка в кодировке UTF-8.
  * 
  * like(haystack, pattern)		- поиск по регулярному выражению LIKE; возвращает 0 или 1. Регистронезависимое, но только для латиницы.
  * notLike(haystack, pattern)
  *
  * match(haystack, pattern)	- поиск по регулярному выражению re2; возвращает 0 или 1.
  *
  * TODO:
  * extract(haystack, pattern)	- вынимает первый subpattern, (или нулевой, если первого нет) согласно регулярному выражению re2;
  * 							  возвращает пустую строку, если не матчится.
  * extract(haystack, pattern, n) - вынимает n-ый subpattern; возвращает пустую строку, если не матчится.
  *
  * replaceOne(haystack, pattern, replacement) - замена шаблона по заданным правилам, только первое вхождение.
  * replaceAll(haystack, pattern, replacement) - замена шаблона по заданным правилам, все вхождения.
  *
  * Внимание! На данный момент, аргементы needle, pattern, n, replacement обязаны быть константами.
  */


struct PositionImpl
{
	typedef UInt64 ResultType;

	/// Предполагается, что res нужного размера и инициализирован нулями.
	static void vector(const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets,
		const std::string & needle,
		std::vector<UInt64> & res)
	{
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
				res[i] = (i != 0) ? pos - begin - offsets[i - 1] + 1 : (pos - begin + 1);

			pos = begin + offsets[i];
			++i;
		}
	}

	static void constant(const std::string & data, const std::string & needle, UInt64 & res)
	{
		res = data.find(needle);
		if (res == std::string::npos)
			res = 0;
		else
			++res;
	}
};


struct PositionUTF8Impl
{
	typedef UInt64 ResultType;
	
	static void vector(const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets,
		const std::string & needle,
		std::vector<UInt64> & res)
	{
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
			{
				/// А теперь надо найти, сколько кодовых точек находится перед pos.
				res[i] = 1;
				for (const UInt8 * c = begin + (i != 0 ? offsets[i - 1] : 0); c < pos; ++c)
					if (*c <= 0x7F || *c >= 0xC0)
						++res[i];
			}

			pos = begin + offsets[i];
			++i;
		}
	}

	static void constant(const std::string & data, const std::string & needle, UInt64 & res)
	{
		res = data.find(needle);
		if (res == std::string::npos)
			res = 0;
		else
			++res;
	}
};


/// Переводит выражение LIKE в regexp re2. Например, abc%def -> ^abc.*def$
inline String likePatternToRegexp(const String & pattern)
{
	String res = "^";
	res.reserve(pattern.size() * 2);
	const char * pos = pattern.data();
	const char * end = pos + pattern.size();

	while (pos < end)
	{
		switch (*pos)
		{
			case '^': case '$': case '.': case '[': case '|': case '(': case ')': case '?': case '*': case '+': case '{':
				res += '\\';
				res += *pos;
				break;
			case '%':
				res += ".*";
				break;
			case '_':
				res += ".";
				break;
			case '\\':
				++pos;
				if (pos == end)
					res += "\\\\";
				else
				{
					if (*pos == '%' || *pos == '_')
						res += *pos;
					else
					{
						res += '\\';
						res += *pos;
					}
				}
				break;
			default:
				res += *pos;
				break;
		}
		++pos;
	}

	res += '$';
	return res;
}


/// Сводится ли выражение LIKE к поиску подстроки в строке?
inline bool likePatternIsStrstr(const String & pattern, String & res)
{
	res = "";

	if (pattern.size() < 2 || *pattern.begin() != '%' || *pattern.rbegin() != '%')
		return false;

	res.reserve(pattern.size() * 2);

	const char * pos = pattern.data();
	const char * end = pos + pattern.size();

	++pos;
	--end;

	while (pos < end)
	{
		switch (*pos)
		{
			case '%': case '_':
				return false;
			case '\\':
				++pos;
				if (pos == end)
					return false;
				else
					res += *pos;
				break;
			default:
				res += *pos;
				break;
		}
		++pos;
	}

	return true;
}


struct Regexps
{
	typedef std::map<String, OptimizedRegularExpression> KnownRegexps;

	static const OptimizedRegularExpression & get(const std::string & pattern)
	{
		/// В GCC thread safe statics.
		static KnownRegexps known_regexps;
		static Poco::FastMutex mutex;
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		KnownRegexps::const_iterator it = known_regexps.find(pattern);
		if (known_regexps.end() == it)
			it = known_regexps.insert(std::make_pair(pattern, OptimizedRegularExpression(pattern))).first;

		return it->second;
	}

	static const OptimizedRegularExpression & getLike(const std::string & pattern)
	{
		/// В GCC thread safe statics.
		static KnownRegexps known_regexps;
		static Poco::FastMutex mutex;
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);

		KnownRegexps::const_iterator it = known_regexps.find(pattern);
		if (known_regexps.end() == it)
 			it = known_regexps.insert(std::make_pair(pattern, OptimizedRegularExpression(likePatternToRegexp(pattern), OptimizedRegularExpression::RE_CASELESS))).first;

		return it->second;
	}
};


/** like - использовать выражения LIKE, если true; использовать выражения re2, если false.
  * Замечание: хотелось бы запускать регексп сразу над всем массивом, аналогично функции position,
  *  но для этого пришлось бы сделать поддержку символов \0 в движке регулярных выражений,
  *  и их интерпретацию как начал и концов строк.
  */
template <bool like, bool revert = false>
struct MatchImpl
{
	typedef UInt8 ResultType;

	static void vector(const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets,
		const std::string & pattern,
		std::vector<UInt8> & res)
	{
		String strstr_pattern;
		/// Простой случай, когда выражение LIKE сводится к поиску подстроки в строке
		if (like && likePatternIsStrstr(pattern, strstr_pattern))
		{
			/// Если отрицание - то заполним вектор единицами (вместо имеющихся там нулей)
			if (revert)
				memset(&res[0], 1, offsets.size());
			
			const UInt8 * begin = &data[0];
			const UInt8 * pos = begin;
			const UInt8 * end = pos + data.size();

			/// Текущий индекс в массиве строк.
			size_t i = 0;

			/// Искать будем следующее вхождение сразу во всех строках.
			while (pos < end && NULL != (pos = reinterpret_cast<UInt8 *>(memmem(pos, end - pos, strstr_pattern.data(), strstr_pattern.size()))))
			{
				/// Определим, к какому индексу оно относится.
				while (begin + offsets[i] < pos)
					++i;

				/// Проверяем, что вхождение не переходит через границы строк.
				if (pos + strstr_pattern.size() < begin + offsets[i])
					res[i] = !revert;

				pos = begin + offsets[i];
				++i;
			}
		}
		else
		{
			const OptimizedRegularExpression & regexp = like ? Regexps::getLike(pattern) : Regexps::get(pattern);

			size_t size = offsets.size();
			for (size_t i = 0; i < size; ++i)
				res[i] = revert ^ regexp.match(reinterpret_cast<const char *>(&data[i != 0 ? offsets[i - 1] : 0]), (i != 0 ? offsets[i] - offsets[i - 1] : offsets[0]) - 1);
		}
	}

	static void constant(const std::string & data, const std::string & pattern, UInt8 & res)
	{
		const OptimizedRegularExpression & regexp = like ? Regexps::getLike(pattern) : Regexps::get(pattern);
		res = revert ^ regexp.match(data);
	}
};


template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
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
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new typename DataTypeFromFieldType<typename Impl::ResultType>::Type;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef typename Impl::ResultType ResultType;
		
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

		const ColumnConstString * col_needle = dynamic_cast<const ColumnConstString *>(&*column_needle);
		if (!col_needle)
			throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);
		
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			Impl::vector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getOffsets(), col_needle->getData(), vec_res);
		}
		else if (const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*column))
		{
			ResultType res = 0;
			Impl::constant(col->getData(), col_needle->getData(), res);

			ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NamePosition 		{ static const char * get() { return "position"; } };
struct NamePositionUTF8		{ static const char * get() { return "positionUTF8"; } };
struct NameMatch			{ static const char * get() { return "match"; } };
struct NameLike				{ static const char * get() { return "like"; } };
struct NameNotLike			{ static const char * get() { return "notLike"; } };

typedef FunctionsStringSearch<PositionImpl, 			NamePosition> 		FunctionPosition;
typedef FunctionsStringSearch<PositionUTF8Impl, 		NamePositionUTF8> 	FunctionPositionUTF8;
typedef FunctionsStringSearch<MatchImpl<false>, 		NameMatch> 			FunctionMatch;
typedef FunctionsStringSearch<MatchImpl<true>, 			NameLike> 			FunctionLike;
typedef FunctionsStringSearch<MatchImpl<true, true>, 	NameNotLike> 		FunctionNotLike;

}
