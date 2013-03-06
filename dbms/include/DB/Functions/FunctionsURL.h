#pragma once

#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/FunctionsString.h>
#include <DB/Functions/FunctionsStringSearch.h>
#include <DB/Functions/FunctionsStringArray.h>


namespace DB
{

/** Функции работы с URL.
  * Все функции работают не по RFC - то есть, максимально упрощены ради производительности.
  *
  * Функции, извлекающие часть URL-а.
  * Если в URL-е нет ничего похожего, то возвращается пустая строка.
  * 
  *  domain
  *  domainWithoutWWW
  *  topLevelDomain
  *  protocol
  *  path
  *  queryString
  *  fragment
  *  queryStringAndFragment
  *
  * Функции, удаляющие часть из URL-а.
  * Если в URL-е нет ничего похожего, то URL остаётся без изменений.
  * 
  *  cutWWW
  *  cutFragment
  *  cutQueryString
  *  cutQueryStringAndFragment
  *
  * Извлечь значение параметра в URL, если он есть. Вернуть пустую строку, если его нет.
  * Если таких параметров много - вернуть значение первого. Значение не разэскейпливается.
  *
  *  getURLParameter(URL, parameter)
  */

typedef const char * Pos;

struct ExtractProtocol
{
	static size_t getReserveLengthForElement() { return strlen("https") + 1; }
	
	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;
		
		Pos pos = data;

		while ((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z') || (*pos >= '0' && *pos <= '9'))
			++pos;

		if (pos == data || pos + 3 >= data + size)
			return;

		if (pos[0] == ':')
			res_size = pos - data;
	}
};

template <bool without_www>
struct ExtractDomain
{
	static size_t getReserveLengthForElement() { return 15; }
	
	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;
		
		Pos pos = data;
		Pos end = pos + size;

		Pos tmp;
		size_t protocol_length;
		ExtractProtocol::execute(data, size, tmp, protocol_length);
		pos += protocol_length + 3;

		if (pos[-1] != '/' || pos[-2] != '/')
			return;

		if (without_www && pos + 4 < end && !strncmp(pos, "www.", 4))
			pos += 4;

		Pos domain_begin = pos;

		while (pos < end && *pos != '/' && *pos != ':' && *pos != '?' && *pos != '#')
			++pos;

		if (pos == domain_begin)
			return;

		res_data = domain_begin;
		res_size = pos - domain_begin;
	}
};

struct ExtractTopLevelDomain
{
	static size_t getReserveLengthForElement() { return 5; }

	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		Pos tmp;
		size_t protocol_length;
		ExtractProtocol::execute(data, size, tmp, protocol_length);
		pos += protocol_length + 3;

		if (pos[-1] != '/' || pos[-2] != '/')
			return;

		Pos domain_begin = pos;

		while (pos < end && *pos != '/' && *pos != ':' && *pos != '?' && *pos != '#')
			++pos;

		if (pos == domain_begin)
			return;

		Pos last_dot = reinterpret_cast<Pos>(memrchr(domain_begin, '.', pos - domain_begin));

		if (!last_dot)
			return;

		/// Для IPv4-адресов не выделяем ничего.
		if (last_dot[1] <= '9')
			return;

		res_data = last_dot + 1;
		res_size = pos - res_data;
	}
};

struct ExtractPath
{
	static size_t getReserveLengthForElement() { return 25; }

	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		if (NULL != (pos = strchr(data, '/')) && pos[1] == '/' && NULL != (pos = strchr(pos + 2, '/')))
		{
			Pos query_string_or_fragment = strpbrk(pos, "?#");

			res_data = pos;
			res_size = (query_string_or_fragment ? query_string_or_fragment : end) - res_data;
		}
	}
};

template <bool without_leading_char>
struct ExtractQueryString
{
	static size_t getReserveLengthForElement() { return 10; }

	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		if (NULL != (pos = strchr(data, '?')))
		{
			Pos fragment = strchr(pos, '#');

			res_data = pos + (without_leading_char ? 1 : 0);
			res_size = (fragment ? fragment : end) - res_data;
		}
	}
};

template <bool without_leading_char>
struct ExtractFragment
{
	static size_t getReserveLengthForElement() { return 10; }

	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		if (NULL != (pos = strchr(data, '#')))
		{
			res_data = pos + (without_leading_char ? 1 : 0);
			res_size = end - res_data;
		}
	}
};

template <bool without_leading_char>
struct ExtractQueryStringAndFragment
{
	static size_t getReserveLengthForElement() { return 20; }
	
	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		if (NULL != (pos = strchr(data, '?')))
		{
			res_data = pos + (without_leading_char ? 1 : 0);
			res_size = end - res_data;
		}
	}
};

/// С точкой на конце.
struct ExtractWWW
{
	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		Pos tmp;
		size_t protocol_length;
		ExtractProtocol::execute(data, size, tmp, protocol_length);
		pos += protocol_length + 3;

		if (pos[-1] != '/' || pos[-2] != '/')
			return;

		if (pos + 4 < end && !strncmp(pos, "www.", 4))
		{
			res_data = pos;
			res_size = 4;
		}
	}
};


struct ExtractURLParameterImpl
{
	static void vector(const std::vector<UInt8> & data,
					    const ColumnArray::Offsets_t & offsets,
					    std::string pattern,
						std::vector<UInt8> & res_data, ColumnArray::Offsets_t & res_offsets)
	{
		res_data.reserve(data.size()  / 5);
		res_offsets.resize(offsets.size());
		
		pattern += '=';
		const char * param_str = pattern.c_str();
		size_t param_len = pattern.size();
		
		size_t prev_offset = 0;
		size_t res_offset = 0;
		
		for (size_t i = 0; i < offsets.size(); ++i)
		{
			size_t cur_offset = offsets[i];
			
			const char * pos = NULL;
			
			do
			{
				const char * str = reinterpret_cast<const char *>(&data[prev_offset]);
				
				const char * begin = strchr(str, '?');
				if (begin == NULL)
					break;
				
				pos = strstr(begin + 1, param_str);
				if (pos == NULL)
					break;
				if (pos != begin + 1 && *(pos - 1) != ';' && *(pos - 1) != '&')
				{
					pos = NULL;
					break;
				}
				
				pos += param_len;
			} while (false);
			
			if (pos != NULL)
			{
				const char * end = strpbrk(pos, "&;#");
				if (end == NULL)
					end = pos + strlen(pos);
				
				res_data.resize(res_offset + (end - pos) + 1);
				memcpy(&res_data[res_offset], pos, end - pos);
				res_offset += end - pos;
			}
			else
			{
				res_data.resize(res_offset + 1);
			}
			
			res_data[res_offset] = 0;
			++res_offset;
			res_offsets[i] = res_offset;
			
			prev_offset = cur_offset;
		}
	}
};


class ExtractURLParametersImpl
{
private:
	Pos pos;
	Pos end;
	bool first;
	
public:
	static String getName() { return "extractURLParameters"; }
	
	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	
	void init(Block & block, const ColumnNumbers & arguments) {}
	
	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		pos = pos_;
		end = end_;
		first = true;
	}
	
	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		if (pos == NULL)
			return false;
		
		if (first)
		{
			first = false;
			pos = strchr(pos, '?');
			if (pos == NULL)
				return false;
			++pos;
		}
		
		token_begin = pos;
		pos = strchr(pos, '=');
		if (pos == NULL)
			return false;
		++pos;
		pos = strpbrk(pos, "&;#");
		if (pos == NULL)
		{
			token_end = end;
		}
		else
		{
			token_end = pos;
			
			if (*pos == '#')
				pos = NULL;
			else
				++pos;
		}
		
		return true;
	}
};


class URLHierarchyImpl
{
private:
	Pos pos;
	Pos end;
	bool first;
	
public:
	static String getName() { return "extractURLParameters"; }
	
	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
				throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	
	void init(Block & block, const ColumnNumbers & arguments) {}
	
	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		pos = pos_;
		end = end_;
		first = true;
	}
	
	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		if (pos == NULL)
			return false;
		
		if (first)
		{
			first = false;
			pos = strchr(pos, '?');
			if (pos == NULL)
				return false;
			++pos;
		}
		
		token_begin = pos;
		pos = strchr(pos, '=');
		if (pos == NULL)
			return false;
		++pos;
		pos = strpbrk(pos, "&;#");
		if (pos == NULL)
			token_end = end;
		else
			token_end = pos++;
		
		return true;
	}
};


/** Выделить кусок строки, используя Extractor.
  */
template <typename Extractor>
struct ExtractSubstringImpl
{
	static void vector(const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets,
		std::vector<UInt8> & res_data, ColumnArray::Offsets_t & res_offsets)
	{
		res_data.reserve(data.size() * Extractor::getReserveLengthForElement());
		size_t size = offsets.size();
		res_offsets.resize(size);

		size_t prev_offset = 0;
		size_t res_offset = 0;

		/// Выделенный кусок.
		Pos start;
		size_t length;
		
		for (size_t i = 0; i < size; ++i)
		{
			Extractor::execute(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);
			
			res_data.resize(res_data.size() + length + 1);
			memcpy(&res_data[res_offset], start, length);
			res_offset += length + 1;
			res_data[res_offset - 1] = 0;

			res_offsets[i] = res_offset;
			prev_offset = offsets[i];
		}
	}

	static void constant(const std::string & data,
		std::string & res_data)
	{
		Pos start;
		size_t length;
		Extractor::execute(data.data(), data.size(), start, length);
		res_data.assign(start, length);
	}

	static void vector_fixed(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt8> & res_data)
	{
		throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Удалить кусок строки, используя Extractor.
  */
template <typename Extractor>
struct CutSubstringImpl
{
	static void vector(const std::vector<UInt8> & data, const ColumnArray::Offsets_t & offsets,
		std::vector<UInt8> & res_data, ColumnArray::Offsets_t & res_offsets)
	{
		res_data.reserve(data.size());
		size_t size = offsets.size();
		res_offsets.resize(size);

		size_t prev_offset = 0;
		size_t res_offset = 0;

		/// Выделенный кусок.
		Pos start;
		size_t length;

		for (size_t i = 0; i < size; ++i)
		{
			const char * current = reinterpret_cast<const char *>(&data[prev_offset]);
			Extractor::execute(current, offsets[i] - prev_offset - 1, start, length);
			size_t start_index = start - reinterpret_cast<const char *>(&data[0]);

			res_data.resize(res_data.size() + offsets[i] - prev_offset - length);
			memcpy(&res_data[res_offset], current, start - current);
			memcpy(&res_data[res_offset + start - current], start + length, offsets[i] - start_index - length);
			res_offset += offsets[i] - prev_offset - length;

			res_offsets[i] = res_offset;
			prev_offset = offsets[i];
		}
	}

	static void constant(const std::string & data,
		std::string & res_data)
	{
		Pos start;
		size_t length;
		Extractor::execute(data.data(), data.size(), start, length);
		res_data.erase(start - data.data(), length);
	}

	static void vector_fixed(const std::vector<UInt8> & data, size_t n,
		std::vector<UInt8> & res_data)
	{
		throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameProtocol 					{ static const char * get() { return "protocol"; } };
struct NameDomain 						{ static const char * get() { return "domain"; } };
struct NameDomainWithoutWWW 			{ static const char * get() { return "domainWithoutWWW"; } };
struct NameTopLevelDomain 				{ static const char * get() { return "topLevelDomain"; } };
struct NamePath 						{ static const char * get() { return "path"; } };
struct NameQueryString					{ static const char * get() { return "queryString"; } };
struct NameFragment 					{ static const char * get() { return "fragment"; } };
struct NameQueryStringAndFragment		{ static const char * get() { return "queryStringAndFragment"; } };

struct NameCutWWW 						{ static const char * get() { return "cutWWW"; } };
struct NameCutQueryString				{ static const char * get() { return "cutQueryString"; } };
struct NameCutFragment 					{ static const char * get() { return "cutFragment"; } };
struct NameCutQueryStringAndFragment 	{ static const char * get() { return "cutQueryStringAndFragment"; } };

struct NameExtractURLParameter 		{ static const char * get() { return "extractURLParameter"; } };

typedef FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, 			NameProtocol>	 		FunctionProtocol;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false> >, 		NameDomain>	 			FunctionDomain;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true>  >, 		NameDomainWithoutWWW>	FunctionDomainWithoutWWW;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain>, 		NameTopLevelDomain>		FunctionTopLevelDomain;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractPath>, 				NamePath>				FunctionPath;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractQueryString<true> >, 	NameQueryString>		FunctionQueryString;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractFragment<true> >, 		NameFragment>			FunctionFragment;
typedef FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true> >, NameQueryStringAndFragment>	FunctionQueryStringAndFragment;

typedef FunctionStringToString<CutSubstringImpl<ExtractWWW>, 						NameCutWWW>				FunctionCutWWW;
typedef FunctionStringToString<CutSubstringImpl<ExtractQueryString<false> >, 		NameCutQueryString>		FunctionCutQueryString;
typedef FunctionStringToString<CutSubstringImpl<ExtractFragment<false> >, 		NameCutFragment>		FunctionCutFragment;
typedef FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false> >, NameCutQueryStringAndFragment>	FunctionCutQueryStringAndFragment;

typedef FunctionsStringSearchToString<ExtractURLParameterImpl, NameExtractURLParameter> FunctionExtractURLParameter;
typedef FunctionTokens<ExtractURLParametersImpl> FunctionExtractURLParameters;

}
