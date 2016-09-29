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
  *  extractURLParameter(URL, name)
  *
  * Извлечь все параметры из URL в виде массива строк вида name=value.
  *  extractURLParameters(URL)
  *
  * Извлечь все имена параметров из URL в виде массива строк
  *  extractURLParameterNames(URL)
  *
  * Убрать указанный параметр из URL.
  *  cutURLParameter(URL, name)
  *
  * Получить массив иерархии URL. См. функцию nextURLInHierarchy в URLParser.
  *  URLHierarchy(URL)
  */

using Pos = const char *;

struct ExtractProtocol
{
	static size_t getReserveLengthForElement() { return strlen("https") + 1; }

	static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;

		while (isAlphaNumericASCII(*pos))
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

struct ExtractFirstSignificantSubdomain
{
	static size_t getReserveLengthForElement() { return 10; }

	static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size, Pos * out_domain_end = nullptr)
	{
		res_data = data;
		res_size = 0;

		Pos tmp;
		size_t domain_length;
		ExtractDomain<true>::execute(data, size, tmp, domain_length);

		if (domain_length == 0)
			return;

		if (out_domain_end)
			*out_domain_end = tmp + domain_length;

		/// cut useless dot
		if (tmp[domain_length - 1] == '.')
			--domain_length;

		res_data = tmp;
		res_size = domain_length;

		auto begin = tmp;
		auto end = begin + domain_length;
		const char * last_3_periods[3]{};
		auto pos = static_cast<const char *>(memchr(begin, '.', domain_length));

		while (pos)
		{
			last_3_periods[2] = last_3_periods[1];
			last_3_periods[1] = last_3_periods[0];
			last_3_periods[0] = pos;
			pos = static_cast<const char *>(memchr(pos + 1, '.', end - pos - 1));
		}

		if (!last_3_periods[0])
			return;

		if (!last_3_periods[1])
		{
			res_size = last_3_periods[0] - begin;
			return;
		}

		if (!last_3_periods[2])
			last_3_periods[2] = begin - 1;

		if (!strncmp(last_3_periods[1] + 1, "com", 3) ||
			!strncmp(last_3_periods[1] + 1, "net", 3) ||
			!strncmp(last_3_periods[1] + 1, "org", 3) ||
			!strncmp(last_3_periods[1] + 1, "co", 2))
		{
			res_data += last_3_periods[2] + 1 - begin;
			res_size = last_3_periods[1] - last_3_periods[2] - 1;
			return;
		}

		res_data += last_3_periods[1] + 1 - begin;
		res_size = last_3_periods[0] - last_3_periods[1] - 1;
	}
};

struct CutToFirstSignificantSubdomain
{
	static size_t getReserveLengthForElement() { return 15; }

	static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos tmp_data;
		size_t tmp_length;
		Pos domain_end;
		ExtractFirstSignificantSubdomain::execute(data, size, tmp_data, tmp_length, &domain_end);

		if (tmp_length == 0)
			return;

		res_data = tmp_data;
		res_size = domain_end - tmp_data;
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

		if (nullptr != (pos = strchr(data, '/')) && pos[1] == '/' && nullptr != (pos = strchr(pos + 2, '/')))
		{
			Pos query_string_or_fragment = strpbrk(pos, "?#");

			res_data = pos;
			res_size = (query_string_or_fragment ? query_string_or_fragment : end) - res_data;
		}
	}
};

struct ExtractPathFull
{
	static size_t getReserveLengthForElement() { return 30; }

	static void execute(const Pos data, const size_t size, Pos & res_data, size_t & res_size)
	{
		res_data = data;
		res_size = 0;

		Pos pos = data;
		Pos end = pos + size;

		if (nullptr != (pos = strchr(data, '/')) && pos[1] == '/' && nullptr != (pos = strchr(pos + 2, '/')))
		{
			res_data = pos;
			res_size = end - res_data;
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

		if (nullptr != (pos = strchr(data, '?')))
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

		if (nullptr != (pos = strchr(data, '#')))
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

		if (nullptr != (pos = strchr(data, '?')))
		{
			res_data = pos + (without_leading_char ? 1 : 0);
			res_size = end - res_data;
		}
		else if (nullptr != (pos = strchr(data, '#')))
		{
			res_data = pos;
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
	static void vector(const ColumnString::Chars_t & data,
					    const ColumnString::Offsets_t & offsets,
					    std::string pattern,
						ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
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

			const char * str = reinterpret_cast<const char *>(&data[prev_offset]);

			const char * pos = nullptr;
			const char * begin = strpbrk(str, "?#");
			if (begin != nullptr)
			{
				pos = begin + 1;
				while (true)
				{
					pos = strstr(pos, param_str);

					if (pos == nullptr)
						break;

					if (pos[-1] != '?' && pos[-1] != '#' && pos[-1] != '&')
					{
						pos += param_len;
						continue;
					}
					else
					{
						pos += param_len;
						break;
					}
				}
			}

			if (pos != nullptr)
			{
				const char * end = strpbrk(pos, "&#");
				if (end == nullptr)
					end = pos + strlen(pos);

				res_data.resize(res_offset + (end - pos) + 1);
				memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], pos, end - pos);
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


struct CutURLParameterImpl
{
	static void vector(const ColumnString::Chars_t & data,
					    const ColumnString::Offsets_t & offsets,
					    std::string pattern,
						ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		res_data.reserve(data.size());
		res_offsets.resize(offsets.size());

		pattern += '=';
		const char * param_str = pattern.c_str();
		size_t param_len = pattern.size();

		size_t prev_offset = 0;
		size_t res_offset = 0;

		for (size_t i = 0; i < offsets.size(); ++i)
		{
			size_t cur_offset = offsets[i];

			const char * url_begin = reinterpret_cast<const char *>(&data[prev_offset]);
			const char * url_end = reinterpret_cast<const char *>(&data[cur_offset]) - 1;
			const char * begin_pos = url_begin;
			const char * end_pos = begin_pos;

			do
			{
				const char * begin = strpbrk(url_begin, "?#");
				if (begin == nullptr)
					break;

				const char * pos = strstr(begin + 1, param_str);
				if (pos == nullptr)
					break;

				if (pos[-1] != '?' && pos[-1] != '#' && pos[-1] != '&')
				{
					pos = nullptr;
					break;
				}

				begin_pos = pos;
				end_pos = begin_pos + param_len;

				/// Пропустим значение.
				while (*end_pos && *end_pos != '&' && *end_pos != '#')
					++end_pos;

				/// Захватим '&' до или после параметра.
				if (*end_pos == '&')
					++end_pos;
				else if (begin_pos[-1] == '&')
					--begin_pos;
			} while (false);

			size_t cut_length = (url_end - url_begin) - (end_pos - begin_pos);
			res_data.resize(res_offset + cut_length + 1);
			memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], url_begin, begin_pos - url_begin);
			memcpySmallAllowReadWriteOverflow15(&res_data[res_offset] + (begin_pos - url_begin), end_pos, url_end - end_pos);
			res_offset += cut_length + 1;
			res_data[res_offset - 1] = 0;
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
	static constexpr auto name = "extractURLParameters";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments) {}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
	}

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
		if (pos == nullptr)
			return false;

		if (first)
		{
			first = false;
			pos = strpbrk(pos, "?#");
			if (pos == nullptr)
				return false;
			++pos;
		}

		while (true)
		{
			token_begin = pos;
			pos = strpbrk(pos, "=&#?");
			if (pos == nullptr)
				return false;

			if (*pos == '?')
			{
				++pos;
				continue;
			}

			break;
		}

		if (*pos == '&' || *pos == '#')
		{
			token_end = pos++;
		}
		else
		{
			++pos;
			pos = strpbrk(pos, "&#");
			if (pos == nullptr)
				token_end = end;
			else
				token_end = pos++;
		}

		return true;
	}
};

class ExtractURLParameterNamesImpl
{
private:
	Pos pos;
	Pos end;
	bool first;

public:
	static constexpr auto name = "extractURLParameterNames";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
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
		if (pos == nullptr)
			return false;

		if (first)
		{
			first = false;
			pos = strpbrk(pos, "?#");
		}
		else
			pos = strpbrk(pos, "&#");

		if (pos == nullptr)
			return false;
		++pos;

		while (true)
		{
			token_begin = pos;

			pos = strpbrk(pos, "=&#?");
			if (pos == nullptr)
				return false;
			else
				token_end = pos;

			if (*pos == '?')
			{
				++pos;
				continue;
			}

			break;
		}

		return true;
	}
};

class URLHierarchyImpl
{
private:
	Pos begin;
	Pos pos;
	Pos end;

public:
	static constexpr auto name = "URLHierarchy";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments) {}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
	}

	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		begin = pos = pos_;
		end = end_;
	}

	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		/// Код из URLParser.

		if (pos == end)
			return false;

		if (pos == begin)
		{
			/// Распарсим всё, что идёт до пути

			/// Предположим, что протокол уже переведён в нижний регистр.
			while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
				++pos;

			/** Будем вычислять иерархию только для URL-ов, в которых есть протокол, и после него идут два слеша.
			 * (http, file - подходят, mailto, magnet - не подходят), и после двух слешей ещё хоть что-нибудь есть
			 * Для остальных просто вернём полный URL как единственный элемент иерархии.
			 */
			if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
			{
				pos = end;
				token_begin = begin;
				token_end = end;
				return true;
			}

			/// Доменом для простоты будем считать всё, что после протокола и двух слешей, до следующего слеша или до ? или до #
			while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
				++pos;

			if (pos != end)
				++pos;

			token_begin = begin;
			token_end = pos;

			return true;
		}

		/// Идём до следующего / или ? или #, пропуская все те, что вначале.
		while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
			++pos;
		if (pos == end)
			return false;
		while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
			++pos;

		if (pos != end)
			++pos;

		token_begin = begin;
		token_end = pos;

		return true;
	}
};


class URLPathHierarchyImpl
{
private:
	Pos begin;
	Pos pos;
	Pos end;
	Pos start;

public:
	static constexpr auto name = "URLPathHierarchy";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments) {}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
	}

	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		begin = pos = pos_;
		start = begin;
		end = end_;
	}

	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		/// Код из URLParser.

		if (pos == end)
			return false;

		if (pos == begin)
		{
			/// Распарсим всё, что идёт до пути

			/// Предположим, что протокол уже переведён в нижний регистр.
			while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
				++pos;

			/** Будем вычислять иерархию только для URL-ов, в которых есть протокол, и после него идут два слеша.
			 * (http, file - подходят, mailto, magnet - не подходят), и после двух слешей ещё хоть что-нибудь есть
			 * Для остальных просто вернём пустой массив.
			 */
			if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
			{
				pos = end;
				return false;
			}

			/// Доменом для простоты будем считать всё, что после протокола и двух слешей, до следующего слеша или до ? или до #
			while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
				++pos;

			start = pos;

			if (pos != end)
				++pos;
		}

		/// Идём до следующего / или ? или #, пропуская все те, что вначале.
		while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
			++pos;
		if (pos == end)
			return false;
		while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
			++pos;

		if (pos != end)
			++pos;

		token_begin = start;
		token_end = pos;

		return true;
	}
};


/** Выделить кусок строки, используя Extractor.
  */
template <typename Extractor>
struct ExtractSubstringImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		size_t size = offsets.size();
		res_offsets.resize(size);
		res_data.reserve(size * Extractor::getReserveLengthForElement());

		size_t prev_offset = 0;
		size_t res_offset = 0;

		/// Выделенный кусок.
		Pos start;
		size_t length;

		for (size_t i = 0; i < size; ++i)
		{
			Extractor::execute(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

			res_data.resize(res_data.size() + length + 1);
			memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
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

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n,
		ColumnString::Chars_t & res_data)
	{
		throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
	}
};


/** Удалить кусок строки, используя Extractor.
  */
template <typename Extractor>
struct CutSubstringImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
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
			memcpySmallAllowReadWriteOverflow15(
				&res_data[res_offset], current, start - current);
			memcpySmallAllowReadWriteOverflow15(
				&res_data[res_offset + start - current], start + length, offsets[i] - start_index - length);
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
		res_data.reserve(data.size() - length);
		res_data.append(data.data(), start);
		res_data.append(start + length, data.data() + data.size());
	}

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n,
		ColumnString::Chars_t & res_data)
	{
		throw Exception("Column of type FixedString is not supported by URL functions", ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameProtocol 					{ static constexpr auto name = "protocol"; };
struct NameDomain 						{ static constexpr auto name = "domain"; };
struct NameDomainWithoutWWW 			{ static constexpr auto name = "domainWithoutWWW"; };
struct NameFirstSignificantSubdomain	{ static constexpr auto name = "firstSignificantSubdomain"; };
struct NameTopLevelDomain 				{ static constexpr auto name = "topLevelDomain"; };
struct NamePath 						{ static constexpr auto name = "path"; };
struct NamePathFull						{ static constexpr auto name = "pathFull"; };
struct NameQueryString					{ static constexpr auto name = "queryString"; };
struct NameFragment 					{ static constexpr auto name = "fragment"; };
struct NameQueryStringAndFragment		{ static constexpr auto name = "queryStringAndFragment"; };

struct NameCutToFirstSignificantSubdomain { static constexpr auto name = "cutToFirstSignificantSubdomain"; };

struct NameCutWWW 						{ static constexpr auto name = "cutWWW"; };
struct NameCutQueryString				{ static constexpr auto name = "cutQueryString"; };
struct NameCutFragment 					{ static constexpr auto name = "cutFragment"; };
struct NameCutQueryStringAndFragment 	{ static constexpr auto name = "cutQueryStringAndFragment"; };

struct NameExtractURLParameter			{ static constexpr auto name = "extractURLParameter"; };
struct NameCutURLParameter 				{ static constexpr auto name = "cutURLParameter"; };

using FunctionProtocol = FunctionStringToString<ExtractSubstringImpl<ExtractProtocol>, 				NameProtocol>	 	;
using FunctionDomain = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<false> >, 		NameDomain>	 		;
using FunctionDomainWithoutWWW = FunctionStringToString<ExtractSubstringImpl<ExtractDomain<true>  >, 		NameDomainWithoutWWW>;
using FunctionFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<ExtractFirstSignificantSubdomain>, NameFirstSignificantSubdomain>;
using FunctionTopLevelDomain = FunctionStringToString<ExtractSubstringImpl<ExtractTopLevelDomain>, 		NameTopLevelDomain>	;
using FunctionPath = FunctionStringToString<ExtractSubstringImpl<ExtractPath>, 					NamePath>			;
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPathFull>,				NamePathFull>		;
using FunctionQueryString = FunctionStringToString<ExtractSubstringImpl<ExtractQueryString<true> >, 	NameQueryString>	;
using FunctionFragment = FunctionStringToString<ExtractSubstringImpl<ExtractFragment<true> >, 		NameFragment>		;
using FunctionQueryStringAndFragment = FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true> >, NameQueryStringAndFragment>;

using FunctionCutToFirstSignificantSubdomain = FunctionStringToString<ExtractSubstringImpl<CutToFirstSignificantSubdomain>, NameCutToFirstSignificantSubdomain>;

using FunctionCutWWW = FunctionStringToString<CutSubstringImpl<ExtractWWW>, 						NameCutWWW>			;
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false> >, 		NameCutQueryString>	;
using FunctionCutFragment = FunctionStringToString<CutSubstringImpl<ExtractFragment<false> >, 			NameCutFragment>	;
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false> >, NameCutQueryStringAndFragment>;

using FunctionExtractURLParameter = FunctionsStringSearchToString<ExtractURLParameterImpl, NameExtractURLParameter>;
using FunctionCutURLParameter = FunctionsStringSearchToString<CutURLParameterImpl, NameCutURLParameter>;
using FunctionExtractURLParameters = FunctionTokens<ExtractURLParametersImpl>;
using FunctionExtractURLParameters = FunctionTokens<ExtractURLParametersImpl>;
using FunctionURLHierarchy = FunctionTokens<URLHierarchyImpl>;
using FunctionURLPathHierarchy = FunctionTokens<URLPathHierarchyImpl>;
using FunctionExtractURLParameterNames = FunctionTokens<ExtractURLParameterNamesImpl>;

}
