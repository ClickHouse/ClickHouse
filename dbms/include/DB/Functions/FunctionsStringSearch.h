#pragma once

#include <Poco/Mutex.h>

#include <statdaemons/OptimizedRegularExpression.h>
#include <memory>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Common/Volnitsky.h>
#include <DB/Functions/IFunction.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <mutex>
#include <stack>

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
  * Применяет регексп re2 и достаёт:
  * - первый subpattern, если в regexp-е есть subpattern;
  * - нулевой subpattern (сматчившуюся часть, иначе);
  * - если не сматчилось - пустую строку.
  * extract(haystack, pattern)
  *
  * replaceOne(haystack, pattern, replacement) - замена шаблона по заданным правилам, только первое вхождение.
  * replaceAll(haystack, pattern, replacement) - замена шаблона по заданным правилам, все вхождения.
  *
  * replaceRegexpOne(haystack, pattern, replacement) - замена шаблона по заданному регекспу, только первое вхождение.
  * replaceRegexpAll(haystack, pattern, replacement) - замена шаблона по заданному регекспу, все вхождения.
  *
  * Внимание! На данный момент, аргументы needle, pattern, n, replacement обязаны быть константами.
  */


struct PositionImpl
{
	typedef UInt64 ResultType;

	/// Предполагается, что res нужного размера и инициализирован нулями.
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		const std::string & needle,
		PODArray<UInt64> & res)
	{
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		Volnitsky searcher(needle.data(), needle.size(), end - pos);

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end && end != (pos = searcher.search(pos, end - pos)))
		{
			/// Определим, к какому индексу оно относится.
			while (begin + offsets[i] < pos)
			{
				res[i] = 0;
				++i;
			}

			/// Проверяем, что вхождение не переходит через границы строк.
			if (pos + needle.size() < begin + offsets[i])
				res[i] = (i != 0) ? pos - begin - offsets[i - 1] + 1 : (pos - begin + 1);
			else
				res[i] = 0;

			pos = begin + offsets[i];
			++i;
		}

		memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
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

	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		const std::string & needle,
		PODArray<UInt64> & res)
	{
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		Volnitsky searcher(needle.data(), needle.size(), end - pos);

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end && end != (pos = searcher.search(pos, end - pos)))
		{
			/// Определим, к какому индексу оно относится.
			while (begin + offsets[i] < pos)
			{
				res[i] = 0;
				++i;
			}

			/// Проверяем, что вхождение не переходит через границы строк.
			if (pos + needle.size() < begin + offsets[i])
			{
				/// А теперь надо найти, сколько кодовых точек находится перед pos.
				res[i] = 1;
				for (const UInt8 * c = begin + (i != 0 ? offsets[i - 1] : 0); c < pos; ++c)
					if (*c <= 0x7F || *c >= 0xC0)
						++res[i];
			}
			else
				res[i] = 0;

			pos = begin + offsets[i];
			++i;
		}

		memset(&res[i], 0, (res.size() - i) * sizeof(res[0]));
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
	String res;
	res.reserve(pattern.size() * 2);
	const char * pos = pattern.data();
	const char * end = pos + pattern.size();

	if (pos < end && *pos == '%')
		++pos;
	else
		res = "^";

	while (pos < end)
	{
		switch (*pos)
		{
			case '^': case '$': case '.': case '[': case '|': case '(': case ')': case '?': case '*': case '+': case '{':
				res += '\\';
				res += *pos;
				break;
			case '%':
				if (pos + 1 != end)
					res += ".*";
				else
					return res;
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

	if (pattern.size() < 2 || pattern.front() != '%' || pattern.back() != '%')
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


namespace Regexps
{
	struct Holder;
	struct Deleter;

	using Regexp = OptimizedRegularExpressionImpl<false>;
	using KnownRegexps = std::map<String, std::unique_ptr<Holder>>;
	using Pointer = std::unique_ptr<Regexp, Deleter>;

	///	Container for regular expressions with embedded mutex for safe addition and removal
	struct Holder
	{
		std::mutex mutex;
		std::stack<std::unique_ptr<Regexp>> stack;

		/**	Extracts and returns a pointer from the collection if it's not empty,
		*	creates a new one by calling provided f() otherwise.
		*/
		template <typename Factory> Pointer get(Factory && f);
	};

	/**	Specialized deleter for std::unique_ptr.
	*	Returns underlying pointer back to holder thus reclaiming its ownership.
	*/
	struct Deleter
	{
		Holder * holder;

		Deleter(Holder * holder = nullptr) : holder{holder} {}

		void operator()(Regexp * owning_ptr) const
		{
			std::lock_guard<std::mutex> lock{holder->mutex};
			holder->stack.emplace(owning_ptr);
		}

	};

	template <typename Factory>
	inline Pointer Holder::get(Factory && f)
	{
		std::lock_guard<std::mutex> lock{mutex};

		if (stack.empty())
			return { f(), this };

		auto regexp = stack.top().release();
		stack.pop();

		return { regexp, this };
	}

	template <bool like>
	inline Regexp createRegexp(const std::string & pattern, int flags) { return {pattern, flags}; }
	template <>
	inline Regexp createRegexp<true>(const std::string & pattern, int flags) { return {likePatternToRegexp(pattern), flags}; }

	template <bool like, bool no_capture>
	inline Pointer get(const std::string & pattern)
	{
		/// C++11 has thread-safe function-local statics on most modern compilers.
		static KnownRegexps known_regexps;	/// Разные переменные для разных параметров шаблона.
		static std::mutex mutex;
		std::lock_guard<std::mutex> lock{mutex};

		auto it = known_regexps.find(pattern);
		if (known_regexps.end() == it)
			it = known_regexps.emplace(pattern, std::make_unique<Holder>()).first;

		return it->second->get([&pattern] {
			return new Regexp{createRegexp<like>(pattern, no_capture ? OptimizedRegularExpression::RE_NO_CAPTURE : 0)};
		});
	}
}


/** like - использовать выражения LIKE, если true; использовать выражения re2, если false.
  * Замечание: хотелось бы запускать регексп сразу над всем массивом, аналогично функции position,
  *  но для этого пришлось бы сделать поддержку символов \0 в движке регулярных выражений,
  *  и их интерпретацию как начал и концов строк.
  */
template <bool like, bool revert = false>
struct MatchImpl
{
	typedef UInt8 ResultType;

	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		const std::string & pattern,
		PODArray<UInt8> & res)
	{
		String strstr_pattern;
		/// Простой случай, когда выражение LIKE сводится к поиску подстроки в строке
		if (like && likePatternIsStrstr(pattern, strstr_pattern))
		{
			const UInt8 * begin = &data[0];
			const UInt8 * pos = begin;
			const UInt8 * end = pos + data.size();

			/// Текущий индекс в массиве строк.
			size_t i = 0;

			/// TODO Надо сделать так, чтобы searcher был общим на все вызовы функции.
			Volnitsky searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

			/// Искать будем следующее вхождение сразу во всех строках.
			while (pos < end && end != (pos = searcher.search(pos, end - pos)))
			{
				/// Определим, к какому индексу оно относится.
				while (begin + offsets[i] < pos)
				{
					res[i] = revert;
					++i;
				}

				/// Проверяем, что вхождение не переходит через границы строк.
				if (pos + strstr_pattern.size() < begin + offsets[i])
					res[i] = !revert;
				else
					res[i] = revert;

				pos = begin + offsets[i];
				++i;
			}

			/// Хвостик, в котором не может быть подстрок.
			memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
		}
		else
		{
			size_t size = offsets.size();

			const auto & regexp = Regexps::get<like, true>(pattern);

			std::string required_substring;
			bool is_trivial;
			bool required_substring_is_prefix;	/// для anchored выполнения регекспа.

			regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

			if (required_substring.empty())
			{
				if (!regexp->getRE2())	/// Пустой регексп. Всегда матчит.
				{
					memset(&res[0], 1, size * sizeof(res[0]));
				}
				else
				{
					size_t prev_offset = 0;
					for (size_t i = 0; i < size; ++i)
					{
						res[i] = revert ^ regexp->getRE2()->Match(
							re2_st::StringPiece(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1),
							0, offsets[i] - prev_offset - 1, re2_st::RE2::UNANCHORED, nullptr, 0);

						prev_offset = offsets[i];
					}
				}
			}
			else
			{
				/// NOTE Это почти совпадает со случаем likePatternIsStrstr.

				const UInt8 * begin = &data[0];
				const UInt8 * pos = begin;
				const UInt8 * end = pos + data.size();

				/// Текущий индекс в массиве строк.
				size_t i = 0;

				Volnitsky searcher(required_substring.data(), required_substring.size(), end - pos);

				/// Искать будем следующее вхождение сразу во всех строках.
				while (pos < end && end != (pos = searcher.search(pos, end - pos)))
				{
					/// Определим, к какому индексу оно относится.
					while (begin + offsets[i] < pos)
					{
						res[i] = revert;
						++i;
					}

					/// Проверяем, что вхождение не переходит через границы строк.
					if (pos + strstr_pattern.size() < begin + offsets[i])
					{
						/// И если не переходит - при необходимости, проверяем регекспом.

						if (is_trivial)
							res[i] = !revert;
						else
						{
							const char * str_data = reinterpret_cast<const char *>(&data[i != 0 ? offsets[i - 1] : 0]);
							size_t str_size = (i != 0 ? offsets[i] - offsets[i - 1] : offsets[0]) - 1;

							if (required_substring_is_prefix)
								res[i] = revert ^ regexp->getRE2()->Match(
									re2_st::StringPiece(str_data, str_size),
									reinterpret_cast<const char *>(pos) - str_data, str_size, re2_st::RE2::ANCHOR_START, nullptr, 0);
							else
								res[i] = revert ^ regexp->getRE2()->Match(
									re2_st::StringPiece(str_data, str_size),
									0, str_size, re2_st::RE2::UNANCHORED, nullptr, 0);
						}
					}
					else
						res[i] = revert;

					pos = begin + offsets[i];
					++i;
				}

				memset(&res[i], revert, (res.size() - i) * sizeof(res[0]));
			}
		}
	}

	static void constant(const std::string & data, const std::string & pattern, UInt8 & res)
	{
		const auto & regexp = Regexps::get<like, true>(pattern);
		res = revert ^ regexp->match(data);
	}
};


struct ExtractImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
					   const std::string & pattern,
					   ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		res_data.reserve(data.size() / 5);
		res_offsets.resize(offsets.size());

		const auto & regexp = Regexps::get<false, false>(pattern);

		unsigned capture = regexp->getNumberOfSubpatterns() > 0 ? 1 : 0;
		OptimizedRegularExpression::MatchVec matches;
		matches.reserve(capture + 1);
		size_t prev_offset = 0;
		size_t res_offset = 0;

		for (size_t i = 0; i < offsets.size(); ++i)
		{
			size_t cur_offset = offsets[i];

			unsigned count = regexp->match(reinterpret_cast<const char *>(&data[prev_offset]), cur_offset - prev_offset - 1, matches, capture + 1);
			if (count > capture && matches[capture].offset != std::string::npos)
			{
				const auto & match = matches[capture];
				res_data.resize(res_offset + match.length + 1);
				memcpy(&res_data[res_offset], &data[prev_offset + match.offset], match.length);
				res_offset += match.length;
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


/** Заменить все вхождения регекспа needle на строку replacement. needle и replacement - константы.
  * Replacement может содержать подстановки, например '\2-\3-\1'
  */
template <bool replaceOne = false>
struct ReplaceRegexpImpl
{
	/// Последовательность инструкций, описывает как получить конечную строку. Каждый элемент
	/// либо подстановка, тогда первое число в паре ее id,
	/// либо строка, которую необходимо вставить, записана второй в паре. (id = -1)
	typedef std::vector< std::pair<int, std::string> > Instructions;

	static void split(const std::string & s, Instructions & instructions)
	{
		instructions.clear();
		String now = "";
		for (size_t i = 0; i < s.size(); ++i)
		{
			if (s[i] == '\\' && i + 1 < s.size())
			{
				if (isdigit(s[i+1])) /// Подстановка
				{
					if (!now.empty())
					{
						instructions.push_back(std::make_pair(-1, now));
						now = "";
					}
					instructions.push_back(std::make_pair(s[i+1] - '0', ""));
				}
				else
					now += s[i+1]; /// Экранирование
				++i;
			}
			else
				now += s[i]; /// Обычный символ
		}
		if (!now.empty())
		{
			instructions.push_back(std::make_pair(-1, now));
			now = "";
		}
	}

	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		const std::string & needle, const std::string & replacement,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		ColumnString::Offset_t res_offset = 0;
		res_data.reserve(data.size());
		size_t size = offsets.size();
		res_offsets.resize(size);

		RE2 searcher(needle);
		int capture = std::min(searcher.NumberOfCapturingGroups() + 1, 10);
		re2::StringPiece matches[10];

		Instructions instructions;
		split(replacement, instructions);

		for (const auto & it : instructions)
			if (it.first >= capture)
				throw Exception("Invalid replace instruction in replacement string. Id: " + toString(it.first) +
				", but regexp has only " + toString(capture - 1) + " subpatterns",
					ErrorCodes::BAD_ARGUMENTS);

		/// Искать вхождение сразу во всех сроках нельзя, будем двигаться вдоль каждой независимо
		for (size_t id = 0; id < size; ++id)
		{
			int from = id > 0 ? offsets[id-1] : 0;
			int start_pos = 0;
			re2::StringPiece input(reinterpret_cast<const char*>(&data[0] + from), offsets[id] - from - 1);

			while (start_pos < input.length())
			{
				/// Правда ли, что с этой строкой больше не надо выполнять преобразования
				bool can_finish_current_string = false;

				if (searcher.Match(input, start_pos, input.length(), re2::RE2::Anchor::UNANCHORED, matches, capture))
				{
					const auto & match = matches[0];
					size_t char_to_copy = (match.data() - input.data()) - start_pos;

					/// Копируем данные без изменения
					res_data.resize(res_data.size() + char_to_copy);
					memcpy(&res_data[res_offset], input.data() + start_pos, char_to_copy);
					res_offset += char_to_copy;
					start_pos += char_to_copy + match.length();

					/// Выполняем инструкции подстановки
					for (const auto & it : instructions)
					{
						if (it.first >= 0)
						{
							res_data.resize(res_data.size() + matches[it.first].length());
							memcpy(&res_data[res_offset], matches[it.first].data(), matches[it.first].length());
							res_offset += matches[it.first].length();
						}
						else
						{
							res_data.resize(res_data.size() + it.second.size());
							memcpy(&res_data[res_offset], it.second.data(), it.second.size());
							res_offset += it.second.size();
						}
					}
					if (replaceOne || match.length() == 0)
						can_finish_current_string = true;
				} else
					can_finish_current_string = true;

				/// Если пора, копируем все символы до конца строки
				if (can_finish_current_string)
				{
					res_data.resize(res_data.size() + input.length() - start_pos);
					memcpy(&res_data[res_offset], input.data() + start_pos, input.length() - start_pos);
					res_offset += input.length() - start_pos;
					res_offsets[id] = res_offset;
					start_pos = input.length();
				}
			}
			res_data.resize(res_data.size() + 1);
			res_data[res_offset++] = 0;
			res_offsets[id] = res_offset;
		}
	}

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n,
		const std::string & needle, const std::string & replacement,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		ColumnString::Offset_t res_offset = 0;
		size_t size = data.size() / n;
		res_data.reserve(data.size());
		res_offsets.resize(size);

		RE2 searcher(needle);
		int capture = std::min(searcher.NumberOfCapturingGroups() + 1, 10);
		re2::StringPiece matches[10];

		Instructions instructions;
		split(replacement, instructions);

		for (const auto & it : instructions)
			if (it.first >= capture)
				throw Exception("Invalid replace instruction in replacement string. Id: " + toString(it.first) +
				", but regexp has only " + toString(capture - 1) + " subpatterns",
					ErrorCodes::BAD_ARGUMENTS);

		/// Искать вхождение сразу во всех сроках нельзя, будем двигаться вдоль каждой независимо.
		for (size_t id = 0; id < size; ++id)
		{
			int from = id * n;
			int start_pos = 0;
			re2::StringPiece input(reinterpret_cast<const char*>(&data[0] + from), (id + 1) * n - from);

			while (start_pos < input.length())
			{
				/// Правда ли, что с этой строкой больше не надо выполнять преобразования.
				bool can_finish_current_string = false;

				if (searcher.Match(input, start_pos, input.length(), re2::RE2::Anchor::UNANCHORED, matches, capture))
				{
					const auto & match = matches[0];
					size_t char_to_copy = (match.data() - input.data()) - start_pos;

					/// Копируем данные без изменения
					res_data.resize(res_data.size() + char_to_copy);
					memcpy(&res_data[res_offset], input.data() + start_pos, char_to_copy);
					res_offset += char_to_copy;
					start_pos += char_to_copy + match.length();

					/// Выполняем инструкции подстановки
					for (const auto & it : instructions)
					{
						if (it.first >= 0)
						{
							res_data.resize(res_data.size() + matches[it.first].length());
							memcpy(&res_data[res_offset], matches[it.first].data(), matches[it.first].length());
							res_offset += matches[it.first].length();
						}
						else
						{
							res_data.resize(res_data.size() + it.second.size());
							memcpy(&res_data[res_offset], it.second.data(), it.second.size());
							res_offset += it.second.size();
						}
					}
					if (replaceOne || match.length() == 0)
						can_finish_current_string = true;
				} else
					can_finish_current_string = true;

				/// Если пора, копируем все символы до конца строки
				if (can_finish_current_string)
				{
					res_data.resize(res_data.size() + input.length() - start_pos);
					memcpy(&res_data[res_offset], input.data() + start_pos, input.length() - start_pos);
					res_offset += input.length() - start_pos;
					res_offsets[id] = res_offset;
					start_pos = input.length();
				}
			}
			res_data.resize(res_data.size() + 1);
			res_data[res_offset++] = 0;
			res_offsets[id] = res_offset;

		}
	}

	static void constant(const std::string & data, const std::string & needle, const std::string & replacement,
		std::string & res_data)
	{
		RE2 searcher(needle);
		int capture = std::min(searcher.NumberOfCapturingGroups() + 1, 10);
		re2::StringPiece matches[10];

		Instructions instructions;
		split(replacement, instructions);

		for (const auto & it : instructions)
			if (it.first >= capture)
				throw Exception("Invalid replace instruction in replacement string. Id: " + toString(it.first) +
				", but regexp has only " + toString(capture - 1) + " subpatterns",
					ErrorCodes::BAD_ARGUMENTS);

		int start_pos = 0;
		re2::StringPiece input(data);
		res_data = "";

		while (start_pos < input.length())
		{
			/// Правда ли, что с этой строкой больше не надо выполнять преобразования.
			bool can_finish_current_string = false;

			if (searcher.Match(input, start_pos, input.length(), re2::RE2::Anchor::UNANCHORED, matches, capture))
			{
				const auto & match = matches[0];
				size_t char_to_copy = (match.data() - input.data()) - start_pos;

				/// Копируем данные без изменения
				res_data += data.substr(start_pos, char_to_copy);
				start_pos += char_to_copy + match.length();

				/// Выполняем инструкции подстановки
				for (const auto & it : instructions)
				{
					if (it.first >= 0)
						res_data += matches[it.first].ToString();
					else
						res_data += it.second;
				}

				if (replaceOne || match.length() == 0)
					can_finish_current_string = true;
			} else
				can_finish_current_string = true;

			/// Если пора, копируем все символы до конца строки
			if (can_finish_current_string)
			{
				res_data += data.substr(start_pos);
				start_pos = input.length();
			}
		}
	}
};


/** Заменить все вхождения подстроки needle на строку replacement. needle и replacement - константы.
  */
template <bool replaceOne = false>
struct ReplaceStringImpl
{
	static void vector(const ColumnString::Chars_t & data, const ColumnString::Offsets_t & offsets,
		const std::string & needle, const std::string & replacement,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		ColumnString::Offset_t res_offset = 0;
		res_data.reserve(data.size());
		size_t size = offsets.size();
		res_offsets.resize(size);

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		Volnitsky searcher(needle.data(), needle.size(), end - pos);

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end)
		{
			const UInt8 * match = searcher.search(pos, end - pos);

			/// Копируем данные без изменения
			res_data.resize(res_data.size() + (match - pos));
			memcpy(&res_data[res_offset], pos, match - pos);

			/// Определим, к какому индексу оно относится.
			while (i < offsets.size() && begin + offsets[i] < match)
			{
				res_offsets[i] = res_offset + ((begin + offsets[i]) - pos);
				++i;
			}
			res_offset += (match - pos);

			/// Если дошли до конца, пора остановиться
			if (i == offsets.size())
				break;

			/// Правда ли, что с этой строкой больше не надо выполнять преобразования.
			bool can_finish_current_string = false;

			/// Проверяем, что вхождение не переходит через границы строк.
			if (match + needle.size() < begin + offsets[i])
			{
				res_data.resize(res_data.size() + replacement.size());
				memcpy(&res_data[res_offset], replacement.data(), replacement.size());
				res_offset += replacement.size();
				pos = match + needle.size();
				if (replaceOne)
					can_finish_current_string = true;
			}
			else
			{
				pos = match;
				can_finish_current_string = true;
			}

			if (can_finish_current_string)
			{
				res_data.resize(res_data.size() + (begin + offsets[i] - pos));
				memcpy(&res_data[res_offset], pos, (begin + offsets[i] - pos));
				res_offset += (begin + offsets[i] - pos);
				res_offsets[i] = res_offset;
				pos = begin + offsets[i];
			}
		}
	}

	static void vector_fixed(const ColumnString::Chars_t & data, size_t n,
		const std::string & needle, const std::string & replacement,
		ColumnString::Chars_t & res_data, ColumnString::Offsets_t & res_offsets)
	{
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		ColumnString::Offset_t res_offset = 0;
		size_t size = data.size() / n;
		res_data.reserve(data.size());
		res_offsets.resize(size);

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		Volnitsky searcher(needle.data(), needle.size(), end - pos);

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end)
		{
			const UInt8 * match = searcher.search(pos, end - pos);

			/// Копируем данные без изменения
			res_data.resize(res_data.size() + (match - pos));
			memcpy(&res_data[res_offset], pos, match - pos);

			/// Определим, к какому индексу оно относится.
			while (i < size && begin + n * (i + 1) < match)
			{
				res_offsets[i] = res_offset + ((begin + n * (i + 1)) - pos);
				++i;
			}
			res_offset += (match - pos);

			/// Если дошли до конца, пора остановиться
			if (i == size)
				break;

			/// Правда ли, что с этой строкой больше не надо выполнять преобразования.
			bool can_finish_current_string = false;

			/// Проверяем, что вхождение не переходит через границы строк.
			if (match + needle.size() < begin + n * (i + 1))
			{
				res_data.resize(res_data.size() + replacement.size());
				memcpy(&res_data[res_offset], replacement.data(), replacement.size());
				res_offset += replacement.size();
				pos = match + needle.size();
				if (replaceOne)
					can_finish_current_string = true;
			}
			else
			{
				pos = match;
				can_finish_current_string = true;
			}

			if (can_finish_current_string)
			{
				res_data.resize(res_data.size() + (begin + n * (i + 1) - pos));
				memcpy(&res_data[res_offset], pos, (begin + n * (i + 1) - pos));
				res_offset += (begin + n * (i + 1) - pos);
				res_offsets[i] = res_offset;
				pos = begin + n * (i + 1);
			}
		}
	}

	static void constant(const std::string & data, const std::string & needle, const std::string & replacement,
		std::string & res_data)
	{
		res_data = "";
		int replace_cnt = 0;
		for (size_t i = 0; i < data.size(); ++i)
		{
			bool match = true;
			if (i + needle.size() > data.size() || (replaceOne && replace_cnt > 0))
				match = false;
			for (size_t j = 0; match && j < needle.size(); ++j)
				if (data[i + j] != needle[j])
					match = false;
			if (match)
			{
				++replace_cnt;
				res_data += replacement;
				i = i + needle.size() - 1;
			} else
				res_data += data[i];
		}
	}
};


template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionStringReplace; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column_src = block.getByPosition(arguments[0]).column;
		const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;
		const ColumnPtr column_replacement = block.getByPosition(arguments[2]).column;

		if (!column_needle->isConst() || !column_replacement->isConst())
			throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.");

		const IColumn * c1 = &*block.getByPosition(arguments[1]).column;
		const IColumn * c2 = &*block.getByPosition(arguments[2]).column;
		const ColumnConstString * c1_const = typeid_cast<const ColumnConstString *>(c1);
		const ColumnConstString * c2_const = typeid_cast<const ColumnConstString *>(c2);
		String needle = c1_const->getData();
		String replacement = c2_const->getData();

		if (needle.size() == 0)
			throw Exception("Length of the second argument of function replace must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column_src))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;
			Impl::vector(col->getChars(), col->getOffsets(),
				needle, replacement,
				col_res->getChars(), col_res->getOffsets());
		}
		else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(&*column_src))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;
			Impl::vector_fixed(col->getChars(), col->getN(),
				needle, replacement,
				col_res->getChars(), col_res->getOffsets());
		}
		else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column_src))
		{
			String res;
			Impl::constant(col->getData(), needle, replacement, res);
			ColumnConstString * col_res = new ColumnConstString(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionsStringSearch; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
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

		const ColumnConstString * col_needle = typeid_cast<const ColumnConstString *>(&*column_needle);
		if (!col_needle)
			throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			typename ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			Impl::vector(col->getChars(), col->getOffsets(), col_needle->getData(), vec_res);
		}
		else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
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


template <typename Impl, typename Name>
class FunctionsStringSearchToString : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionsStringSearchToString; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 2.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

		const ColumnConstString * col_needle = typeid_cast<const ColumnConstString *>(&*column_needle);
		if (!col_needle)
			throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & vec_res = col_res->getChars();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
			Impl::vector(col->getChars(), col->getOffsets(), col_needle->getData(), vec_res, offsets_res);
		}
		else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
		{
			const std::string & data = col->getData();
			ColumnString::Chars_t vdata(
				reinterpret_cast<const ColumnString::Chars_t::value_type *>(data.c_str()),
				reinterpret_cast<const ColumnString::Chars_t::value_type *>(data.c_str() + data.size() + 1));
			ColumnString::Offsets_t offsets(1, vdata.size());
			ColumnString::Chars_t res_vdata;
			ColumnString::Offsets_t res_offsets;
			Impl::vector(vdata, offsets, col_needle->getData(), res_vdata, res_offsets);

			std::string res;

			if (!res_offsets.empty())
				res.assign(&res_vdata[0], &res_vdata[res_vdata.size() - 1]);

			ColumnConstString * col_res = new ColumnConstString(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NamePosition 		{ static constexpr auto name = "position"; };
struct NamePositionUTF8		{ static constexpr auto name = "positionUTF8"; };
struct NameMatch			{ static constexpr auto name = "match"; };
struct NameLike				{ static constexpr auto name = "like"; };
struct NameNotLike			{ static constexpr auto name = "notLike"; };
struct NameExtract			{ static constexpr auto name = "extract"; };
struct NameReplaceOne		{ static constexpr auto name = "replaceOne"; };
struct NameReplaceAll		{ static constexpr auto name = "replaceAll"; };
struct NameReplaceRegexpOne	{ static constexpr auto name = "replaceRegexpOne"; };
struct NameReplaceRegexpAll	{ static constexpr auto name = "replaceRegexpAll"; };

typedef FunctionsStringSearch<PositionImpl, 			NamePosition> 		FunctionPosition;
typedef FunctionsStringSearch<PositionUTF8Impl, 		NamePositionUTF8> 	FunctionPositionUTF8;
typedef FunctionsStringSearch<MatchImpl<false>, 		NameMatch> 			FunctionMatch;
typedef FunctionsStringSearch<MatchImpl<true>, 			NameLike> 			FunctionLike;
typedef FunctionsStringSearch<MatchImpl<true, true>, 	NameNotLike> 		FunctionNotLike;
typedef FunctionsStringSearchToString<ExtractImpl, 		NameExtract> 		FunctionExtract;
typedef FunctionStringReplace<ReplaceStringImpl<true>,		NameReplaceOne>		FunctionReplaceOne;
typedef FunctionStringReplace<ReplaceStringImpl<false>,		NameReplaceAll>		FunctionReplaceAll;
typedef FunctionStringReplace<ReplaceRegexpImpl<true>,		NameReplaceRegexpOne>		FunctionReplaceRegexpOne;
typedef FunctionStringReplace<ReplaceRegexpImpl<false>,		NameReplaceRegexpAll>		FunctionReplaceRegexpAll;

}
