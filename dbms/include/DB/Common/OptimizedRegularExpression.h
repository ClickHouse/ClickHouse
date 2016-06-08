#pragma once

#include <string>
#include <vector>
#include <memory>

#include <re2/re2.h>
#include <re2_st/re2.h>


/** Использует два способа оптимизации регулярного выражения:
  * 1. Если регулярное выражение является тривиальным (сводится к поиску подстроки в строке),
  *     то заменяет поиск на strstr или strcasestr.
  * 2. Если регулярное выражение содержит безальтернативную подстроку достаточной длины,
  *     то перед проверкой используется strstr или strcasestr достаточной длины;
  *     регулярное выражение проверяется полностью только если подстрока найдена.
  * 3. В остальных случаях, используется движок re2.
  *
  * Это имеет смысл, так как strstr и strcasestr в libc под Linux хорошо оптимизированы.
  *
  * Подходит, если одновременно выполнены следующие условия:
  * - если в большинстве вызовов, регулярное выражение не матчится;
  * - если регулярное выражение совместимо с движком re2;
  * - можете использовать на свой риск, так как, возможно, не все случаи учтены.
  */

namespace OptimizedRegularExpressionDetails
{
	struct Match
	{
		std::string::size_type offset;
		std::string::size_type length;
	};
}

template <bool thread_safe>
class OptimizedRegularExpressionImpl
{
public:
	enum Options
	{
		RE_CASELESS		= 0x00000001,
		RE_NO_CAPTURE	= 0x00000010,
		RE_DOT_NL		= 0x00000100
	};

	using Match = OptimizedRegularExpressionDetails::Match;
	using MatchVec = std::vector<Match>;

	using RegexType = typename std::conditional<thread_safe, re2::RE2, re2_st::RE2>::type;
	using StringPieceType = typename std::conditional<thread_safe, re2::StringPiece, re2_st::StringPiece>::type;

	OptimizedRegularExpressionImpl(const std::string & regexp_, int options = 0);

	bool match(const std::string & subject) const
	{
		return match(subject.data(), subject.size());
	}

	bool match(const std::string & subject, Match & match_) const
	{
		return match(subject.data(), subject.size(), match_);
	}

	unsigned match(const std::string & subject, MatchVec & matches) const
	{
		return match(subject.data(), subject.size(), matches);
	}

	unsigned match(const char * subject, size_t subject_size, MatchVec & matches) const
	{
		return match(subject, subject_size, matches, number_of_subpatterns + 1);
	}

	bool match(const char * subject, size_t subject_size) const;
	bool match(const char * subject, size_t subject_size, Match & match) const;
	unsigned match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const;

	unsigned getNumberOfSubpatterns() const { return number_of_subpatterns; }

	/// Получить регексп re2 или nullptr, если шаблон тривиален (для вывода в лог).
	const std::unique_ptr<RegexType>& getRE2() const { return re2; }

	static void analyze(const std::string & regexp_, std::string & required_substring, bool & is_trivial, bool & required_substring_is_prefix);

	void getAnalyzeResult(std::string & out_required_substring, bool & out_is_trivial, bool & out_required_substring_is_prefix) const
	{
		out_required_substring = required_substring;
		out_is_trivial = is_trivial;
		out_required_substring_is_prefix = required_substring_is_prefix;
	}

private:
	bool is_trivial;
	bool required_substring_is_prefix;
	bool is_case_insensitive;
	std::string required_substring;
	std::unique_ptr<RegexType> re2;
	unsigned number_of_subpatterns;
};

using OptimizedRegularExpression = OptimizedRegularExpressionImpl<true>;

#include "OptimizedRegularExpression.inl"
