#include <iostream>

#include <Poco/Exception.h>

#include <Common/OptimizedRegularExpression.h>


#define MIN_LENGTH_FOR_STRSTR 3
#define MAX_SUBPATTERNS 5


template <bool thread_safe>
void OptimizedRegularExpressionImpl<thread_safe>::analyze(
    const std::string & regexp,
    std::string & required_substring,
    bool & is_trivial,
    bool & required_substring_is_prefix)
{
    /** The expression is trivial if all the metacharacters in it are escaped.
      * The non-alternative string is
      *  a string outside parentheses,
      *  in which all metacharacters are escaped,
      *  and also if there are no '|' outside the brackets,
      *  and also avoid substrings of the form `http://` or `www` and some other
      *   (this is the hack for typical use case in Yandex.Metrica).
      */
    const char * begin = regexp.data();
    const char * pos = begin;
    const char * end = regexp.data() + regexp.size();
    int depth = 0;
    is_trivial = true;
    required_substring_is_prefix = false;
    required_substring.clear();
    bool has_alternative_on_depth_0 = false;

    /// Substring with a position.
    using Substring = std::pair<std::string, size_t>;
    using Substrings = std::vector<Substring>;

    Substrings trivial_substrings(1);
    Substring * last_substring = &trivial_substrings.back();

    bool in_curly_braces = false;
    bool in_square_braces = false;

    while (pos != end)
    {
        switch (*pos)
        {
            case '\0':
                pos = end;
                break;

            case '\\':
            {
                ++pos;
                if (pos == end)
                    break;

                switch (*pos)
                {
                    case '|': case '(': case ')': case '^': case '$': case '.': case '[': case '?': case '*': case '+': case '{':
                        if (depth == 0 && !in_curly_braces && !in_square_braces)
                        {
                            if (last_substring->first.empty())
                                last_substring->second = pos - begin;
                            last_substring->first.push_back(*pos);
                        }
                        break;
                    default:
                        /// all other escape sequences are not supported
                        is_trivial = false;
                        if (!last_substring->first.empty())
                        {
                            trivial_substrings.resize(trivial_substrings.size() + 1);
                            last_substring = &trivial_substrings.back();
                        }
                        break;
                }

                ++pos;
                break;
            }

            case '|':
                if (depth == 0)
                    has_alternative_on_depth_0 = true;
                is_trivial = false;
                if (!in_square_braces && !last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                ++pos;
                break;

            case '(':
                if (!in_square_braces)
                {
                    ++depth;
                    is_trivial = false;
                    if (!last_substring->first.empty())
                    {
                        trivial_substrings.resize(trivial_substrings.size() + 1);
                        last_substring = &trivial_substrings.back();
                    }
                }
                ++pos;
                break;

            case '[':
                in_square_braces = true;
                ++depth;
                is_trivial = false;
                if (!last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                ++pos;
                break;

            case ']':
                if (!in_square_braces)
                    goto ordinary;

                in_square_braces = false;
                --depth;
                is_trivial = false;
                if (!last_substring->first.empty())
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                ++pos;
                break;

            case ')':
                if (!in_square_braces)
                {
                    --depth;
                    is_trivial = false;
                    if (!last_substring->first.empty())
                    {
                        trivial_substrings.resize(trivial_substrings.size() + 1);
                        last_substring = &trivial_substrings.back();
                    }
                }
                ++pos;
                break;

            case '^': case '$': case '.': case '+':
                is_trivial = false;
                if (!last_substring->first.empty() && !in_square_braces)
                {
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                ++pos;
                break;

            /// Quantifiers that allow a zero number of occurences.
            case '{':
                in_curly_braces = true;
            case '?': case '*':
                is_trivial = false;
                if (!last_substring->first.empty() && !in_square_braces)
                {
                    last_substring->first.resize(last_substring->first.size() - 1);
                    trivial_substrings.resize(trivial_substrings.size() + 1);
                    last_substring = &trivial_substrings.back();
                }
                ++pos;
                break;

            case '}':
                if (!in_curly_braces)
                    goto ordinary;

                in_curly_braces = false;
                ++pos;
                break;

            ordinary:   /// Normal, not escaped symbol.
            default:
                if (depth == 0 && !in_curly_braces && !in_square_braces)
                {
                    if (last_substring->first.empty())
                        last_substring->second = pos - begin;
                    last_substring->first.push_back(*pos);
                }
                ++pos;
                break;
        }
    }

    if (last_substring && last_substring->first.empty())
        trivial_substrings.pop_back();

    if (!is_trivial)
    {
        if (!has_alternative_on_depth_0)
        {
            /** We choose the non-alternative substring of the maximum length, among the prefixes,
              *  or a non-alternative substring of maximum length.
              */
            size_t max_length = 0;
            Substrings::const_iterator candidate_it = trivial_substrings.begin();
            for (Substrings::const_iterator it = trivial_substrings.begin(); it != trivial_substrings.end(); ++it)
            {
                if (((it->second == 0 && candidate_it->second != 0)
                        || ((it->second == 0) == (candidate_it->second == 0) && it->first.size() > max_length))
                    /// Tuning for typical usage domain
                    && (it->first.size() > strlen("://") || strncmp(it->first.data(), "://", strlen("://")))
                    && (it->first.size() > strlen("http://") || strncmp(it->first.data(), "http", strlen("http")))
                    && (it->first.size() > strlen("www.") || strncmp(it->first.data(), "www", strlen("www")))
                    && (it->first.size() > strlen("Windows ") || strncmp(it->first.data(), "Windows ", strlen("Windows "))))
                {
                    max_length = it->first.size();
                    candidate_it = it;
                }
            }

            if (max_length >= MIN_LENGTH_FOR_STRSTR)
            {
                required_substring = candidate_it->first;
                required_substring_is_prefix = candidate_it->second == 0;
            }
        }
    }
    else
    {
        required_substring = trivial_substrings.front().first;
        required_substring_is_prefix = trivial_substrings.front().second == 0;
    }

/*    std::cerr
        << "regexp: " << regexp
        << ", is_trivial: " << is_trivial
        << ", required_substring: " << required_substring
        << ", required_substring_is_prefix: " << required_substring_is_prefix
        << std::endl;*/
}


template <bool thread_safe>
OptimizedRegularExpressionImpl<thread_safe>::OptimizedRegularExpressionImpl(const std::string & regexp_, int options)
{
    analyze(regexp_, required_substring, is_trivial, required_substring_is_prefix);

    /// Just three following options are supported
    if (options & (~(RE_CASELESS | RE_NO_CAPTURE | RE_DOT_NL)))
        throw Poco::Exception("OptimizedRegularExpression: Unsupported option.");

    is_case_insensitive   = options & RE_CASELESS;
    bool is_no_capture    = options & RE_NO_CAPTURE;
    bool is_dot_nl        = options & RE_DOT_NL;

    number_of_subpatterns = 0;
    if (!is_trivial)
    {
        /// Compile the re2 regular expression.
        typename RegexType::Options options;

        if (is_case_insensitive)
            options.set_case_sensitive(false);

        if (is_dot_nl)
            options.set_dot_nl(true);

        re2 = std::make_unique<RegexType>(regexp_, options);
        if (!re2->ok())
            throw Poco::Exception("OptimizedRegularExpression: cannot compile re2: " + regexp_ + ", error: " + re2->error());

        if (!is_no_capture)
        {
            number_of_subpatterns = re2->NumberOfCapturingGroups();
            if (number_of_subpatterns > MAX_SUBPATTERNS)
                throw Poco::Exception("OptimizedRegularExpression: too many subpatterns in regexp: " + regexp_);
        }
    }
}


template <bool thread_safe>
bool OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size) const
{
    if (is_trivial)
    {
        if (is_case_insensitive)
            return nullptr != strcasestr(subject, required_substring.data());
        else
            return nullptr != strstr(subject, required_substring.data());
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return 0;
        }

        return re2->Match(StringPieceType(subject, subject_size), 0, subject_size, RegexType::UNANCHORED, nullptr, 0);
    }
}


template <bool thread_safe>
bool OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size, Match & match) const
{
    if (is_trivial)
    {
        const char * pos;
        if (is_case_insensitive)
            pos = strcasestr(subject, required_substring.data());
        else
            pos = strstr(subject, required_substring.data());

        if (pos == nullptr)
            return 0;
        else
        {
            match.offset = pos - subject;
            match.length = required_substring.size();
            return 1;
        }
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return 0;
        }

        StringPieceType piece;

        if (!RegexType::PartialMatch(StringPieceType(subject, subject_size), *re2, &piece))
            return 0;
        else
        {
            match.offset = piece.data() - subject;
            match.length = piece.length();
            return 1;
        }
    }
}


template <bool thread_safe>
unsigned OptimizedRegularExpressionImpl<thread_safe>::match(const char * subject, size_t subject_size, MatchVec & matches, unsigned limit) const
{
    matches.clear();

    if (limit == 0)
        return 0;

    if (limit > number_of_subpatterns + 1)
        limit = number_of_subpatterns + 1;

    if (is_trivial)
    {
        const char * pos;
        if (is_case_insensitive)
            pos = strcasestr(subject, required_substring.data());
        else
            pos = strstr(subject, required_substring.data());

        if (pos == nullptr)
            return 0;
        else
        {
            Match match;
            match.offset = pos - subject;
            match.length = required_substring.size();
            matches.push_back(match);
            return 1;
        }
    }
    else
    {
        if (!required_substring.empty())
        {
            const char * pos;
            if (is_case_insensitive)
                pos = strcasestr(subject, required_substring.data());
            else
                pos = strstr(subject, required_substring.data());

            if (nullptr == pos)
                return 0;
        }

        StringPieceType pieces[MAX_SUBPATTERNS];

        if (!re2->Match(StringPieceType(subject, subject_size), 0, subject_size, RegexType::UNANCHORED, pieces, limit))
            return 0;
        else
        {
            matches.resize(limit);
            for (size_t i = 0; i < limit; ++i)
            {
                if (pieces[i] != nullptr)
                {
                    matches[i].offset = pieces[i].data() - subject;
                    matches[i].length = pieces[i].length();
                }
                else
                {
                    matches[i].offset = std::string::npos;
                    matches[i].length = 0;
                }
            }
            return limit;
        }
    }
}

#undef MIN_LENGTH_FOR_STRSTR
#undef MAX_SUBPATTERNS

