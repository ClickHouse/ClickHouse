#include <Client/LineReader.h>

#include <iostream>
#include <string_view>
#include <algorithm>

#include <cstring>
#include <unistd.h>
#include <poll.h>


#pragma clang diagnostic ignored "-Wreserved-identifier"

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

struct NoCaseCompare
{
    bool operator()(const std::string & str1, const std::string & str2)
    {
        return std::lexicographical_compare(begin(str1), end(str1), begin(str2), end(str2), [](const char c1, const char c2)
        {
            return std::tolower(c1) < std::tolower(c2);
        });
    }
};

using Words = std::vector<std::string>;
template <class Compare>
void addNewWords(Words & to, const Words & from, Compare comp)
{
    size_t old_size = to.size();
    size_t new_size = old_size + from.size();

    to.reserve(new_size);
    to.insert(to.end(), from.begin(), from.end());
    auto middle = to.begin() + old_size;
    std::inplace_merge(to.begin(), middle, to.end(), comp);

    auto last_unique = std::unique(to.begin(), to.end());
    to.erase(last_unique, to.end());
}

}

namespace DB
{

/// Check if multi-line query is inserted from the paste buffer.
/// Allows delaying the start of query execution until the entirety of query is inserted.
bool LineReader::hasInputData() const
{
    pollfd pfd{.fd = in_fd, .events = POLLIN, .revents = 0};
    return poll(&pfd, 1, 0) == 1 && (pfd.revents & POLLIN);
}

LineReader::Suggest::Words LineReader::Suggest::getMatchingWords(
    const String & prefix, size_t prefix_length, const char * word_break_characters,
    const Words & priority_words, bool & last_word_empty)
{
    std::string_view last_word;

    auto last_word_pos = prefix.find_last_of(word_break_characters);
    if (std::string::npos == last_word_pos)
        last_word = prefix;
    else
        last_word = std::string_view{prefix}.substr(last_word_pos + 1, std::string::npos);

    last_word_empty = last_word.empty();

    std::pair<Words::const_iterator, Words::const_iterator> range;

    Words to_search;
    Words recent;
    bool no_case = false;

    {
        std::lock_guard lock(mutex);
        /// Only perform case sensitive completion when the prefix string contains any uppercase characters
        if (std::none_of(prefix.begin(), prefix.end(), [](char32_t x) { return iswupper(static_cast<wint_t>(x)); }))
        {
            to_search = words_no_case;
            no_case = true;
        }
        else
            to_search = words;

        recent.assign(recently_used.begin(), recently_used.end());
    }

    if (custom_completions_callback)
    {
        auto new_words = custom_completions_callback(prefix, prefix_length);
        chassert(std::is_sorted(new_words.begin(), new_words.end()));
        addNewWords(to_search, new_words, std::less<std::string>{});
    }

    if (no_case)
        range = std::equal_range(
            to_search.begin(), to_search.end(), last_word, [prefix_length](std::string_view s, std::string_view prefix_searched)
            {
                return strncasecmp(s.data(), prefix_searched.data(), prefix_length) < 0; /// NOLINT(bugprone-suspicious-stringview-data-usage)
            });
    else
        range = std::equal_range(
            to_search.begin(), to_search.end(), last_word, [prefix_length](std::string_view s, std::string_view prefix_searched)
            {
                return strncmp(s.data(), prefix_searched.data(), prefix_length) < 0; /// NOLINT(bugprone-suspicious-stringview-data-usage)
            });

    Words result(range.first, range.second);

    /// When matching case-insensitively, membership and deduplication are compared
    /// case-insensitively too (consistent with the prefix matching above).
    auto fold = [no_case](std::string_view s)
    {
        std::string folded(s);
        if (no_case)
            std::transform(folded.begin(), folded.end(), folded.begin(),
                [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return folded;
    };

    std::unordered_set<std::string> recent_set;
    for (const auto & w : recent)
        recent_set.insert(fold(w));
    std::unordered_set<std::string> priority_set;
    for (const auto & w : priority_words)
        priority_set.insert(fold(w));

    /// Identifiers already present in the current query (aliases, column names) and identifiers
    /// used earlier this session are eligible candidates even when they are not in the loaded
    /// suggestion dictionary — otherwise a query-only alias such as `SELECT veryUniqueAlias AS x,
    /// veryU` could never be hinted or completed, only re-ranked when it also exists in
    /// `system.completions`. Add those that match the typed prefix (with the same case rules as the
    /// dictionary lookup), skip the token currently being typed, and dedup against the dictionary
    /// matches and each other.
    if (!last_word_empty && (!recent_set.empty() || !priority_set.empty()))
    {
        auto prefix_matches = [&](const std::string & w)
        {
            if (w.size() < prefix_length || last_word.size() < prefix_length)
                return false;
            return no_case
                ? strncasecmp(w.data(), last_word.data(), prefix_length) == 0 /// NOLINT(bugprone-suspicious-stringview-data-usage)
                : strncmp(w.data(), last_word.data(), prefix_length) == 0; /// NOLINT(bugprone-suspicious-stringview-data-usage)
        };

        std::unordered_set<std::string> present;
        present.reserve(result.size());
        for (const auto & w : result)
            present.insert(fold(w));

        const std::string typed = fold(last_word);
        auto add_candidates = [&](const Words & extra)
        {
            for (const auto & w : extra)
            {
                if (!prefix_matches(w))
                    continue;
                std::string folded = fold(w);
                if (folded == typed)
                    continue; /// the word being typed completes to itself - nothing to offer
                if (present.insert(folded).second)
                    result.push_back(w);
            }
        };
        /// Query-local identifiers first, then session-recent ones (their tiers are applied below).
        add_candidates(priority_words);
        add_candidates(recent);
    }

    /// Prioritize words that the user has used or already typed.
    if (!result.empty() && (!recent_set.empty() || !priority_set.empty()))
    {
        /// Tier 0: used earlier this session; tier 1: present in the current input; tier 2: rest.
        /// Compute the tier once per word, then a stable sort preserves the alphabetical order
        /// within each tier.
        std::vector<std::pair<int, std::string>> ranked;
        ranked.reserve(result.size());
        for (auto & w : result)
        {
            std::string folded = fold(w);
            int tier = 2;
            if (recent_set.contains(folded))
                tier = 0;
            else if (priority_set.contains(folded))
                tier = 1;
            ranked.emplace_back(tier, std::move(w));
        }
        std::stable_sort(ranked.begin(), ranked.end(),
            [](const auto & a, const auto & b) { return a.first < b.first; });

        result.clear();
        for (auto & p : ranked)
            result.push_back(std::move(p.second));
    }

    return result;
}

replxx::Replxx::completions_t LineReader::Suggest::getCompletions(
    const String & prefix, size_t prefix_length, const char * word_break_characters, const Words & priority_words)
{
    bool last_word_empty = false;
    Words matched = getMatchingWords(prefix, prefix_length, word_break_characters, priority_words, last_word_empty);
    return replxx::Replxx::completions_t(matched.begin(), matched.end());
}

void LineReader::Suggest::addUsedWords(const Words & used)
{
    if (used.empty())
        return;
    std::lock_guard lock(mutex);
    for (const auto & word : used)
        recently_used.insert(word);
}

void LineReader::Suggest::addWords(Words && new_words) // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
{
    Words new_words_no_case = new_words;
    if (!new_words.empty())
    {
        std::sort(new_words.begin(), new_words.end());
        std::sort(new_words_no_case.begin(), new_words_no_case.end(), NoCaseCompare{});
    }

    {
        std::lock_guard lock(mutex);
        addNewWords(words, new_words, std::less<std::string>{});
        addNewWords(words_no_case, new_words_no_case, NoCaseCompare{});

        chassert(std::is_sorted(words.begin(), words.end()));
        chassert(std::is_sorted(words_no_case.begin(), words_no_case.end(), NoCaseCompare{}));
    }
}

LineReader::LineReader
(
    const String & history_file_path_,
    bool multiline_,
    Patterns extenders_,
    Patterns delimiters_,
    std::istream & input_stream_,
    std::ostream & output_stream_,
    int in_fd_
)
    : history_file_path(history_file_path_)
    , multiline(multiline_)
    , extenders(std::move(extenders_))
    , delimiters(std::move(delimiters_))
    , input_stream(input_stream_)
    , output_stream(output_stream_)
    , in_fd(in_fd_)
{
    /// FIXME: check extender != delimiter
}

String LineReader::readLine(const String & first_prompt, const String & second_prompt)
{
    String line;
    bool need_next_line = false;

    while (auto status = readOneLine(need_next_line ? second_prompt : first_prompt))
    {
        if (status == RESET_LINE)
        {
            line.clear();
            need_next_line = false;
            continue;
        }

        if (input.empty())
        {
            if (!line.empty() && !multiline && !hasInputData())
                break;
            continue;
        }

        const char * has_extender = nullptr;
        for (const auto * extender : extenders)
        {
            if (input.ends_with(extender))
            {
                has_extender = extender;
                break;
            }
        }

        const char * has_delimiter = nullptr;
        for (const auto * delimiter : delimiters)
        {
            if (input.ends_with(delimiter))
            {
                has_delimiter = delimiter;
                break;
            }
        }

        need_next_line = has_extender || (multiline && !has_delimiter) || hasInputData();

        if (has_extender)
        {
            input.resize(input.size() - strlen(has_extender));
            trim(input);
            if (input.empty())
                continue;
        }

        line += (line.empty() ? "" : "\n") + input;

        if (!need_next_line)
            break;
    }

    if (!line.empty() && line != prev_line && !line.starts_with(" "))
    {
        addToHistory(line);
        prev_line = line;
    }

    return line;
}

LineReader::InputStatus LineReader::readOneLine(const String & prompt)
{
    input.clear();

    {
        output_stream << prompt;
        std::getline(input_stream, input);
        if (!input_stream.good())
            return ABORT;
    }

    trim(input);
    return INPUT_LINE;
}

}
