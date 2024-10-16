#include <Client/LineReader.h>

#include <iostream>
#include <string_view>
#include <algorithm>

#include <cassert>
#include <cstring>
#include <unistd.h>
#include <poll.h>
#include <sys/time.h>
#include <sys/types.h>


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
    pollfd fd{in_fd, POLLIN, 0};
    return poll(&fd, 1, 0) == 1;
}

replxx::Replxx::completions_t LineReader::Suggest::getCompletions(const String & prefix, size_t prefix_length, const char * word_break_characters)
{
    std::string_view last_word;

    auto last_word_pos = prefix.find_last_of(word_break_characters);
    if (std::string::npos == last_word_pos)
        last_word = prefix;
    else
        last_word = std::string_view{prefix}.substr(last_word_pos + 1, std::string::npos);
    /// last_word can be empty.

    std::pair<Words::const_iterator, Words::const_iterator> range;

    std::lock_guard lock(mutex);

    Words to_search;
    bool no_case = false;
    /// Only perform case sensitive completion when the prefix string contains any uppercase characters
    if (std::none_of(prefix.begin(), prefix.end(), [](char32_t x) { return iswupper(static_cast<wint_t>(x)); }))
    {
        to_search = words_no_case;
        no_case = true;
    }
    else
        to_search = words;

    if (custom_completions_callback)
    {
        auto new_words = custom_completions_callback(prefix, prefix_length);
        assert(std::is_sorted(new_words.begin(), new_words.end()));
        addNewWords(to_search, new_words, std::less<std::string>{});
    }

    if (no_case)
        range = std::equal_range(
            to_search.begin(), to_search.end(), last_word, [prefix_length](std::string_view s, std::string_view prefix_searched)
            {
                return strncasecmp(s.data(), prefix_searched.data(), prefix_length) < 0;
            });
    else
        range = std::equal_range(
            to_search.begin(), to_search.end(), last_word, [prefix_length](std::string_view s, std::string_view prefix_searched)
            {
                return strncmp(s.data(), prefix_searched.data(), prefix_length) < 0;
            });

    return replxx::Replxx::completions_t(range.first, range.second);
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

        assert(std::is_sorted(words.begin(), words.end()));
        assert(std::is_sorted(words_no_case.begin(), words_no_case.end(), NoCaseCompare{}));
    }
}

LineReader::LineReader(
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

    if (!line.empty() && line != prev_line)
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
