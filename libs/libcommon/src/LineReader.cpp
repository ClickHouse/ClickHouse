#include <common/LineReader.h>

#ifdef USE_REPLXX
#   include <replxx.hxx>
#endif

#include <iostream>

#include <port/unistd.h>
#include <string.h>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

/// Check if multi-line query is inserted from the paste buffer.
/// Allows delaying the start of query execution until the entirety of query is inserted.
bool hasInputData()
{
    timeval timeout = {0, 0};
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(STDIN_FILENO, &fds);
    return select(1, &fds, nullptr, nullptr, &timeout) == 1;
}

}

LineReader::Suggest::WordsRange LineReader::Suggest::getCompletions(const String & prefix, size_t prefix_length) const
{
    if (!ready)
        return std::make_pair(words.end(), words.end());

    return std::equal_range(
        words.begin(), words.end(), prefix, [prefix_length](const std::string & s, const std::string & prefix_searched)
        {
            return strncmp(s.c_str(), prefix_searched.c_str(), prefix_length) < 0;
        });
}

LineReader::LineReader(const Suggest * suggest, const String & history_file_path_, char extender_, char delimiter_)
    : history_file_path(history_file_path_), extender(extender_), delimiter(delimiter_)
{
#ifdef USE_REPLXX
    impl = new replxx::Replxx;
    auto & rx = *(replxx::Replxx*)(impl);

    if (!history_file_path.empty())
        rx.history_load(history_file_path);

    auto callback = [suggest] (const String & context, size_t context_size)
    {
        auto range = suggest->getCompletions(context, context_size);
        return replxx::Replxx::completions_t(range.first, range.second);
    };

    if (suggest)
    {
        rx.set_completion_callback(callback);
        rx.set_complete_on_empty(false);
        rx.set_word_break_characters(" \t\n\r\"\\'`@$><=;|&{(.");
    }
#endif
    /// FIXME: check extender != delimiter
}

LineReader::~LineReader()
{
#ifdef USE_REPLXX
    auto & rx = *(replxx::Replxx*)(impl);
    if (!history_file_path.empty())
        rx.history_save(history_file_path);
    delete (replxx::Replxx *)impl;
#endif
}

String LineReader::readLine(const String & first_prompt, const String & second_prompt)
{
    String line;
    bool is_multiline = false;

    while (auto status = readOneLine(is_multiline ? second_prompt : first_prompt))
    {
        if (status == RESET_LINE)
        {
            line.clear();
            is_multiline = false;
            continue;
        }

        if (input.empty())
            continue;

        is_multiline = (input.back() == extender) || (delimiter && input.back() != delimiter) || hasInputData();

        if (input.back() == extender)
        {
            input = input.substr(0, input.size() - 1);
            trim(input);
            if (input.empty())
                continue;
        }

        line += (line.empty() ? "" : " ") + input;

        if (!is_multiline)
        {
            if (line != prev_line)
            {
                addToHistory(line);
                prev_line = line;
            }

            return line;
        }
    }

    return {};
}

LineReader::InputStatus LineReader::readOneLine(const String & prompt)
{
    input.clear();

#ifdef USE_REPLXX
    auto & rx = *(replxx::Replxx*)(impl);
    const char* cinput = rx.input(prompt);
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;
#else
    std::cout << prompt;
    std::getline(std::cin, input);
    if (!std::cin.good())
        return ABORT;
#endif

    trim(input);
    return INPUT_LINE;
}

void LineReader::addToHistory(const String & line)
{
#ifdef USE_REPLXX
    auto & rx = *(replxx::Replxx*)(impl);
    rx.history_add(line);
#endif
}
