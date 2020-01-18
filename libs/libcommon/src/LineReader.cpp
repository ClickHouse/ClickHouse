#include <common/LineReader.h>

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

LineReader::LineReader(const String & history_file_path_, char extender_, char delimiter_)
    : history_file_path(history_file_path_), extender(extender_), delimiter(delimiter_)
{
    /// FIXME: check extender != delimiter
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

    std::cout << prompt;
    std::getline(std::cin, input);
    if (!std::cin.good())
        return ABORT;

    trim(input);
    return INPUT_LINE;
}
