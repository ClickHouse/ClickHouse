#include <common/ReplxxLineReader.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

}

ReplxxLineReader::ReplxxLineReader(const Suggest & suggest, const String & history_file_path_, char extender_, char delimiter_)
    : LineReader(history_file_path_, extender_, delimiter_)
{
    if (!history_file_path.empty())
        rx.history_load(history_file_path);

    auto callback = [&suggest] (const String & context, size_t context_size)
    {
        auto range = suggest.getCompletions(context, context_size);
        return replxx::Replxx::completions_t(range.first, range.second);
    };

    rx.set_completion_callback(callback);
    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);
}

ReplxxLineReader::~ReplxxLineReader()
{
    if (!history_file_path.empty())
        rx.history_save(history_file_path);
}

LineReader::InputStatus ReplxxLineReader::readOneLine(const String & prompt)
{
    input.clear();

    const char* cinput = rx.input(prompt);
    if (cinput == nullptr)
        return (errno != EAGAIN) ? ABORT : RESET_LINE;
    input = cinput;

    trim(input);
    return INPUT_LINE;
}

void ReplxxLineReader::addToHistory(const String & line)
{
    rx.history_add(line);
}
