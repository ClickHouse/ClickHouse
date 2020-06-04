#include <common/ReplxxLineReader.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <functional>

namespace
{

/// Trim ending whitespace inplace
void trim(String & s)
{
    s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

}

ReplxxLineReader::ReplxxLineReader(
    const Suggest & suggest,
    const String & history_file_path_,
    bool multiline_,
    Patterns extenders_,
    Patterns delimiters_,
    replxx::Replxx::highlighter_callback_t highlighter_)
    : LineReader(history_file_path_, multiline_, std::move(extenders_), std::move(delimiters_)), highlighter(std::move(highlighter_))
{
    using namespace std::placeholders;
    using Replxx = replxx::Replxx;

    if (!history_file_path.empty())
        rx.history_load(history_file_path);

    auto callback = [&suggest] (const String & context, size_t context_size)
    {
        auto range = suggest.getCompletions(context, context_size);
        return Replxx::completions_t(range.first, range.second);
    };

    rx.set_completion_callback(callback);
    rx.set_complete_on_empty(false);
    rx.set_word_break_characters(word_break_characters);

    if (highlighter)
        rx.set_highlighter_callback(highlighter);

    /// By default C-p/C-n binded to COMPLETE_NEXT/COMPLETE_PREV,
    /// bind C-p/C-n to history-previous/history-next like readline.
    rx.bind_key(Replxx::KEY::control('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_NEXT, code); });
    rx.bind_key(Replxx::KEY::control('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::HISTORY_PREVIOUS, code); });
    /// By default COMPLETE_NEXT/COMPLETE_PREV was binded to C-p/C-n, re-bind
    /// to M-P/M-N (that was used for HISTORY_COMMON_PREFIX_SEARCH before, but
    /// it also binded to M-p/M-n).
    rx.bind_key(Replxx::KEY::meta('N'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_NEXT, code); });
    rx.bind_key(Replxx::KEY::meta('P'), [this](char32_t code) { return rx.invoke(Replxx::ACTION::COMPLETE_PREVIOUS, code); });
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

void ReplxxLineReader::enableBracketedPaste()
{
    rx.enable_bracketed_paste();
};
