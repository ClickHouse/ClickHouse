#include <Client/ClientSlashCommands.h>

#include <Common/StringUtils.h>

#include <algorithm>


namespace DB
{

std::span<const ClientSlashCommand> clientSlashCommands()
{
    /// Alphabetical, so that the order the commands are offered in is stable and predictable.
    static constexpr ClientSlashCommand commands[]
    {
        {"/clear", false},
        {"/help", true},
        {"/man", true},
    };
    return commands;
}

namespace
{

/// The commands are dispatched case-insensitively, so they are matched case-insensitively here as
/// well (`/HELP` is a command, and it is completed too).
bool startsWithCaseInsensitive(std::string_view s, std::string_view prefix)
{
    return prefix.size() <= s.size() && equalsCaseInsensitive(s.substr(0, prefix.size()), prefix);
}

}

bool isClientSlashCommand(std::string_view trimmed_input)
{
    for (const auto & command : clientSlashCommands())
    {
        if (equalsCaseInsensitive(trimmed_input, command.name))
            return true;
        if (command.takes_argument && trimmed_input.size() > command.name.size()
            && startsWithCaseInsensitive(trimmed_input, command.name) && isWhitespaceASCII(trimmed_input[command.name.size()]))
            return true;
    }
    return false;
}

ClientSlashCommandMatch matchClientSlashCommandPrefix(std::string_view text_before_cursor)
{
    size_t pos = 0;
    auto skip_blanks = [&]
    {
        /// Deliberately not skipping newlines: a `/` on a continuation line of a multiline query is
        /// a part of the query, not a command.
        while (pos < text_before_cursor.size() && isWhitespaceASCIIOneLine(text_before_cursor[pos]))
            ++pos;
    };

    skip_blanks();

    /// The inline form of the AI chat (`? /help`) runs the command as well, so a leading `?` is
    /// skipped. In the `?` mode of the line editor the marker is not a part of the input at all.
    if (pos < text_before_cursor.size() && text_before_cursor[pos] == '?')
    {
        ++pos;
        skip_blanks();
    }

    if (pos >= text_before_cursor.size() || text_before_cursor[pos] != '/')
        return {};

    const std::string_view typed = text_before_cursor.substr(pos);

    /// Only the name of the command is completed: what follows it is its argument.
    if (std::any_of(typed.begin(), typed.end(), [](char c) { return isWhitespaceASCII(c); }))
        return {};

    ClientSlashCommandMatch match;
    for (const auto & command : clientSlashCommands())
        if (startsWithCaseInsensitive(command.name, typed))
            match.commands.emplace_back(command.name);

    if (!match.commands.empty())
        match.prefix_length = typed.size();

    return match;
}

}
