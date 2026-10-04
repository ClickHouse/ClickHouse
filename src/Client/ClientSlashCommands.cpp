#include <Client/ClientSlashCommands.h>

#include <Common/NamePrompter.h>
#include <Common/StringUtils.h>

#include <fmt/format.h>

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

/// How many commands to suggest for a misspelled name.
constexpr size_t MAX_COMMAND_HINTS = 3;

/// The commands a misspelled name probably meant: the ones it is a prefix of (an unfinished name,
/// which is the most likely mistake) and the ones within a typo distance of it.
VectorWithMemoryTracking<String> getSimilarCommands(std::string_view name)
{
    VectorWithMemoryTracking<String> all_names;
    for (const auto & command : clientSlashCommands())
        all_names.emplace_back(command.name);

    /// `NamePrompter` skips candidates whose length differs too much from the name, so a short
    /// prefix (`/cl`) gets no hint from it - hence the prefix matches are collected separately.
    auto similar = NamePrompter<MAX_COMMAND_HINTS>::getHints(String(name), all_names);

    VectorWithMemoryTracking<String> result;
    for (const auto & command_name : all_names)
    {
        const bool is_prefix = startsWithCaseInsensitive(command_name, name);
        if (is_prefix || std::find(similar.begin(), similar.end(), command_name) != similar.end())
            result.push_back(command_name);
    }
    return result;
}

}

std::optional<String> diagnoseClientSlashCommand(std::string_view trimmed_input)
{
    /// A `/` starts the command list, while `/*` opens a SQL comment.
    if (trimmed_input.empty() || trimmed_input[0] != '/'
        || (trimmed_input.size() > 1 && !isAlphaASCII(trimmed_input[1])))
        return {};

    const auto name_end = std::find_if(trimmed_input.begin(), trimmed_input.end(), [](char c) { return isWhitespaceASCII(c); });
    const size_t name_size = static_cast<size_t>(name_end - trimmed_input.begin());
    const std::string_view name = trimmed_input.substr(0, name_size);
    /// The input is trimmed, so whitespace after the name means that an argument follows it.
    const bool has_argument = name_size < trimmed_input.size();

    for (const auto & command : clientSlashCommands())
    {
        if (!equalsCaseInsensitive(name, command.name))
            continue;
        if (!has_argument || command.takes_argument)
            return {}; /// A valid command, dispatched elsewhere.
        return fmt::format("The `{}` command does not accept an argument", command.name);
    }

    String message = fmt::format("Unknown command `{}`", name);
    message += getHintsErrorMessageSuffix(getSimilarCommands(name));
    message += ". Type `/` at the beginning of the line to see all the commands";
    return message;
}

ClientSlashCommandMatch matchClientSlashCommandPrefix(std::string_view text_before_cursor)
{
    size_t pos = 0;
    /// Deliberately not skipping newlines: a `/` on a continuation line of a multiline query is a
    /// part of the query, not a command.
    while (pos < text_before_cursor.size() && isWhitespaceASCIIOneLine(text_before_cursor[pos]))
        ++pos;

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
