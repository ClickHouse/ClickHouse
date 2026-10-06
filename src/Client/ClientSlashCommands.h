#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <base/types.h>


namespace DB
{

/// A `/`-prefixed meta-command of the client, such as `/help`. The client runs it itself instead of
/// sending it to the server, and the line editor offers the commands as as-you-type hints and as Tab
/// completions.
struct ClientSlashCommand
{
    std::string_view name;
    /// Whether an argument may follow the name, as in `/help MergeTree`.
    bool takes_argument;
};

/// All the `/`-commands, in the order they are offered in.
/// Keep in sync with the commands dispatched in `ClientBase::processQueryText`.
std::span<const ClientSlashCommand> clientSlashCommands();

/// Diagnose input that looks like a `/`-command but is not one: a misspelled name (with the similar
/// commands suggested) or an argument given to a command that takes none. Returns nothing when the
/// input is a valid command, or when it is not a command at all (a `/* comment */`, ...) - it is then
/// left to the SQL parser as before. The input has to be trimmed of whitespace and `;` already.
std::optional<String> diagnoseClientSlashCommand(std::string_view trimmed_input);

/// The `/`-commands matching what is being typed; see `matchClientSlashCommandPrefix`.
struct ClientSlashCommandMatch
{
    /// The names of the matching commands; empty when the input is not a `/`-command prefix.
    std::vector<std::string> commands;
    /// How much of the input the commands replace: the `/` and everything typed after it. The
    /// commands are ASCII, so this is both a number of bytes and a number of code points (the line
    /// editor counts the context to replace in code points).
    size_t prefix_length = 0;
};

/// Match the beginning of the input up to the cursor against the names of the `/`-commands, for the
/// hints and the completion of the line editor. There is a match only while the command name itself
/// is being typed - leading whitespace is skipped, but as soon as the name is followed by whitespace
/// what is being typed is its argument, and a `/` that is not at the beginning of the input (in a
/// `/* comment */`, on a continuation line, in a path) is not a command at all.
ClientSlashCommandMatch matchClientSlashCommandPrefix(std::string_view text_before_cursor);

}
