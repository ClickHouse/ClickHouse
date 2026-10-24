#pragma once

#include <string>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// Ask a question in the terminal and expect either 'y' or 'n' as an answer.
/// `default_yes` selects the answer for an empty reply (the user just pressing Enter):
/// `false` (the default) means Enter is "no" (for a `[y/N]` prompt), `true` means Enter
/// is "yes" (for a `[Y/n]` prompt).
/// There are two implementations: use each of them where appropriate.
/// The one with ReadBuffer/WriteBuffer is for Client application while
/// the one with std::cin/std::cout is for helper scripts mostly.
[[nodiscard]] bool ask(std::string question, ReadBuffer & in, WriteBuffer & out, bool default_yes = false);
[[nodiscard]] bool ask(std::string question, bool default_yes = false);
}
