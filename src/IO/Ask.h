#pragma once

#include <string>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

/// Ask a question in the terminal and expect either 'y' or 'n' as an answer.
/// There are two implementations: use each of them where appropriate.
/// The one with ReadBuffer/WriteBuffer is for Client application while
/// the one with std::cin/std::cout is for helper scripts mostly.
[[nodiscard]] bool ask(std::string question, ReadBuffer & in, WriteBuffer & out);
[[nodiscard]] bool ask(std::string question);
}
