#pragma once

#include <string>
#include <vector>

namespace DB
{

/// Interactive inline picker over `history` (oldest first) for Ctrl-R.
/// Expects the terminal in raw mode and the cursor on the last line of the input: draws below it and returns the
/// cursor to that line. Returns the picked entry, or an empty string on abort.
std::string pickFromHistory(int in_fd, int out_fd, const std::string & initial_query, const std::vector<std::string> & history);

}
