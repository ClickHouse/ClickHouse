#include <string>

/// Ask a question in the terminal and expect either 'y' or 'n' as an answer.
[[nodiscard]] bool ask(std::string question);
