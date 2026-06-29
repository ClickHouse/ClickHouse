#include <Utils.h>

std::vector<std::string> splitOnUnquotedSemicolons(const std::string & input)
{
    std::vector<std::string> results;
    std::string current;
    bool in_single_quote = false;
    bool in_double_quote = false;
    bool escaped = false; // Tracks whether the *previous* character was a backslash (escape)

    for (const char c : input)
    {
        if (escaped)
        {
            // The current character is escaped; just add it literally to 'current'
            current.push_back(c);
            escaped = false; // Reset escape state
        }
        else
        {
            // We're NOT currently escaping. Check what we have:
            if (c == '\\')
            {
                // A backslash can start an escape sequence unless we are in single quotes.
                // (POSIX shells treat backslash inside single quotes as literal)
                if (!in_single_quote)
                {
                    escaped = true;
                }
                else
                {
                    // In single quotes, backslash is literal:
                    current.push_back(c);
                }
            }
            else if (c == '\'' && !in_double_quote)
            {
                // Toggle single-quote mode
                in_single_quote = !in_single_quote;
                current.push_back(c);
            }
            else if (c == '\"' && !in_single_quote)
            {
                // Toggle double-quote mode
                in_double_quote = !in_double_quote;
                current.push_back(c);
            }
            else if (c == ';' && !in_single_quote && !in_double_quote)
            {
                // We found an unquoted semicolon -> finalize the current segment
                results.push_back(current);
                current.clear();
            }
            else
            {
                // Normal character, just add it
                current.push_back(c);
            }
        }
    }

    // Add whatever remains in 'current'
    if (!current.empty())
    {
        results.push_back(current);
    }

    return results;
}
