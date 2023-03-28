#include <vector>
#include <iostream>
#include <sstream>
#include <string>

#include <cassert>
#include <cstring>

const char *const CANCELLING_HILITE = "\033[0m";

const std::vector<const char *> HILITES =
    {
        "\033[1m", "\033[0;36m", "\033[0;33m", "\033[1;33m", "\033[0;32m", "\033[1;36m",
        CANCELLING_HILITE
    };

[[maybe_unused]] const char * consume_hilites(const char ** it)
{
    const char * last_hilite = nullptr;
    while (true)
    {
        bool changed_hilite = false;
        for (const char * hilite : HILITES)
        {
            if (std::string_view(*it).starts_with(hilite))
            {
                *it += strlen(hilite);
                changed_hilite = true;
                last_hilite = hilite;
            }
        }
        if (!changed_hilite)
            break;
    }
    return last_hilite;
}

std::string remove_hilites(std::string_view string)
{
    const char * it = string.begin();
    std::stringstream ss;
    while (true)
    {
        consume_hilites(&it);
        if (it == string.end())
            return ss.str();
        ss << *(it++);
    }
}

bool are_equal_with_hilites_removed(std::string_view left, std::string_view right)
{
    return remove_hilites(left) == remove_hilites(right);
}

bool are_equal_with_hilites(std::string_view left, std::string_view right)
{
    // Unnecessary, but helps debug.
    // Comment out for speed.
    //if (!are_equal_with_hilites_removed(left, right))
    //{
    //    std::cerr << "Not equal as strings" << std::endl;
    //    return false;
    //}

    const char * left_it = left.begin();
    const char * right_it = right.begin();
    const char * left_hilite = CANCELLING_HILITE;
    const char * right_hilite = CANCELLING_HILITE;

    while (true)
    {
        // Consume all prefix hilites, update the current hilite to be the last one.
        const char * last_hilite = consume_hilites(&left_it);
        if (last_hilite != nullptr)
            left_hilite = last_hilite;

        last_hilite = consume_hilites(&right_it);
        if (last_hilite != nullptr)
            right_hilite = last_hilite;

        if (left_it == left.end() && right_it == right.end())
            return true;

        if (left_it == left.end() || right_it == right.end())
            return false;

        // Lookup one character.
        // Check characters match.
        // Redundant check, given the hilite-ignorant comparison at the beginning, but let's keep it just in case.
        if (*left_it != *right_it)
            return false;


        // Check hilites match if it's not a whitespace.
        if (!std::isspace(*left_it) && left_hilite != right_hilite)
            return false;

        // Consume one character.
        left_it++;
        right_it++;
    }
}

int main(int argc, char* argv[])
{
    assert(argc == 3);
    return !are_equal_with_hilites(argv[1], argv[2]);
}
