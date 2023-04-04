#include "HiliteComparator.h"

namespace HiliteComparator
{

void consume_hilites(const char * & ptr, Hilite * last_hilite)
{
    bool changed_hilite = false;
    do
    {
        for (Hilite hilite : HILITES)
        {
            if (std::string_view(ptr).starts_with(hilite))
            {
                ptr += strlen(hilite);
                changed_hilite = true;
                if (last_hilite != nullptr)
                    *last_hilite = hilite;
            }
        }
    } while (changed_hilite);
}

bool are_equal_with_hilites_removed(std::string_view left, std::string_view right)
{
    return remove_hilites(left) == remove_hilites(right);
}

String remove_hilites(std::string_view string)
{
    const char * ptr = string.begin();
    String string_without_hilites;
    while (true)
    {
        consume_hilites(ptr);
        if (ptr == string.end())
            return string_without_hilites;
        string_without_hilites += *(ptr++);
    }
}

/*
 * Hilited queries cannot be compared symbol-by-symbol, as there's some frivolousness introduced with the hilites. Specifically:
 * 1. Whitespaces could be hilited with any hilite type.
 * 2. Hilite could or could be not reset with hilite_none before the next hilite, i.e. the following strings a and b are equal:
 *      a. hilite_keyword foo hilite_none hilite_operator +
 *      b. hilite_keyword foo hilite_operator +
 */
bool are_equal_with_hilites(std::string_view left, std::string_view right)
{
    if (!are_equal_with_hilites_removed(left, right))
        return false;

    const char * left_it = left.begin();
    const char * right_it = right.begin();
    Hilite left_hilite = DB::IAST::hilite_none;
    Hilite right_hilite = DB::IAST::hilite_none;

    while (true)
    {
        // Consume all prefix hilites, update the current hilite to be the last one.
        consume_hilites(left_it, &left_hilite);
        consume_hilites(right_it, &right_hilite);

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


}
