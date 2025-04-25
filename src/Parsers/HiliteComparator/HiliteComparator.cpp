#include "HiliteComparator.h"

namespace HiliteComparator
{

void consume_hilites(std::string_view::iterator * ptr, Hilite * last_hilite)
{
    while (true)
    {
        bool changed_hilite = false;
        for (Hilite hilite : hilites)
        {
            if (std::string_view(&**ptr).starts_with(hilite))
            {
                *(ptr) += strlen(hilite);
                changed_hilite = true;
                if (last_hilite != nullptr)
                    *last_hilite = hilite;
            }
        }
        if (!changed_hilite)
            break;
    }
}

bool are_equal_with_hilites_removed(std::string_view left, std::string_view right)
{
    return remove_hilites(left) == remove_hilites(right);
}

String remove_hilites(std::string_view string)
{
    auto ptr = string.begin();
    String string_without_hilites;
    while (true)
    {
        consume_hilites(&ptr);
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
bool are_equal_with_hilites(std::string_view left, std::string_view right, bool check_end_without_hilite)
{
    auto left_it = left.begin();
    auto right_it = right.begin();
    Hilite left_hilite = DB::IAST::hilite_none;
    Hilite right_hilite = DB::IAST::hilite_none;

    while (true)
    {
        // For each argument, consume all prefix hilites, and update the current hilite to be the last one.
        consume_hilites(&left_it, &left_hilite);
        consume_hilites(&right_it, &right_hilite);

        if (left_it == left.end() && right_it == right.end())
        {
            if (left_hilite != right_hilite)
                return false;
            if (check_end_without_hilite)
                if (left_hilite != DB::IAST::hilite_none)
                    throw std::logic_error("Expected string ends with a hilite");
            return true;
        }

        if (left_it == left.end() || right_it == right.end())
            return false;

        // Lookup one character.
        // Check characters match.
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

bool are_equal_with_hilites_and_end_without_hilite(std::string_view left, std::string_view right)
{
    return are_equal_with_hilites(left, right, true);
}


}
