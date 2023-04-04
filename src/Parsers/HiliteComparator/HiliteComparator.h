#pragma once

#include <Parsers/IAST.h>


namespace HiliteComparator {

using Hilite = const char *;

const std::vector<Hilite> HILITES =
    {
        DB::IAST::hilite_keyword,
        DB::IAST::hilite_identifier,
        DB::IAST::hilite_function,
        DB::IAST::hilite_operator,
        DB::IAST::hilite_alias,
        DB::IAST::hilite_substitution,
        DB::IAST::hilite_none
};

[[maybe_unused]] Hilite consume_hilites(const char ** ptr_ptr);

String remove_hilites(std::string_view string);

bool are_equal_with_hilites_removed(std::string_view left, std::string_view right);

/*
 * Hilited queries cannot be compared symbol-by-symbol, as there's some frivolousness introduced with the hilites. Specifically:
 * 1. Whitespaces could be hilited with any hilite type.
 * 2. Hilite could or could be not reset with hilite_none before the next hilite, i.e. the following strings a and b are equal:
 *      a. hilite_keyword foo hilite_none hilite_operator +
 *      b. hilite_keyword foo hilite_operator +
 */
bool are_equal_with_hilites(std::string_view left, std::string_view right);

}
