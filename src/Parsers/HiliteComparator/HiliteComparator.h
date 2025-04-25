#pragma once

#include <Parsers/IAST.h>
#include <string_view>


namespace HiliteComparator
{

using Hilite = const char *;

static const std::vector<Hilite> hilites = {
        DB::IAST::hilite_keyword,
        DB::IAST::hilite_identifier,
        DB::IAST::hilite_function,
        DB::IAST::hilite_operator,
        DB::IAST::hilite_alias,
        DB::IAST::hilite_substitution,
        DB::IAST::hilite_none
    };

/*
 * Consume all prefix hilites, by moving `ptr` to
 * If `last_hilite` is not `nullptr`, update the last hilite to be the last hilite of the prefix hilites.
 */
void consume_hilites(std::string_view::iterator *, Hilite * last_hilite = nullptr);

String remove_hilites(std::string_view string);

/*
 * Copies both strings, for the simplicity of the implementation.
 */
bool are_equal_with_hilites_removed(std::string_view left, std::string_view right);

/*
 * Hilited queries cannot be compared symbol-by-symbol, as there's some frivolousness introduced with the hilites. Specifically:
 * 1. Whitespaces could be hilited with any hilite type.
 * 2. Hilite could or could be not reset with hilite_none before the next hilite, i.e. the following strings a and b are equal:
 *      a. hilite_keyword foo hilite_none hilite_operator +
 *      b. hilite_keyword foo hilite_operator +
 */
bool are_equal_with_hilites(std::string_view left, std::string_view right, bool check_end_without_hilite);

// Google tests's ASSERT_PRED_2 doesn't see overloads with default parameter values.
bool are_equal_with_hilites_and_end_without_hilite(std::string_view left, std::string_view right);

}
