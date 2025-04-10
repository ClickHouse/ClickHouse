#pragma once

#include <base/types.h>

namespace DB
{

/** Replaces double quotes (") by single quotes (') for all string literals
  * SELECT _latin1"abc"; -- might be also valid for MySQL
  * https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
  *
  * Also resolves charset introducers if any by converting to utf8.
  * https://dev.mysql.com/doc/refman/8.0/en/charset-introducer.html
  *
  * Some charsets might be not supported f.e. _binary''.
  * Returns true if the query was rewritten.
  */
bool tryConvertStringLiterals(String & query);

}
