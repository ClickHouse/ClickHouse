#pragma once

#include <string>


namespace DB
{

/** Convert a string, so result could be used as a file name.
  * In fact it percent-encode all non-word characters, as in URL.
  */

std::string escapeForFileName(const std::string & s);
std::string unescapeForFileName(const std::string & s);

}
