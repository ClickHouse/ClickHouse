#pragma once

#include <string>
#include <vector>

#include "sql_grammar.pb.h"

namespace chfuzz {

void SQLQueryToString(std::string &ret, const sql_query_grammar::SQLQuery&);

}  // namespace chfuzz
