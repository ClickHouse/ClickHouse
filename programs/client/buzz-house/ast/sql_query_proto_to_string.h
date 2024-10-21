#pragma once

#include <string>
#include <vector>

#include "buzz-house/ast/sql_grammar.pb.h"

namespace buzzhouse {

void SQLQueryToString(std::string &ret, const sql_query_grammar::SQLQuery&);

}  // namespace buzzhouse
