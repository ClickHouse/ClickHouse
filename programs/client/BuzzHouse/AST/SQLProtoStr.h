#pragma once

#include <string>
#include <vector>

#include "SQLGrammar.pb.h"

namespace BuzzHouse
{

void SQLQueryToString(std::string & ret, const SQLQuery &);

}
