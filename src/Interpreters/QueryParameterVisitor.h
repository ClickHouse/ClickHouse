#pragma once

#include <string>
#include <Core/Names.h>


namespace DB
{

/// Find parameters in a query and collect them into set.
NameSet analyzeReceiveQueryParams(const std::string & query);

}
