#pragma once

#include <string>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class Context;

/// Parses a common argument for table functions such as table structure given in string
ColumnsDescription parseColumnsListFromString(const std::string & structure, const Context & context);

}
