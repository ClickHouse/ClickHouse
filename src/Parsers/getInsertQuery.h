#pragma once
#include <Core/ColumnsWithTypeAndName.h>
#include <Parsers/IdentifierQuotingStyle.h>

namespace DB
{
std::string getInsertQuery(const std::string & db_name, const std::string & table_name, const ColumnsWithTypeAndName & columns, IdentifierQuotingStyle quoting);
}
