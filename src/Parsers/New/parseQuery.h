#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

// Compatibility interface

ASTPtr parseQuery(const std::string& query);

}
