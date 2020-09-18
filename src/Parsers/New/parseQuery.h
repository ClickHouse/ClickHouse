#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB
{

// Compatibility interface

ASTPtr parseQuery(const char * begin, const char * end, size_t max_query_size, size_t max_parser_depth);

}
