#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/New/AST/fwd_decl.h>
#include <common/types.h>

namespace DB
{

// Compatibility interface
AST::PtrTo<AST::Query> parseQuery(const std::string & query, const String & current_database);
ASTPtr parseQuery(const char * begin, const char * end, size_t max_query_size, size_t max_parser_depth, const String & current_database);

}
