#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Storages/IndicesDescription.h>

namespace DB
{

inline constexpr auto TABLE_SET_INDEX_TYPE = "table_set";
inline constexpr auto TABLE_JOIN_INDEX_TYPE = "table_join";

bool isLookupIndexType(std::string_view type);

void validateLookupIndex(const IndexDescription & index);
IndicesDescription getLookupIndicesFromAST(const ASTExpressionList * lookup_indices_ast, const ColumnsDescription & columns, ContextPtr context);
IndicesDescription parseLookupIndices(const String & str, const ColumnsDescription & columns, ContextPtr context);

}
