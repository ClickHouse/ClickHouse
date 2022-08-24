#pragma once

#include <Core/Block.h>

#include <Parsers/IAST.h>

#include <Processors/QueryPlan/QueryPlan.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/// Dump query plan
String dumpQueryPlan(QueryPlan & query_plan);

/// Dump query plan result pipeline
String dumpQueryPipeline(QueryPlan & query_plan);

/// Build common header for UNION query
Block buildCommonHeaderForUnion(const Blocks & queries_headers);

/// Convert query node to ASTSelectQuery
ASTPtr queryNodeToSelectQuery(const QueryTreeNodePtr & query_node);

}
