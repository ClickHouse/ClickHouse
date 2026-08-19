#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    bool build_logical_plan = false,
    const std::string & default_database = {},
    /// If set (build_logical_plan=true path only), an already-resolved query tree from the
    /// outer/initiator-level analysis is reused instead of re-resolving `query_ast` from
    /// scratch. Fixes #111893: re-resolving needs the shard's table to exist in the
    /// initiator's own DatabaseCatalog (e.g. remote()'s target database, or a per-shard
    /// default_database), which it legitimately may not.
    const QueryTreeNodePtr & query_tree = nullptr);
}
