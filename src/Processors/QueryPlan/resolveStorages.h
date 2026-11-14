#pragma once

#include <Interpreters/Context_fwd.h>
namespace DB
{

class Identifier;
class TableNode;
class QueryPlan;

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context);
Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context);

/// Whether `QueryPlan::resolveStorages` would resolve a read of a table that cannot be read from a serialized
/// plan, see `IStorage::readRequiresFullQuery`. Tables that cannot be resolved in `context` are skipped.
bool queryPlanReadsStorageRequiringFullQuery(const QueryPlan & plan, const ContextPtr & context);

}
