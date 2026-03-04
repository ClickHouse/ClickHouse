#pragma once

namespace DB
{
class QueryPlan;
class Block;

void addConvertingActions(QueryPlan & plan, const Block & header, const ContextPtr & context);
}
