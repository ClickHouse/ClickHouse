#pragma once

#include <Core/Types.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class WriteBuffer;
struct FragmentGraph;
struct FragmentExchange;

/// Pieces of the text output of a `FragmentGraph` shared by `EXPLAIN PLAN fragments = 1` and `EXPLAIN PLAN tree = 1`.

/// `hash(keys)`, `broadcast`, `gather`, `gather sorted by ...`, `any` or `set values`. With `pretty`, the keys and the
/// sort columns are printed without their table prefix, as in the plan.
String formatExchangeKind(const FragmentExchange & exchange, bool pretty);

/// `streaming` or `persisted`.
std::string_view formatExchangeMode(ExchangeDescription::Kind mode);

/// Prints a graph of one fragment without exchanges (a plan that was not cut) as `EXPLAIN` without options prints its
/// plan, followed by `Not distributed: <reason>` when the fragment has a fallback reason. Returns false and prints
/// nothing for any other graph.
bool tryExplainSingleFragment(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length);

/// The `Exchanges:` section: one line per exchange with its kind, mode, producer and consumer, and buckets.
void writeExchangesSection(const FragmentGraph & graph, WriteBuffer & buf, bool pretty);

/// The `Levels:` line: the fragment ids grouped by topological level, leaves first (see `computeLevels`).
void writeLevelsSection(const FragmentGraph & graph, WriteBuffer & buf);

}
