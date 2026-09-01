#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPlanResourceHolder.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct SerializedSetsRegistry;
struct DeserializedSetsRegistry;

/// Serialization context passed to `IQueryPlanStep::serialize`.
/// Settings are handled separately via `serializeSettings` method.
struct IQueryPlanStep::Serialization
{
    WriteBuffer & out;
    SerializedSetsRegistry & registry;

    // Set when a step is serialized to compute its Auto-PR plan cache-key hash (not for transmission).
    // In that mode the serialization omits fields that would otherwise break hash matching between the
    // single-node and distributed (parallel-replicas) plan builds: `AggregatingStep`'s `final` flag
    // (which differs between those builds) and its stats-collecting cache key, and the runtime-filter
    // id value in `ActionsDAG::serialize`.
    // MUST be kept in sync with `for_cache_key` on `SerializedSetsRegistry` (the registry one drives
    // `ActionsDAG::serialize`, this one drives the step's own `serialize`): set both or neither.
    bool for_cache_key = false;

    /// Query-plan serialization version the stream is being written with (DBMS_QUERY_PLAN_SERIALIZATION_VERSION).
    UInt64 version = 0;

    /// Write a column name carried by a step's payload (a GROUP BY key, an aggregate argument, a sort
    /// column, ...). In cache-key mode the index of the analyzer-generated table qualifier is erased
    /// first, because it is branch-local: the same column is `__table3.o_custkey` in one plan build and
    /// `__table4.o_custkey` in the other. Everything else about the name is kept, so two different
    /// columns still hash apart. See `normalizeGeneratedTableQualifiers` and `ActionsDAG::Node::updateHash`.
    void writeColumnName(const String & name) const;
};

struct SerializedSetsRegistry;

/// Deserialization context passed to `IQueryPlanStep::deserialize`.
struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;
    std::vector<StoragePtr> storage_holders;    /// Storages that are referenced by the step and need to be kept alive

    const ContextPtr & context;

    const SharedHeaders & input_headers;
    const SharedHeader & output_header;
    const QueryPlanSerializationSettings & settings;

    /// Binary type-decoding complexity limit resolved once at the deserialization entry point (0 == unlimited).
    /// A client QueryPlan packet (TCPHandler::receiveQueryPlan) passes the effective
    /// input_format_binary_max_type_complexity; trusted server-to-server plans pass 0.
    size_t max_type_complexity = 0;

    /// Query-plan serialization version the stream was written with (DBMS_QUERY_PLAN_SERIALIZATION_VERSION).
    UInt64 version = 0;
    /// The plan is being drained (e.g. TCPHandler::skipData) and will be discarded, not executed.
    /// Steps that are expensive or need execution-only context (index analysis, parallel-replicas
    /// callbacks) may read their bytes but build a lightweight placeholder instead of a real step.
    bool skipping = false;
};

}
