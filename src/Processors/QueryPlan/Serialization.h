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
};

struct SerializedSetsRegistry;

/// The output headers of the children of a step being deserialized. The number of children comes from
/// the serialized plan, so it may not be what the step expects: accessing a missing child throws
/// instead of reading out of bounds. Converts to `const SharedHeaders &` to pass all of them on.
class DeserializedInputHeaders
{
public:
    DeserializedInputHeaders(const SharedHeaders & headers_) : headers(headers_) {} /// NOLINT(google-explicit-constructor)
    /// Only a reference is stored, so a temporary vector would dangle right after the initialization.
    DeserializedInputHeaders(SharedHeaders && headers_) = delete;
    operator const SharedHeaders &() const { return headers; } /// NOLINT(google-explicit-constructor)

    [[nodiscard]] size_t size() const { return headers.size(); }
    [[nodiscard]] bool empty() const { return headers.empty(); }
    [[nodiscard]] auto begin() const { return headers.begin(); }
    [[nodiscard]] auto end() const { return headers.end(); }

    [[nodiscard]] const SharedHeader & at(size_t i) const;
    [[nodiscard]] const SharedHeader & operator[](size_t i) const { return at(i); }
    [[nodiscard]] const SharedHeader & front() const { return at(0); }
    [[nodiscard]] const SharedHeader & back() const { return at(headers.empty() ? 0 : headers.size() - 1); }

private:
    const SharedHeaders & headers;
};

/// Deserialization context passed to `IQueryPlanStep::deserialize`.
struct IQueryPlanStep::Deserialization
{
    ReadBuffer & in;
    DeserializedSetsRegistry & registry;
    std::vector<StoragePtr> storage_holders;    /// Storages that are referenced by the step and need to be kept alive

    const ContextPtr & context;

    DeserializedInputHeaders input_headers;
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
