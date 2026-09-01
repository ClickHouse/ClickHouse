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

    /// The payload format version this step is writing, in a framed stream. Preset to the newest
    /// format the step registered; a step that writes an older form of its payload, for an older
    /// stream version, must lower it. Otherwise the outline would name a format the bytes are not
    /// in and newer readers would misread them.
    UInt64 step_format_version = 1;

    /// The oldest plan version that can read what has been written so far. A step raises it on the
    /// very line that writes something an old reader would have to act on to run the plan
    /// correctly, a flag that changes results for example. Without that the old reader would skip
    /// the bytes as something it may ignore and quietly return different results.
    UInt64 min_reader_version = 0;

    void requireReaderVersion(UInt64 version_) { min_reader_version = std::max(min_reader_version, version_); }
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

    /// The payload format version this step was written with, in a framed stream, 1 otherwise.
    /// This may be higher than any format the reader knows: the fields at the end are then ones it
    /// may ignore, and the payload's own size tells it where they stop.
    UInt64 step_format_version = 1;
};

/// How a header goes on the wire, the same in the older stream and in the outline: column names and
/// encoded types only. Steps refill the constants.
void serializeQueryPlanHeader(const Block & header, WriteBuffer & out);
Block deserializeQueryPlanHeader(ReadBuffer & in, size_t max_type_complexity);

}
