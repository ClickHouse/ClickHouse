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

    // A durty hack used by the automatic parallel replicas implementation:
    // the `final` value differs for `AggregatingStep` in single-node and distributed query plans.
    // This breaks matching by hash.
    bool skip_final_flag = false;
    // The same situation as above.
    bool skip_cache_key = false;

    /// Query-plan serialization version the stream is being written with (DBMS_QUERY_PLAN_SERIALIZATION_VERSION).
    UInt64 version = 0;

    /// v5 outline streams: the payload format version this step is writing. Preset by the
    /// framework to the step's registered maximum; a step that emits an older payload form
    /// (e.g. toward an older stream version) must lower it, otherwise the outline would
    /// advertise a format the bytes do not have and newer readers would misparse them.
    UInt64 step_format_version = 1;

    /// The oldest plan version able to read what was actually written so far ("needed to read").
    /// A step raises it on the exact line that writes value-dependent content an old reader must
    /// understand to execute correctly (e.g. a semantic flag) - without this the content would be
    /// skipped as ignorable and an old reader would silently produce different results.
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

    /// v5 outline streams: the payload format version this step was written with (1 otherwise).
    /// A reader may see a version above the one it knows; the tail fields are then ignorable by
    /// the append-only rule and are skipped via the payload frame.
    UInt64 step_format_version = 1;
};

/// Header encoding shared by the plan stream and the v5 outline section: column names and
/// encoded types only, constants are refilled by steps.
void serializeQueryPlanHeader(const Block & header, WriteBuffer & out);
Block deserializeQueryPlanHeader(ReadBuffer & in, size_t max_type_complexity);

}
