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
};

}
