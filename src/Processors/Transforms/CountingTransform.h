#pragma once

#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Access/EnabledQuota.h>


namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ThreadStatus;

/// Proxy class which counts number of written block, rows, bytes
class CountingTransform final : public ExceptionKeepingTransform
{
public:
    enum class InsertSource : uint8_t
    {
        Direct,
        MaterializedView,
        /// Rows that are neither a direct INSERT nor a push from a materialized view into its target table,
        /// e.g. rows entering a window view. Counted only in the generic InsertedRows/InsertedBytes events.
        Other,
    };

    explicit CountingTransform(
        SharedHeader header,
        InsertSource source_,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr,
        UInt64 normalized_query_hash_ = 0)
        : ExceptionKeepingTransform(header, header)
        , source(source_)
        , quota(std::move(quota_))
        , normalized_query_hash(normalized_query_hash_) {}

    String getName() const override { return "CountingTransform"; }

    void setProgressCallback(const ProgressCallback & callback)
    {
        progress_callback = callback;
    }

    void setProcessListElement(QueryStatusPtr elem)
    {
        process_elem = elem;
    }

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

protected:
    ProgressCallback progress_callback;
    QueryStatusPtr process_elem;
    InsertSource source;

    /// Quota is used to limit amount of written bytes.
    std::shared_ptr<const EnabledQuota> quota;
    UInt64 normalized_query_hash = 0;
    Chunk cur_chunk;
};

}
