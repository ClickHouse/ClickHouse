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
    explicit CountingTransform(
        SharedHeader header,
        std::shared_ptr<const EnabledQuota> quota_ = nullptr,
        UInt64 normalized_query_hash_ = 0)
        : ExceptionKeepingTransform(header, header)
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

    /// Disable the global InsertedRows / InsertedBytes profile-event increments for this transform.
    /// Used for the nested insert of a transparent forwarder (a distributed INSERT's local shard, an
    /// INSERT through an Alias table), whose rows an outer pipeline already accounted for the query.
    void disableProfileEventsCounting()
    {
        count_profile_events = false;
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

    /// Quota is used to limit amount of written bytes.
    std::shared_ptr<const EnabledQuota> quota;
    UInt64 normalized_query_hash = 0;
    bool count_profile_events = true;
    Chunk cur_chunk;
};

}
