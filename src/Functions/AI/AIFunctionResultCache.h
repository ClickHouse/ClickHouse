#pragma once

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>

#include <memory>


namespace CurrentMetrics
{
    extern const Metric AIFunctionResultCacheBytes;
    extern const Metric AIFunctionResultCacheCells;
}

namespace DB
{

/// A provider response to one request: `text` for chat functions, `embedding` for embedding functions.
struct AIFunctionResult
{
    String text;
    VectorWithMemoryTracking<Float32> embedding;
};

struct AIFunctionResultWeight
{
    size_t operator()(const AIFunctionResult & result) const
    {
        return sizeof(AIFunctionResult) + result.text.size() + result.embedding.size() * sizeof(Float32);
    }
};

/// Provider responses kept for all queries of the server, keyed by the hash of the request and of the user who sent it:
/// the function, its named collection, its parameters and the input. Only successful responses are stored.
class AIFunctionResultCache : public CacheBase<UInt128, AIFunctionResult, UInt128TrivialHash, AIFunctionResultWeight>
{
public:
    using Base = CacheBase<UInt128, AIFunctionResult, UInt128TrivialHash, AIFunctionResultWeight>;

    explicit AIFunctionResultCache(size_t max_size_in_bytes)
        : Base(CurrentMetrics::AIFunctionResultCacheBytes, CurrentMetrics::AIFunctionResultCacheCells, max_size_in_bytes)
    {
    }
};

using AIFunctionResultCachePtr = std::shared_ptr<AIFunctionResultCache>;

}
