#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

#include <memory>
#include <string_view>


namespace DB
{

/// A provider response for one input: `text` for chat functions, `embedding` for embedding functions.
struct AIFunctionOutput
{
    String text;
    VectorWithMemoryTracking<Float32> embedding;
};

struct AIFunctionOutputWeight
{
    size_t operator()(const AIFunctionOutput & output) const
    {
        return sizeof(AIFunctionOutput) + output.text.size() + output.embedding.size() * sizeof(Float32);
    }
};

/// Responses of one AI function in a query, keyed by the hash of the input.
/// All other arguments of an AI function are constant, so the input alone identifies the request.
class AIFunctionOutputCache : public CacheBase<UInt128, AIFunctionOutput, UInt128TrivialHash, AIFunctionOutputWeight>
{
public:
    using Base = CacheBase<UInt128, AIFunctionOutput, UInt128TrivialHash, AIFunctionOutputWeight>;

    explicit AIFunctionOutputCache(size_t max_size_in_bytes);

    static UInt128 hash(std::string_view input);
};

using AIFunctionOutputCachePtr = std::shared_ptr<AIFunctionOutputCache>;

/// Null when `ai_function_enable_repeated_output_cache` is disabled or `ai_function_repeated_output_cache_size` is 0.
AIFunctionOutputCachePtr createAIFunctionOutputCache(const ContextPtr & context);

}
