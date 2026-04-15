#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>
#include <atomic>

namespace DB
{


static constexpr auto MEMORY_ORDER = std::memory_order_relaxed;

class AIQuotaTracker
{
public:
    AIQuotaTracker(
        UInt64 max_input_tokens_, UInt64 max_output_tokens_,
        UInt64 max_api_calls_, const String & on_quota_exceeded_, const String & on_error_)
        : max_input_tokens(max_input_tokens_)
        , max_output_tokens(max_output_tokens_)
        , max_api_calls(max_api_calls_)
        , on_quota_exceeded(on_quota_exceeded_)
        , on_error(on_error_)
    {}

    /// Check if we have already exceeded a quota, or if we are about to exceed the API calls, estimated input tokens quota.
    /// Also checks if we exceeded the max_output_tokens quota on the previous call
    /// estimated_text_bytes is the total size of the prompt text in bytes
    bool checkBeforeDispatch(size_t estimated_text_bytes = 0);

    /// Record counts of input and output tokens. To be called after the API call.
    void recordResponse(UInt64 in_tokens, UInt64 out_tokens);

    bool handleRowError();
    bool isQuotaExceeded() const { return quota_exceeded.load(MEMORY_ORDER); }
    String onError() const { return on_error; }

    std::atomic<UInt64> input_tokens{0};
    std::atomic<UInt64> output_tokens{0};
    std::atomic<UInt64> api_calls{0};
    std::atomic<UInt64> rows_processed{0};
    std::atomic<UInt64> rows_skipped{0};

private:
    const UInt64 max_input_tokens;
    const UInt64 max_output_tokens;
    const UInt64 max_api_calls;
    const String on_quota_exceeded;
    const String on_error;
    std::atomic<bool> quota_exceeded{false};
};

using AIQuotaTrackerPtr = std::shared_ptr<AIQuotaTracker>;

}
