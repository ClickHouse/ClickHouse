#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>
#include <atomic>

namespace DB
{


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

    bool checkBeforeDispatch(UInt64 estimated_input_tokens);
    void recordResponse(UInt64 in_tokens, UInt64 out_tokens);
    bool handleRowError();
    bool isQuotaExceeded() const { return quota_exceeded.load(std::memory_order_relaxed); }
    String onError() const { return on_error; }

    std::atomic<UInt64> input_tokens{0};
    std::atomic<UInt64> output_tokens{0};
    std::atomic<UInt64> api_calls{0};
    std::atomic<UInt64> rows_processed{0};
    std::atomic<UInt64> rows_skipped{0};

private:
    UInt64 max_input_tokens;
    UInt64 max_output_tokens;
    UInt64 max_api_calls;
    String on_quota_exceeded;
    String on_error;
    std::atomic<bool> quota_exceeded{false};
};

using AIQuotaTrackerPtr = std::shared_ptr<AIQuotaTracker>;

}
