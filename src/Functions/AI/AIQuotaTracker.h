#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

namespace DB
{

class AIQuotaTracker
{
public:
    AIQuotaTracker(
        UInt64 max_input_tokens_, UInt64 max_output_tokens_,
        UInt64 max_api_calls_, bool throw_on_quota_exceeded_, bool throw_on_error_)
        : max_input_tokens(max_input_tokens_)
        , max_output_tokens(max_output_tokens_)
        , max_api_calls(max_api_calls_)
        , throw_on_quota_exceeded(throw_on_quota_exceeded_)
        , throw_on_error(throw_on_error_)
    {}

    /// Check if we have already exceeded a quota, or if we are about to exceed the API calls, estimated input tokens quota.
    /// Also checks if we exceeded the max_output_tokens quota on the previous call
    /// estimated_text_bytes is the total size of the prompt text in bytes
    bool checkBeforeDispatch(size_t estimated_text_bytes = 0);

    /// Record counts of input and output tokens. To be called after the API call.
    void recordResponse(UInt64 in_tokens, UInt64 out_tokens);

    bool handleRowError();
    bool isQuotaExceeded() const { return quota_exceeded; }
    bool throwsOnError() const { return throw_on_error; }

private:
    const UInt64 max_input_tokens;
    const UInt64 max_output_tokens;
    const UInt64 max_api_calls;
    const bool throw_on_quota_exceeded;
    const bool throw_on_error;
    bool quota_exceeded = false;

    UInt64 input_tokens = 0;
    UInt64 output_tokens = 0;
    UInt64 api_calls = 0;
    UInt64 rows_skipped = 0;
};

}
