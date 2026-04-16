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

    /// Check all quotes, return true if any quota is met or exceeded, false otherwise. Should be called before issuing API call.
    bool checkQuotas();

    /// Record one API call and its token counts. Should be called after an API call, does not check quotas.
    void recordResponse(UInt64 in_tokens, UInt64 out_tokens);

    bool throwOnError() const { return throw_on_error; }

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
};

}
