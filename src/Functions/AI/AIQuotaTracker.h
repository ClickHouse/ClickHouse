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

    /// Record one API call and its token counts. Checks all quotas after updating.
    /// If a limit is exceeded: throws if throw_on_quota_exceeded, otherwise sets quota_exceeded flag.
    void recordResponse(UInt64 in_tokens, UInt64 out_tokens);

    bool isQuotaExceeded() const { return quota_exceeded; }
    bool throwsOnError() const { return throw_on_error; }

private:
    void checkQuotas();

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
