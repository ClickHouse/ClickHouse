#include <Functions/FunctionBaseAI.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <thread>
#include <Common/logger_useful.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
namespace ProfileEvents
{
    extern const Event AIInputTokens;
    extern const Event AIOutputTokens;
    extern const Event AIAPICalls;
    extern const Event AIRowsProcessed;
    extern const Event AIRowsSkipped;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ai_functions;
    extern const SettingsUInt64 ai_function_request_timeout_sec;
    extern const SettingsUInt64 ai_function_max_retries;
    extern const SettingsUInt64 ai_function_retry_initial_delay_ms;
    extern const SettingsBool ai_function_throw_on_error;
    extern const SettingsUInt64 ai_function_max_input_tokens_per_query;
    extern const SettingsUInt64 ai_function_max_output_tokens_per_query;
    extern const SettingsUInt64 ai_function_max_api_calls_per_query;
    extern const SettingsBool ai_function_throw_on_quota_exceeded;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int SUPPORT_IS_DISABLED;
}

namespace {

/// Strip control characters (U+0000..U+001F except \t \n \r) that break JSON serialization.
static String sanitizeTextForAI(const String & input)
{
    String output;
    output.reserve(input.size());
    for (unsigned char ch : input)
    {
        if (ch < 0x20 && ch != '\t' && ch != '\n' && ch != '\r')
            output.push_back(' ');
        else
            output.push_back(static_cast<char>(ch));
    }
    return output;
}

}

FunctionBaseAI::FunctionBaseAI(ContextPtr context_) : context_weak(context_)
{
    if (!getContext()->getSettingsRef()[Setting::allow_experimental_ai_functions])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "AI functions are experimental. Set `allow_experimental_ai_functions` setting to enable it");
}

FunctionBaseAI::ResolvedConfig FunctionBaseAI::resolveConfig(const ColumnsWithTypeAndName & arguments) const
{
    ResolvedConfig config;

    const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
    if (!col_const)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument to AI function must be a named collection (constant String)");

    String collection_name = col_const->getValue<String>();
    const auto & nc = NamedCollectionFactory::instance().get(collection_name);

    config.provider = nc->getOrDefault<String>("provider", DEFAULT_AI_PROVIDER);
    config.endpoint = nc->getOrDefault<String>("endpoint", "");
    config.model = nc->getOrDefault<String>("model", "");
    config.api_key = nc->getOrDefault<String>("api_key", "");
    config.api_version = nc->getOrDefault<String>("api_version", "");
    config.max_tokens = nc->getOrDefault<UInt64>("max_tokens", DEFAULT_AI_MAX_TOKENS);
    config.temperature = defaultTemperature();

    if (config.endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'endpoint'", collection_name);
    if (config.model.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'model'", collection_name);
    if (config.api_key.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "AI named collection '{}' must have 'api_key'", collection_name);

    return config;
}

float FunctionBaseAI::resolveTemperature(const ColumnsWithTypeAndName & arguments, const ResolvedConfig & config) const
{
    size_t temp_idx = FIRST_DATA_ARG_INDEX + 2;
    if (temp_idx < arguments.size() && isNumber(arguments[temp_idx].type))
    {
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[temp_idx].column.get());
        if (col_const)
            return static_cast<float>(col_const->getFloat64(0));
    }

    return config.temperature;
}

ColumnPtr FunctionBaseAI::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    auto config = resolveConfig(arguments);
    auto provider = createAIProvider(config.provider, config.endpoint, config.api_key, config.api_version);
    float temperature = resolveTemperature(arguments, config);

    const auto & settings = getContext()->getSettingsRef();
    UInt64 timeout_sec = settings[Setting::ai_function_request_timeout_sec].value;
    UInt64 max_retries = settings[Setting::ai_function_max_retries].value;
    UInt64 retry_delay_ms = settings[Setting::ai_function_retry_initial_delay_ms].value;

    AIQuotaTracker quota(
        settings[Setting::ai_function_max_input_tokens_per_query].value,
        settings[Setting::ai_function_max_output_tokens_per_query].value,
        settings[Setting::ai_function_max_api_calls_per_query].value,
        settings[Setting::ai_function_throw_on_quota_exceeded].value,
        settings[Setting::ai_function_throw_on_error].value);

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(settings, getContext()->getServerSettings());
    timeouts.receive_timeout = Poco::Timespan(static_cast<int64_t>(timeout_sec) /*s*/, 0 /*us*/);

    String system_prompt = sanitizeTextForAI(buildSystemPrompt(arguments));
    auto response_format = buildResponseFormat(arguments);

    auto result_col = ColumnString::create();
    const auto & text_col = arguments[FIRST_DATA_ARG_INDEX].column;

    UInt64 total_api_calls = 0;
    UInt64 total_input_tokens = 0;
    UInt64 total_output_tokens = 0;
    UInt64 rows_processed = 0;
    UInt64 rows_skipped = 0;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        if (text_col->isNullAt(i) || quota.isQuotaExceeded())
        {
            result_col->insertDefault();
            ++rows_skipped;
            continue;
        }

        String user_message = sanitizeTextForAI(buildUserMessage(arguments, i));
        String result;
        bool success = false;

        for (UInt64 attempt = 0; attempt <= max_retries; ++attempt)
        {
            try
            {
                AIRequest ai_request;
                ai_request.system_prompt = system_prompt;
                ai_request.user_message = user_message;
                ai_request.response_format = response_format;
                ai_request.model = config.model;
                ai_request.temperature = temperature;
                ai_request.max_tokens = config.max_tokens;

                auto ai_response = provider->call(ai_request, timeouts);

                quota.recordResponse(ai_response.input_tokens, ai_response.output_tokens);
                total_input_tokens += ai_response.input_tokens;
                total_output_tokens += ai_response.output_tokens;
                ++total_api_calls;

                result = postProcessResponse(ai_response.result);
                success = true;
                break;
            }
            catch (const Exception & e)
            {
                if (attempt < max_retries && e.code() == ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms * (1ULL << attempt)));
                    continue;
                }

                if (!quota.throwsOnError())
                    break;

                throw;
            }
        }

        result_col->insertData(result.data(), result.size());
        if (success)
            ++rows_processed;
        else
            ++rows_skipped;
    }

    ProfileEvents::increment(ProfileEvents::AIAPICalls, total_api_calls);
    ProfileEvents::increment(ProfileEvents::AIInputTokens, total_input_tokens);
    ProfileEvents::increment(ProfileEvents::AIOutputTokens, total_output_tokens);
    ProfileEvents::increment(ProfileEvents::AIRowsProcessed, rows_processed);
    ProfileEvents::increment(ProfileEvents::AIRowsSkipped, rows_skipped);

    return result_col;
}

}
