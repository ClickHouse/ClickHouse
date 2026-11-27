#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/LLMDeploymentRegistry.h>
#include <Interpreters/LLMQueryLog.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <charconv>
#include <chrono>
#include <cmath>

namespace ProfileEvents
{
extern const Event AIAPICalls;
extern const Event AIAPICallsFailed;
extern const Event AIInputTokens;
extern const Event AIOutputTokens;
extern const Event AIRequestMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
extern const int AI_PROVIDER_RESPONSE_INCOMPLETE;
extern const int LIMIT_EXCEEDED;
}

namespace
{

struct QueryOptions
{
    UInt64 max_tokens = 1024;
    double temperature = 0.7;
    String system_prompt;
};

template <typename T>
T parseParameter(const String & value, const String & name)
{
    T result{};
    auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid `llmQuery` parameter '{}'", name);
    return result;
}

QueryOptions getOptions(const ColumnsWithTypeAndName & arguments)
{
    QueryOptions options;
    if (arguments.size() == 3)
    {
        const auto & map_column = assert_cast<const ColumnConst &>(*arguments[2].column);
        Field map_field = map_column.getDataColumn()[0];
        const auto & map = map_field.safeGet<Map>();
        UnorderedSetWithMemoryTracking<String> keys;
        for (const auto & item : map)
        {
            const auto & tuple = item.safeGet<Tuple>();
            const auto & key = tuple[0].safeGet<String>();
            const auto & value = tuple[1].safeGet<String>();
            if (!keys.insert(key).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate `llmQuery` parameter '{}'", key);
            if (key == "max_tokens")
                options.max_tokens = parseParameter<UInt64>(value, key);
            else if (key == "temperature")
                options.temperature = parseParameter<double>(value, key);
            else if (key == "system_prompt")
                options.system_prompt = value;
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown `llmQuery` parameter '{}'", key);
        }
    }
    if (!options.max_tokens || options.max_tokens > 1000000)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`llmQuery` max_tokens must be between 1 and 1000000");
    if (!std::isfinite(options.temperature) || options.temperature < 0 || options.temperature > 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`llmQuery` temperature must be between 0 and 2");
    return options;
}

void setEventTime(LLMQueryLogElement & event)
{
    auto now = std::chrono::system_clock::now().time_since_epoch();
    event.event_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    event.event_time_microseconds = Decimal64(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

class FunctionLLMQuery final : public IFunction
{
public:
    static constexpr auto name = "llmQuery";

    explicit FunctionLLMQuery(ContextPtr context_)
        : context(std::move(context_))
    {
        FunctionBaseAI::throwIfNotAllowed(context);
        context->checkAccess(AccessType::llmQuery);
    }

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionLLMQuery>(std::move(context_));
    }
    String getName() const override
    {
        return name;
    }
    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }
    bool isStateful() const override
    {
        return true;
    }
    bool isDeterministic() const override
    {
        return false;
    }
    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
    bool isExpensive() const override
    {
        return true;
    }
    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }
    bool useDefaultImplementationForConstants() const override
    {
        return false;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory{
            {"route",
             [](const IDataType & type)
             {
                 return isString(type);
             },
             &isColumnConst,
             "const String"},
            {"prompt", &FunctionBaseAI::isStringOrNullableString, nullptr, "String or Nullable(String)"}};
        FunctionArgumentDescriptors optional{{"params", &FunctionBaseAI::isStringToStringMap, &isColumnConst, "const Map(String, String)"}};
        validateFunctionArguments(*this, arguments, mandatory, optional);
        return FunctionBaseAI::wrapReturnTypeForNullablePrompt(arguments, 1, std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t rows) const override
    {
        FunctionBaseAI::checkHasUser(context);
        auto options = getOptions(arguments);
        auto registry = context->getLLMDeploymentRegistry();
        auto request_log = context->getLLMQueryLog();
        if (!request_log)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "`llmQuery` requires the `llm_query_log` server configuration");

        auto result = removeNullable(result_type)->createColumn();
        auto null_map = result_type->isNullable() ? ColumnUInt8::create(rows, static_cast<UInt8>(0)) : nullptr;
        if (!rows)
        {
            if (null_map)
                return ColumnNullable::create(std::move(result), std::move(null_map));
            return result;
        }

        auto snapshot = registry->getSnapshot(context);
        const String route(arguments[0].column->getDataAt(0));
        /// Check the whole route before choosing a deployment, so routing cannot intermittently select inaccessible credentials.
        for (const auto & deployment : snapshot.deployments)
        {
            if (deployment.route == route && deployment.weight)
                context->checkAccess(AccessType::NAMED_COLLECTION, deployment.credentials);
        }
        auto prompt_column = arguments[1].column->convertToFullColumnIfConst();
        const auto * nullable = typeid_cast<const ColumnNullable *>(prompt_column.get());
        const auto & prompts = nullable ? nullable->getNestedColumn() : *prompt_column;
        auto quota = context->getAIQuotaTracker();
        const auto policy = FunctionBaseAI::makeRequestPolicy(context);

        for (size_t row = 0; row < rows; ++row)
        {
            if (nullable && nullable->getNullMapData()[row])
            {
                result->insertDefault();
                null_map->getData()[row] = 1;
                continue;
            }
            quota->throwIfCancelled();
            auto reservation = registry->reserve(snapshot, route);
            const auto & deployment = reservation->deployment;
            auto credentials = FunctionBaseAI::resolveAINamedCollection(context, deployment.credentials);
            auto provider
                = createAIProvider(context, credentials.provider, credentials.endpoint, credentials.api_key, credentials.api_version);

            AIChatRequest request;
            request.function_name = name;
            request.query_id = context->getCurrentQueryId();
            request.initial_query_id = context->getClientInfo().initial_query_id;
            request.model = deployment.model;
            request.user_message = FunctionBaseAI::sanitizeForModel(prompts.getDataAt(row));
            request.system_prompt = FunctionBaseAI::sanitizeForModel(options.system_prompt);
            request.max_tokens = options.max_tokens;
            request.temperature = static_cast<float>(options.temperature);

            LLMQueryLogElement event;
            event.request_id = UUIDHelpers::generateV4();
            event.query_id = request.query_id;
            event.initial_query_id = request.initial_query_id;
            event.user = context->getUserName();
            event.route = route;
            event.deployment = deployment.name;
            event.model = deployment.model;
            event.config_version = reservation->version;
            event.prompt = request.user_message;
            event.system_prompt = request.system_prompt;
            event.max_tokens = request.max_tokens;
            event.temperature = static_cast<Float64>(request.temperature);
            setEventTime(event);
            request_log->add(
                [&](auto & entry)
                {
                    entry = event;
                });

            AIResponse response;
            Stopwatch stopwatch;
            SCOPE_EXIT({
                quota->recordTokens(response.input_tokens, response.output_tokens);
                ProfileEvents::increment(ProfileEvents::AIInputTokens, response.input_tokens);
                ProfileEvents::increment(ProfileEvents::AIOutputTokens, response.output_tokens);
                ProfileEvents::increment(ProfileEvents::AIRequestMicroseconds, stopwatch.elapsedMicroseconds());
            });
            try
            {
                if (!quota->recordApiCall())
                    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "`llmQuery` API-call quota is exhausted");
                ProfileEvents::increment(ProfileEvents::AIAPICalls);
                provider->call(request, policy.timeouts, response);
                if (response.finish_reason != FinishReason::Complete && response.finish_reason != FinishReason::Unknown)
                    throw Exception(ErrorCodes::AI_PROVIDER_RESPONSE_INCOMPLETE, "The LLM provider did not return a complete response");
                event.type = LLMQueryLogElement::RequestFinish;
            }
            catch (...)
            {
                event.type = LLMQueryLogElement::RequestException;
                ProfileEvents::increment(ProfileEvents::AIAPICallsFailed);
                event.exception_code = getCurrentExceptionCode();
                event.prompt.clear();
                event.system_prompt.clear();
                event.response = response.result;
                event.finish_reason = response.raw_finish_reason;
                event.input_tokens = response.input_tokens;
                event.output_tokens = response.output_tokens;
                event.duration_microseconds = stopwatch.elapsedMicroseconds();
                setEventTime(event);
                request_log->add(
                    [&](auto & entry)
                    {
                        entry = event;
                    });
                throw;
            }
            event.prompt.clear();
            event.system_prompt.clear();
            event.response = response.result;
            event.finish_reason = response.raw_finish_reason;
            event.input_tokens = response.input_tokens;
            event.output_tokens = response.output_tokens;
            event.duration_microseconds = stopwatch.elapsedMicroseconds();
            setEventTime(event);
            request_log->add(
                [&](auto & entry)
                {
                    entry = event;
                });
            quota->throwIfCancelled();
            result->insertData(response.result.data(), response.result.size());
        }
        if (null_map)
            return ColumnNullable::create(std::move(result), std::move(null_map));
        return result;
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(LLMQuery)
{
    factory.registerFunction<FunctionLLMQuery>(FunctionDocumentation{
        .description = R"(
Sends a synchronous text request through a Keeper-configured model route and returns the response.
Deployments are managed through `system.llm_deployments`. Selection is weighted among deployments
with capacity on the current server. RPM and concurrency limits are per server, not cluster-wide.
Each dispatched request appends a start and terminal event to `system.llm_query_log`; events are
flushed asynchronously, like `system.query_log`. This function does not automatically retry requests.
Requires `allow_experimental_llm_query`, the `llmQuery` privilege and access to the selected named collection.
)",
        .syntax = "llmQuery(route, prompt[, params])",
        .arguments
        = {{"route", "Constant logical model route.", {"String"}},
           {"prompt", "Question or prompt. A NULL prompt produces NULL without calling the provider.", {"String", "Nullable(String)"}},
           {"params",
            "Optional constant parameter map: `system_prompt`, `temperature` (0 to 2; default 0.7), and `max_tokens` (1 to 1000000; "
            "default 1024).",
            {"Map(String, String)"}}},
        .returned_value = {"The model response. Provider and admission errors propagate as exceptions.", {"String", "Nullable(String)"}},
        .examples = {{"Ask a configured model", "SELECT llmQuery('chat', 'What is 2 + 2?')", "4"}},
        .introduced_in = {26, 12},
        .category = FunctionDocumentation::Category::AI});
}

}
