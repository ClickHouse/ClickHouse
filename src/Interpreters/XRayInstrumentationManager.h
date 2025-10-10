#pragma once

#include "config.h"

#if USE_XRAY

#include <unordered_map>
#include <list>
#include <vector>
#include <variant>

#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/SharedMutex.h>
#include <xray/xray_interface.h>

class XRayInstrumentationManagerTest;

namespace DB
{

enum class HandlerType
{
    Sleep,
    Log,
    Profile,
};

using XRayHandlerFunction = std::function<void(int32_t, XRayEntryType)>;
using InstrumentParameter = std::variant<String, Int64, Float64>;

class XRayInstrumentationManager
{
public:
    struct InstrumentedFunctionInfo
    {
        uint64_t id;
        uint32_t function_id;
        String function_name;
        String handler_name;
        std::optional<std::vector<InstrumentParameter>> parameters;
        ContextPtr context;
    };

    static XRayInstrumentationManager & instance();

    void setHandlerAndPatch(const String & function_name, const String & handler_name, std::optional<std::vector<InstrumentParameter>> &parameters, ContextPtr context);
    void unpatchFunction(uint64_t instrumentation_point_id);

    using InstrumentedFunctions = std::list<InstrumentedFunctionInfo>;
    using HandlerTypeToIP = std::unordered_map<HandlerType, InstrumentedFunctions::iterator>;

    InstrumentedFunctions getInstrumentedFunctions();

protected:
    static std::string_view removeTemplateArgs(std::string_view input);
    static String extractNearestNamespaceAndFunction(std::string_view signature);

private:
    XRayInstrumentationManager();
    void registerHandler(const String & name, XRayHandlerFunction handler);
    XRayHandlerFunction getHandler(const String & name) const;
    void parseXRayInstrumentationMap();

    HandlerType getHandlerType(const String & handler_name);

    [[clang::xray_never_instrument]] static void dispatchHandler(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void dispatchHandlerImpl(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void sleep(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void log(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void profile(int32_t func_id, XRayEntryType entry_type);

    std::unordered_map<String, XRayHandlerFunction> xrayHandlerNameToFunction;
    std::unordered_map<String, int64_t> functionNameToXRayID;
    std::unordered_map<String, std::vector<int64_t>> strippedFunctionNameToXRayID;
    std::unordered_map<int64_t, String> xrayIdToFunctionName;

    SharedMutex shared_mutex;
    std::atomic<uint64_t> instrumentation_point_ids;
    InstrumentedFunctions instrumented_functions TSA_GUARDED_BY(shared_mutex);
    std::unordered_map<int32_t, HandlerTypeToIP> functionIdToHandlers TSA_GUARDED_BY(shared_mutex);

    static constexpr const char* UNKNOWN = "<unknown>";

    friend class ::XRayInstrumentationManagerTest;
};

}

#endif
