#pragma once

#include "Interpreters/Context_fwd.h"
#include "base/types.h"
#include "config.h"

#if USE_XRAY

#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <variant>
#include <xray/xray_interface.h>

namespace DB
{

enum class HandlerType
{
    Sleep,
    Log,
    Profile,
};

using XRayHandlerFunction = void(*)(int32_t, XRayEntryType);
using InstrumentParameter = std::variant<String, Int64, Float64>;

class XRayInstrumentationManager
{
public:

    struct InstrumentedFunctionInfo
    {
        uint64_t instrumentation_point_id;
        uint32_t function_id;
        std::string function_name;
        std::string handler_name;
        std::optional<std::vector<InstrumentParameter>> parameters;
        ContextPtr context;
    };
    static XRayInstrumentationManager & instance();

    void setHandlerAndPatch(const std::string & function_name, const std::string & handler_name, std::optional<std::vector<InstrumentParameter>> &parameters, ContextPtr context);
    void unpatchFunction(const std::string & function_name, const std::string & handler_name);

    using InstrumentedFunctions = std::list<InstrumentedFunctionInfo>;
    using HandlerTypeToIP = std::unordered_map<HandlerType, std::list<InstrumentedFunctionInfo>::iterator>;
    InstrumentedFunctions getInstrumentedFunctions()
    {
        std::lock_guard lock(functions_to_instrument_mutex);
        return instrumented_functions;
    }

private:

    XRayInstrumentationManager();
    void registerHandler(const std::string & name, XRayHandlerFunction handler);
    XRayHandlerFunction getHandler(const std::string & name) const;
    void parseXRayInstrumentationMap();
    static std::string_view removeTemplateArgs(std::string_view input);
    std::string extractNearestNamespaceAndFunction(std::string_view signature);
    HandlerType getHandlerType(const std::string & handler_name);

    [[clang::xray_never_instrument]] static void dispatchHandler(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] static void sleep(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] static void log(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] static void profile(int32_t func_id, XRayEntryType entry_type);

    mutable std::mutex mutex;
    uint64_t instrumentation_point_id;
    std::mutex functions_to_instrument_mutex;
    static std::unordered_map<std::string, XRayHandlerFunction> xrayHandlerNameToFunction;
    std::unordered_map<std::string, int64_t> functionNameToXRayID;
    std::unordered_map<std::string, std::vector<int64_t>> strippedFunctionNameToXRayID;
    static std::unordered_map<int64_t, std::string> xrayIdToFunctionName;
    std::list<InstrumentedFunctionInfo> instrumented_functions;
    static std::unordered_map<int32_t, HandlerTypeToIP> functionIdToHandlers;

    static inline std::mutex log_mutex;
    static inline std::shared_mutex shared_mutex;

    static constexpr const char* UNKNOWN = "<unknown>";
};

}

#endif
