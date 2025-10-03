#pragma once

#include "Interpreters/Context_fwd.h"
#include "base/types.h"
#include "config.h"

#if USE_XRAY

#include <string>
#include <unordered_map>
#include <list>
#include <vector>
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

using XRayHandlerFunction = std::function<void(int32_t, XRayEntryType)>;
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
    using HandlerTypeToIP = std::unordered_map<HandlerType, InstrumentedFunctions::iterator>;

protected:
    static std::string_view removeTemplateArgs(std::string_view input);
    static std::string extractNearestNamespaceAndFunction(std::string_view signature);

private:

    XRayInstrumentationManager();
    void registerHandler(const std::string & name, XRayHandlerFunction handler);
    XRayHandlerFunction getHandler(const std::string & name) const;
    void parseXRayInstrumentationMap();

    HandlerType getHandlerType(const std::string & handler_name);
    std::string toLower(const std::string & s);

    [[clang::xray_never_instrument]] static void dispatchHandler(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void dispatchHandlerImpl(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void sleep(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void log(int32_t func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void profile(int32_t func_id, XRayEntryType entry_type);

    std::unordered_map<std::string, XRayHandlerFunction> xrayHandlerNameToFunction;
    std::unordered_map<std::string, int64_t> functionNameToXRayID;
    std::unordered_map<std::string, std::vector<int64_t>> strippedFunctionNameToXRayID;
    std::unordered_map<int64_t, std::string> xrayIdToFunctionName;

    std::shared_mutex shared_mutex;

    uint64_t instrumentation_point_id TSA_GUARDED_BY(shared_mutex);

    std::list<InstrumentedFunctionInfo> instrumented_functions TSA_GUARDED_BY(shared_mutex);

    std::unordered_map<int32_t, HandlerTypeToIP> functionIdToHandlers TSA_GUARDED_BY(shared_mutex);

    static constexpr const char* UNKNOWN = "<unknown>";
};

}

#endif
