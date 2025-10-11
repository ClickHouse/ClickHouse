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

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>


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
    void unpatchFunction(std::variant<uint64_t, bool> id);

    using InstrumentedFunctions = std::list<InstrumentedFunctionInfo>;
    using HandlerTypeToIP = std::unordered_map<HandlerType, InstrumentedFunctions::iterator>;

    InstrumentedFunctions getInstrumentedFunctions();

protected:
    static std::string_view removeTemplateArgs(std::string_view input);
    static String extractNearestNamespaceAndFunction(std::string_view signature);

private:
    struct FunctionInfo
    {
        int64_t function_id;
        String function_name;
        String stripped_function_name;

        FunctionInfo(int64_t function_id_, const String & function_name_, const String & stripped_function_name_) :
            function_id(function_id_),
            function_name(function_name_),
            stripped_function_name(stripped_function_name_)
        {}

        bool operator<(const FunctionInfo & other) const { return function_id < other.function_id; }
    };

    struct FunctionId {};
    struct FunctionName {};
    struct StrippedFunctionName {};

    using FunctionsContainer = boost::multi_index_container<
        FunctionInfo,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionId>, boost::multi_index::member<FunctionInfo, int64_t, &FunctionInfo::function_id>>,
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionName>, boost::multi_index::member<FunctionInfo, String, &FunctionInfo::function_name>>,
            boost::multi_index::hashed_non_unique<boost::multi_index::tag<StrippedFunctionName>, boost::multi_index::member<FunctionInfo, String, &FunctionInfo::stripped_function_name>>
        >>;

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

    FunctionsContainer functions_container;
    std::unordered_map<String, XRayHandlerFunction> handler_name_to_function;

    SharedMutex shared_mutex;
    std::atomic<uint64_t> instrumentation_point_ids;
    InstrumentedFunctions instrumented_functions TSA_GUARDED_BY(shared_mutex);
    std::unordered_map<int32_t, HandlerTypeToIP> functionIdToHandlers TSA_GUARDED_BY(shared_mutex);

    static constexpr const char* UNKNOWN = "<unknown>";

    friend class ::XRayInstrumentationManagerTest;
};

}

#endif
