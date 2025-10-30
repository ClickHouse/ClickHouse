#pragma once

#include "config.h"

#if USE_XRAY

#include <unordered_map>
#include <vector>
#include <variant>

#include <base/types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/SharedMutex.h>
#include <xray/xray_interface.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>


class InstrumentationManagerTest;

namespace DB
{

class InstrumentationManager
{
public:
    using InstrumentedParameter = std::variant<String, Int64, Float64>;

    enum class HandlerType
    {
        SLEEP,
        LOG,
        PROFILE,
    };

    struct InstrumentedPointInfo
    {
        ContextPtr context;
        UInt64 id;
        Int32 function_id;
        String function_name;
        String handler_name;
        std::optional<XRayEntryType> entry_type;
        std::optional<std::vector<InstrumentedParameter>> parameters;

        String toString()
        {
            String entry_type_str = !entry_type.has_value() ? "none" : (entry_type.value() == XRayEntryType::ENTRY ? "entry" : "exit");
            String parameters_str;
            if (parameters.has_value())
            {
                parameters_str = ", parameters (";
                for (const auto & param : parameters.value())
                {
                    if (std::holds_alternative<String>(param))
                        parameters_str += fmt::format("{}, ", std::get<String>(param));
                    else if (std::holds_alternative<Int64>(param))
                        parameters_str += fmt::format("{}, ", std::get<Int64>(param));
                    else if (std::holds_alternative<Float64>(param))
                        parameters_str += fmt::format("{}, ", std::get<Float64>(param));
                }
                parameters_str = ")";
            }

            return fmt::format("id {}, function_id {}, function_name '{}', handler_name {}, entry_type {}{}",
                id, function_id, function_name, handler_name, entry_type_str, parameters_str);
        }
    };

    using XRayHandlerFunction = std::function<void(XRayEntryType, const InstrumentedPointInfo &)>;

    static InstrumentationManager & instance();

    void patchFunction(ContextPtr context, const String & function_name, const String & handler_name, std::optional<XRayEntryType> entry_type, std::optional<std::vector<InstrumentedParameter>> & parameters);
    void unpatchFunction(std::variant<UInt64, bool> id);

    using InstrumentedPoints = std::vector<InstrumentedPointInfo>;
    InstrumentedPoints getInstrumentedPoints();

protected:
    static std::string_view removeTemplateArgs(std::string_view input);
    static String extractNearestNamespaceAndFunction(std::string_view signature);

private:
    struct InstrumentedPointKey
    {
        Int32 function_id;
        std::optional<XRayEntryType> entry_type;
        String handler_name;

        bool operator==(const InstrumentedPointKey & other) const
        {
            return function_id == other.function_id && entry_type == other.entry_type && handler_name == other.handler_name;
        }
    };

    struct InstrumentedPointHash
    {
        std::size_t operator()(const InstrumentationManager::InstrumentedPointKey& k) const
        {
            auto entry_type = !k.entry_type.has_value() ? XRayEntryType::TYPED_EVENT + 1 : k.entry_type.value();
            return ((std::hash<Int32>()(k.function_id)
                    ^ (std::hash<uint8_t>()(static_cast<uint8_t>(entry_type)) << 1)) >> 1)
                    ^ (std::hash<String>()(k.handler_name) << 1);
        }
    };

    struct FunctionInfo
    {
        Int32 function_id;
        String function_name;
        String stripped_function_name;

        FunctionInfo(Int32 function_id_, const String & function_name_, const String & stripped_function_name_) :
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
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionId>, boost::multi_index::member<FunctionInfo, Int32, &FunctionInfo::function_id>>,
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionName>, boost::multi_index::member<FunctionInfo, String, &FunctionInfo::function_name>>,
            boost::multi_index::hashed_non_unique<boost::multi_index::tag<StrippedFunctionName>, boost::multi_index::member<FunctionInfo, String, &FunctionInfo::stripped_function_name>>
        >>;

    InstrumentationManager();
    void registerHandler(const String & name, XRayHandlerFunction handler);
    void parseInstrumentationMap();

    [[clang::xray_never_instrument]] void patchFunctionIfNeeded(Int32 function) TSA_REQUIRES(shared_mutex);
    [[clang::xray_never_instrument]] void unpatchFunctionIfNeeded(Int32 function) TSA_REQUIRES(shared_mutex);

    [[clang::xray_never_instrument]] static void dispatchHandler(Int32 func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void dispatchHandlerImpl(Int32 func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void sleep(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);
    [[clang::xray_never_instrument]] void log(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);
    [[clang::xray_never_instrument]] void profile(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);

    FunctionsContainer functions_container;
    std::vector<std::pair<String, XRayHandlerFunction>> handler_name_to_function;

    SharedMutex shared_mutex;
    std::atomic<UInt64> instrumentation_point_ids;
    std::unordered_map<InstrumentedPointKey, InstrumentedPointInfo, InstrumentedPointHash> instrumented_points TSA_GUARDED_BY(shared_mutex);
    std::unordered_map<Int32, Int32> instrumented_functions TSA_GUARDED_BY(shared_mutex);

    enum class InitializationStatus
    {
        UNINITIALIZED,
        INITIALIZING,
        INITIALIZED
    };

    std::atomic<InitializationStatus> initialization_status = InstrumentationManager::InitializationStatus::UNINITIALIZED;

    friend class ::InstrumentationManagerTest;
};

}

#endif
