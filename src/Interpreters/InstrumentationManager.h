#pragma once

#include "config.h"
#include <base/types.h>

namespace DB::Instrumentation
{

enum class EntryType : UInt8
{
    ENTRY,
    EXIT,
    ENTRY_AND_EXIT
};

}

#if USE_XRAY

#include <Interpreters/Context_fwd.h>
#include <Common/callOnce.h>
#include <Common/SharedMutex.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <xray/xray_interface.h>

#include <chrono>
#include <functional>
#include <unordered_map>
#include <variant>
#include <vector>

namespace DB
{

namespace Instrumentation
{

EntryType fromXRayEntryType(XRayEntryType entry_type);
String entryTypeToString(EntryType entry_type);

struct All {};

}

struct TraceLogElement;

class InstrumentationManager
{
public:
    using InstrumentedParameter = std::variant<String, Int64, Float64>;

    enum class HandlerType : UInt8
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
        Instrumentation::EntryType entry_type;
        String symbol;
        std::vector<InstrumentedParameter> parameters;

        String toString() const;
        bool operator<(const InstrumentedPointInfo & other) const { return id < other.id; }
    };

    struct FunctionInfo
    {
        Int32 function_id;
        String function_name;

        FunctionInfo(Int32 function_id_, const String & function_name_) :
            function_id(function_id_),
            function_name(function_name_)
        {}

        bool operator<(const FunctionInfo & other) const { return function_id < other.function_id; }
    };

    struct Id {};
    struct FunctionId {};
    struct FunctionName {};

    using FunctionsContainer = boost::multi_index_container<
        FunctionInfo,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionId>, boost::multi_index::member<FunctionInfo, Int32, &FunctionInfo::function_id>>,
            boost::multi_index::hashed_unique<boost::multi_index::tag<FunctionName>, boost::multi_index::member<FunctionInfo, String, &FunctionInfo::function_name>>
        >>;

    using XRayHandlerFunction = std::function<void(XRayEntryType, const InstrumentedPointInfo &)>;

    static InstrumentationManager & instance();

    [[clang::xray_never_instrument]] void unpatchFunction(std::variant<UInt64, Instrumentation::All, String> id);
    [[clang::xray_never_instrument]] static bool shouldPatchFunction(String function_to_patch, String full_qualified_function);
    [[clang::xray_never_instrument]] void patchFunction(ContextPtr context, const String & function_name, const String & handler_name, Instrumentation::EntryType entry_type, const std::vector<InstrumentedParameter> & parameters);

    using InstrumentedPoints = std::vector<InstrumentedPointInfo>;
    InstrumentedPoints getInstrumentedPoints() const;
    const FunctionsContainer & getFunctions();

private:
    using InstrumentedPointContainer = boost::multi_index_container<
        InstrumentedPointInfo,
        boost::multi_index::indexed_by<
            boost::multi_index::hashed_unique<boost::multi_index::tag<Id>, boost::multi_index::member<InstrumentedPointInfo, UInt64, &InstrumentedPointInfo::id>>,
            boost::multi_index::ordered_non_unique<boost::multi_index::tag<FunctionId>,
                boost::multi_index::composite_key<
                    InstrumentedPointInfo,
                    boost::multi_index::member<InstrumentedPointInfo, Int32, &InstrumentedPointInfo::function_id>,
                    boost::multi_index::member<InstrumentedPointInfo, UInt64, &InstrumentedPointInfo::id>>>
        >>;

    InstrumentationManager();

    [[clang::xray_never_instrument]] void ensureInitialization();
    [[clang::xray_never_instrument]] void registerHandler(const String & name, XRayHandlerFunction handler);
    [[clang::xray_never_instrument]] void parseInstrumentationMap();

    [[clang::xray_never_instrument]] void patchFunctionIfNeeded(Int32 function) TSA_REQUIRES(shared_mutex);
    [[clang::xray_never_instrument]] void unpatchFunctionIfNeeded(Int32 function) TSA_REQUIRES(shared_mutex);

    [[clang::xray_never_instrument]] static void dispatchHandler(Int32 func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] void dispatchHandlerImpl(Int32 func_id, XRayEntryType entry_type);
    [[clang::xray_never_instrument]] TraceLogElement createTraceLogElement(const InstrumentedPointInfo & instrumented_point, XRayEntryType entry_type, std::chrono::system_clock::time_point event_time) const;
    [[clang::xray_never_instrument]] void sleep(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);
    [[clang::xray_never_instrument]] void log(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);
    [[clang::xray_never_instrument]] void profile(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point);

    OnceFlag initialized;
    FunctionsContainer functions_container;
    std::unordered_map<String, XRayHandlerFunction> handler_name_to_function;

    mutable SharedMutex shared_mutex;
    UInt64 instrumented_point_ids TSA_GUARDED_BY(shared_mutex);
    InstrumentedPointContainer instrumented_points TSA_GUARDED_BY(shared_mutex);
};

}

#endif
