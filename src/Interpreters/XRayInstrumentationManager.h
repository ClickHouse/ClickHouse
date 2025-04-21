#pragma once

#include "config.h"

#if USE_XRAY

#include <string>
#include <unordered_map>
#include <list>
#include <mutex>

#include <xray/xray_interface.h>
#include <llvm/Object/Binary.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/XRay/InstrumentationMap.h>
#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/Support/Path.h>
#include <llvm/Support/TargetSelect.h>

namespace DB
{

using XRayHandlerFunction = void(*)(int32_t, XRayEntryType);

class XRayInstrumentationManager
{
public:

    struct InstrumentedFunctionInfo
    {
        uint32_t function_id;
        std::string function_name;
        std::string handler_name;
    };
    static XRayInstrumentationManager & instance();

    void setHandlerAndPatch(const std::string & function_name, const std::string & handler_name);
    void unpatchFunction(const std::string & function_name);

    using InstrumentedFunctions = std::list<InstrumentedFunctionInfo>;
    InstrumentedFunctions getFunctionToInstrument()
    {
        std::lock_guard lock(functions_to_instrument_mutex);
        return instrumented_functions;
    }

private:

    XRayInstrumentationManager();
    void registerHandler(const std::string & name, XRayHandlerFunction handler);
    XRayHandlerFunction getHandler(const std::string & name) const;
    std::unordered_map<int32_t, std::string> parseXRayInstrumentationMap();

    [[clang::xray_never_instrument]] static void logEntry(int32_t FuncId, XRayEntryType Type);
    [[clang::xray_never_instrument]] static void logAndSleep(int32_t FuncId, XRayEntryType Type);
    [[clang::xray_never_instrument]] static void logEntryExit(int32_t FuncId, XRayEntryType Type);
    // TODO: more handlers

    mutable std::mutex mutex;
    std::mutex functions_to_instrument_mutex;
    std::unordered_map<std::string, XRayHandlerFunction> xrayHandlerNameToFunction;
    std::unordered_map<std::string, int64_t> functionNameToXRayID;
    std::unordered_map<int64_t, std::string> xrayIdToFunctionName; // may be unnecessary, but we'll keep it for now
    std::list<InstrumentedFunctionInfo> instrumented_functions;
    std::unordered_map<int64_t, std::list<InstrumentedFunctionInfo>::iterator> functionIdToInstrumentPoint;

    static inline std::mutex log_mutex;
};

}

#endif
