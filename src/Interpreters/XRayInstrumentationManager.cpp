#include "XRayInstrumentationManager.h"
#include <cstdint>
#include <string>

#if USE_XRAY

#include <filesystem>
#include <print>
#include <stdexcept>
#include <thread>
#include <string_view>
#include <unistd.h>

#include <llvm/Object/Binary.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/XRay/InstrumentationMap.h>
#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/Support/Path.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/IR/Function.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/InstrumentationProfilingLog.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h> 


using namespace llvm;
using namespace llvm::object;
using namespace llvm::xray;
using namespace llvm::symbolize;

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
std::unordered_map<int64_t, std::list<XRayInstrumentationManager::InstrumentedFunctionInfo>::iterator> XRayInstrumentationManager::functionIdToInstrumentPoint;
std::unordered_map<int64_t, std::string> XRayInstrumentationManager::xrayIdToFunctionName;

void XRayInstrumentationManager::registerHandler(const std::string & name, XRayHandlerFunction handler)
{
    std::lock_guard<std::mutex> lock(mutex);
    xrayHandlerNameToFunction[name] = handler;
}


XRayInstrumentationManager::XRayInstrumentationManager()
{
    // maybe would be better to map not handler names but smth else(smth that would be more convenient to use in system statement)
    registerHandler("logEntry", &logEntry);
    registerHandler("logAndSleep", &logAndSleep);
    registerHandler("logEntryExit", &logEntryExit);
    registerHandler("SLEEP", &sleep);
    registerHandler("LOG", &log);
    registerHandler("PROFILE", &profile);
    parseXRayInstrumentationMap();
}

XRayInstrumentationManager & XRayInstrumentationManager::instance()
{
    static XRayInstrumentationManager instance;
    return instance;
}


void XRayInstrumentationManager::setHandlerAndPatch(const std::string & function_name, const std::string & handler_name, std::optional<std::vector<InstrumentParameter>> &parameters, ContextPtr context)
{
    std::lock_guard lock(mutex);
    auto handler_it = xrayHandlerNameToFunction.find(handler_name);
    if (handler_it == xrayHandlerNameToFunction.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown XRAY handler: ({})", handler_name);

    auto handler_function = handler_it->second;

    int64_t function_id;
    auto fn_it = functionNameToXRayID.find(function_name);
    if (fn_it != functionNameToXRayID.end())
        function_id = fn_it->second;
    else
    {
        auto stripped_it = strippedFunctionNameToXRayID.find(function_name);
        if (stripped_it != strippedFunctionNameToXRayID.end())
            function_id = stripped_it->second.back();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown function to instrument: ({})", function_name);
    }
    instrumented_functions.emplace_front(instrumentation_point_id, function_id, function_name, handler_name, parameters, context);
    functionIdToInstrumentPoint[function_id] = instrumented_functions.begin();
    instrumentation_point_id++;
    __xray_set_handler(handler_function);
    __xray_patch_function(function_id);
    // add/update a row "intrumentation_point_id | function_id | function_name | handler_name | parameters" to system.instrument -- TODO
}


void XRayInstrumentationManager::unpatchFunction(const std::string & function_name)
{
    std::lock_guard lock(mutex);
    int64_t function_id;
    auto fn_it = functionNameToXRayID.find(function_name);
    if (fn_it != functionNameToXRayID.end())
        function_id = fn_it->second;
    else
    {
        auto stripped_it = strippedFunctionNameToXRayID.find(function_name);
        if (stripped_it != strippedFunctionNameToXRayID.end())
            function_id = stripped_it->second.back();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown function to instrument: ({})", function_name);
    }
    instrumented_functions.erase(functionIdToInstrumentPoint[function_id]);
    functionIdToInstrumentPoint.erase(function_id);
    __xray_unpatch_function(function_id);
    // delete a row "intrumentation_point_id | function_id | function_name | handler_name | parameters" in system.instrument by functionId
}


XRayHandlerFunction XRayInstrumentationManager::getHandler(const std::string & name) const
{
    std::lock_guard lock(mutex);
    auto it = xrayHandlerNameToFunction.find(name);
    if (it == xrayHandlerNameToFunction.end())
        throw std::runtime_error("Handler not found: " + name);
    return it->second;
}


//Takes path to the elf-binary file(that should contain xray_instr_map section),
// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
void XRayInstrumentationManager::parseXRayInstrumentationMap()
{
    auto binary_path = std::filesystem::canonical(std::filesystem::path("/proc/self/exe")).string();

    // Load the XRay instrumentation map from the binary
    auto instr_map_or_error = loadInstrumentationMap(binary_path);
    if (!instr_map_or_error)
    {
        errs() << "Failed to load instrumentation map: " << toString(instr_map_or_error.takeError()) << "\n";
    }
    auto &instr_map = *instr_map_or_error;

    // Retrieve the mapping of function IDs to addresses
    auto function_addresses = instr_map.getFunctionAddresses();

    // Initialize the LLVM symbolizer to resolve function names
    LLVMSymbolizer symbolizer;


    // Iterate over all instrumented functions
    for (const auto &[FuncID, Addr] : function_addresses)
    {
        // Create a SectionedAddress structure to hold the function address
        object::SectionedAddress module_address;
        module_address.Address = Addr;
        module_address.SectionIndex = object::SectionedAddress::UndefSection;

        // Default function name if symbolization fails
        std::string function_name = "<unknown>";

        // Attempt to symbolize the function address (resolve its name)
        if (auto res_or_err = symbolizer.symbolizeCode(binary_path, module_address))
        {
            auto &di = *res_or_err;
            if (di.FunctionName != DILineInfo::BadString)
                function_name = di.FunctionName;
        }

        // map function ID to its resolved name and vice versa
        if (function_name != "<unknown>")
        {
            auto stripped_function_name = extractNearestNamespaceAndFunction(function_name);
            strippedFunctionNameToXRayID[stripped_function_name].push_back(FuncID);
            functionNameToXRayID[function_name] = FuncID;
            xrayIdToFunctionName[FuncID] = function_name;
        }
    }
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logEntry(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }
    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::println("[logEntry] Entered Function ID {}", FuncId);
    }
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logAndSleep(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::println("[logAndSleep] Function ID {} entered. Sleeping...", FuncId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logEntryExit(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook)
    {
        return;
    }

    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        if (Type == XRayEntryType::ENTRY)
        {
            std::println("[logEntryExit] Entering Function ID {}", FuncId);
        }
        else if (Type == XRayEntryType::EXIT)
        {
            std::println("[logEntryExit] Exiting Function ID {}", FuncId);
        }
    }
    in_hook = false;
}


[[clang::xray_never_instrument]] void XRayInstrumentationManager::sleep(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    // but does XRay allow us to throw exceptions in handlers?
    auto parameters_it = functionIdToInstrumentPoint.find(FuncId);
    auto & params_opt = parameters_it->second->parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for sleep instrumentation"); // or maybe we want to use some default parameter instead of throwing exception??

    const auto & params = params_opt.value();

    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected exactly one parameter for instrumentation, but got {}", params.size());

    const auto & param = params[0];

    if (std::holds_alternative<Int64>(param))
    {
        Int64 seconds = std::get<Int64>(param);
        if (seconds < 0)
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Sleep duration must be non-negative");
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
    }
    else if (std::holds_alternative<Float64>(param))
    {
        Float64 seconds = std::get<Float64>(param);
        if (seconds < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sleep duration must be non-negative");
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(seconds));
        std::this_thread::sleep_for(duration);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected numeric parameter (Int64 or Float64) for sleep, but got something else");
    }
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::log(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    // but does XRay allow us to throw exceptions in handlers?
    auto parameters_it = functionIdToInstrumentPoint.find(FuncId);
    auto & params_opt = parameters_it->second->parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for log instrumentation"); // or maybe we want to use some default parameter instead of throwing exception??

    const auto & params = params_opt.value();

    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected exactly one parameter for instrumentation, but got {}", params.size());

    const auto & param = params[0];

    if (std::holds_alternative<String>(param))
    {
        String logger_info = std::get<String>(param);
        LOG_DEBUG(getLogger("XRayInstrumentationManager::log"), "Instrumentation log: {}", logger_info); //maybe better to write smth else?
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String for log, but got something else");
    }
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::profile(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    // but does XRay allow us to throw exceptions in handlers?
    auto parameters_it = functionIdToInstrumentPoint.find(FuncId);
    auto & params_opt = parameters_it->second->parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected parameters for profiling instrumentation"); // or maybe we want to use some default parameter instead of throwing exception??
    // TODO -- not clear yet how to write what we want to the system.instrumentation_profiling_log
    auto context_it = functionIdToInstrumentPoint.find(FuncId);
    auto & context = context_it->second->context;
    if (!context)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No context for profiling instrumentation");
    if (auto log = context->getInstrumentationProfilingLog())
    {
        InstrumentationProfilingLogElement element;
        element.function_name = xrayIdToFunctionName[FuncId];
        element.category = "function";
        if (Type == XRayEntryType::ENTRY)
            element.phase = "B";
        else
            element.phase = "E";

        element.pid = getpid();  
        element.tid = getThreadId();  
        using namespace std::chrono;

        auto now = system_clock::now();
        auto now_us = duration_cast<microseconds>(now.time_since_epoch()).count();

        element.timestamp = Decimal64(now_us);
        element.event_time = time_t(duration_cast<seconds>(now.time_since_epoch()).count());
        element.query_id = CurrentThread::isInitialized() ? CurrentThread::getQueryId() : ""; 
        element.function_id = FuncId;
        log->add(std::move(element));
    }
    in_hook = false;
}

std::string_view XRayInstrumentationManager::removeTemplateArgs(std::string_view input)
{
    std::string_view result = input;
    size_t pos = result.find('<');
    if (pos == std::string_view::npos)
        return result;

    size_t depth = 0;
    for (size_t i = pos; i < result.size(); ++i)
    {
        if (result[i] == '<')
            ++depth;
        else if (result[i] == '>')
        {
            if (depth > 0)
                --depth;
        }
        else if (depth == 0 && result[i] == ':')
        {
            continue;
        }

        if (depth == 0)
        {
            return result.substr(0, i);
        }
    }

    return result.substr(0, pos);
}

std::string XRayInstrumentationManager::extractNearestNamespaceAndFunction(std::string_view signature)
{
    size_t paren_pos = signature.find('(');
    if (paren_pos == std::string_view::npos)
        return {};

    std::string_view before_args = signature.substr(0, paren_pos);

    size_t last_colon = before_args.rfind("::");

    std::string_view function_name;
    std::string_view class_or_namespace_name;

    if (last_colon != std::string_view::npos)
    {
        function_name = before_args.substr(last_colon + 2);

        size_t second_last_colon = before_args.rfind("::", last_colon - 2);
        if (second_last_colon != std::string_view::npos)
            class_or_namespace_name = before_args.substr(second_last_colon + 2, last_colon - (second_last_colon + 2));
        else
        {
            size_t first_space = before_args.find_last_of(' ', last_colon);
            if (first_space != std::string_view::npos)
                class_or_namespace_name = before_args.substr(first_space + 1, last_colon - (first_space + 1));
            else
                class_or_namespace_name = before_args.substr(0, last_colon);
        }
    }
    else
    {
        function_name = before_args;

        size_t last_space = function_name.rfind(' ');
        if (last_space != std::string_view::npos)
            function_name = function_name.substr(last_space + 1);
    }

    function_name = removeTemplateArgs(function_name);
    class_or_namespace_name = removeTemplateArgs(class_or_namespace_name);

    std::string result;
    if (!class_or_namespace_name.empty())
    {
        result += class_or_namespace_name;
        result += "::";
    }
    result += function_name;

    return result;
}

}

#endif
