#include <Interpreters/XRayInstrumentationManager.h>

#if USE_XRAY

#include <cstdint>
#include <string>

#include <filesystem>
#include <print>
#include <stdexcept>
#include <thread>
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
#include <base/getThreadId.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Common/SharedLockGuard.h>
#include <Interpreters/Context.h>
#include <Interpreters/XRayInstrumentationProfilingLog.h>
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

const String SleepHandler
    = "sleep";
const String LogHandler
    = "log";
const String ProfileHandler
    = "profile";

void XRayInstrumentationManager::registerHandler(const String & name, XRayHandlerFunction handler)
{
    xrayHandlerNameToFunction[name] = handler;
}

XRayInstrumentationManager::XRayInstrumentationManager()
{
    registerHandler(SleepHandler, [this](int32_t func_id, XRayEntryType entry_type) { sleep(func_id, entry_type); });
    registerHandler(LogHandler, [this](int32_t func_id, XRayEntryType entry_type) { log(func_id, entry_type); });
    registerHandler(ProfileHandler, [this](int32_t func_id, XRayEntryType entry_type) { profile(func_id, entry_type); });
    parseXRayInstrumentationMap();
}

XRayInstrumentationManager & XRayInstrumentationManager::instance()
{
    static XRayInstrumentationManager instance;
    return instance;
}

String XRayInstrumentationManager::toLower(const String & s)
{
    String lower = s;
    std::ranges::transform(lower, lower.begin(), ::tolower);
    return lower;
}

HandlerType XRayInstrumentationManager::getHandlerType(const String & handler_name)
{
    String name_lower = toLower(handler_name);
    if (name_lower == SleepHandler)
        return HandlerType::Sleep;

    if (name_lower == LogHandler)
        return HandlerType::Log;

    if (name_lower == ProfileHandler)
        return HandlerType::Profile;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown handler type: ({})", handler_name);
}

void XRayInstrumentationManager::setHandlerAndPatch(const String & function_name, const String & handler_name, std::optional<std::vector<InstrumentParameter>> &parameters, ContextPtr context)
{
    auto handler_name_lower = toLower(handler_name);
    auto handler_it = xrayHandlerNameToFunction.find(handler_name_lower);
    if (handler_it == xrayHandlerNameToFunction.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown XRay handler: ({})", handler_name);

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

    HandlerType type;
    try
    {
        type = getHandlerType(handler_name_lower);
    }
    catch (const std::exception & e)
    {
        throw e;
    }

    SharedLockGuard lock(shared_mutex);
    auto handlers_set_it = functionIdToHandlers.find(function_id);
    if (handlers_set_it !=  functionIdToHandlers.end() && handlers_set_it->second.contains(type))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler of this type is already installed for function ({})", function_name);
    }

    if (handlers_set_it ==  functionIdToHandlers.end() || handlers_set_it->second.empty())
    {
        __xray_set_handler(&XRayInstrumentationManager::dispatchHandler);
        __xray_patch_function(function_id);
    }

    instrumented_functions.emplace_front(instrumentation_point_id, function_id, function_name, handler_name_lower, parameters, context);
    functionIdToHandlers[function_id][type] = instrumented_functions.begin();
    instrumentation_point_id++;
}


void XRayInstrumentationManager::unpatchFunction(const String & function_name, const String & handler_name)
{
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
    HandlerType type = getHandlerType(handler_name);

    SharedLockGuard lock(shared_mutex);
    if (!functionIdToHandlers.contains(function_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "This function wasn't previously instrumented, nothing to unpatch: ({})", function_name);
    if (!functionIdToHandlers[function_id].contains(type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "This function was not instrumenented with this handler type, nothing to unpatch: ({}), ({})", function_name, handler_name);
    instrumented_functions.erase(functionIdToHandlers[function_id][type]);
    functionIdToHandlers[function_id].erase(type);
    if (functionIdToHandlers[function_id].empty())
    {
        functionIdToHandlers.erase(function_id);
        __xray_unpatch_function(function_id);
    }
}

XRayInstrumentationManager::InstrumentedFunctions XRayInstrumentationManager::getInstrumentedFunctions()
{
    SharedLockGuard lock(shared_mutex);
    return instrumented_functions;
}

XRayHandlerFunction XRayInstrumentationManager::getHandler(const String & name) const
{
    auto it = xrayHandlerNameToFunction.find(name);
    if (it == xrayHandlerNameToFunction.end())
        throw std::runtime_error("Handler not found: " + name);
    return it->second;
}

void XRayInstrumentationManager::dispatchHandler(int32_t func_id, XRayEntryType entry_type)
{
    XRayInstrumentationManager::instance().dispatchHandlerImpl(func_id, entry_type);
}

void XRayInstrumentationManager::dispatchHandlerImpl(int32_t func_id, XRayEntryType entry_type)
{
    static thread_local bool in_hook = false;
    if (in_hook) return;
    in_hook = true;
    SCOPE_EXIT({ in_hook = false; });

    SharedLockGuard lock(shared_mutex);
    auto handlers_set_it = functionIdToHandlers.find(func_id);
    if (handlers_set_it == functionIdToHandlers.end())
    {
        return;
    }

    for (const auto & [type, ip_it] : handlers_set_it->second)
    {
        auto handler_it = xrayHandlerNameToFunction.find(ip_it->handler_name);
        if (handler_it == xrayHandlerNameToFunction.end())
        {
            LOG_ERROR(getLogger("XRayInstrumentationManager::dispatchHandler"), "Handler not found");
        }
        auto handler = handler_it->second;
        if (handler)
        {
            try
            {
                handler(func_id, entry_type);
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(getLogger("XRayInstrumentationManager::dispatchHandler"), "Exception in handler '{}': {}", ip_it->handler_name, e.what());
            }
        }
        else
        {
            LOG_ERROR(getLogger("XRayInstrumentationManager::dispatchHandler"), "Handler not found");
        }
    }
}


// Takes path to the elf-binary file(that should contain xray_instr_map section),
// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
void XRayInstrumentationManager::parseXRayInstrumentationMap()
{
    auto binary_path = std::filesystem::canonical(std::filesystem::path("/proc/self/exe")).string();

    /// Load the XRay instrumentation map from the binary
    auto instr_map_or_error = loadInstrumentationMap(binary_path);
    if (!instr_map_or_error)
    {
        errs() << "Failed to load instrumentation map: " << toString(instr_map_or_error.takeError()) << "\n";
    }
    auto &instr_map = *instr_map_or_error;

    /// Retrieve the mapping of function IDs to addresses
    auto function_addresses = instr_map.getFunctionAddresses();

    /// Initialize the LLVM symbolizer to resolve function names
    LLVMSymbolizer symbolizer;


    /// Iterate over all instrumented functions
    for (const auto &[func_id, addr] : function_addresses)
    {
        /// Create a SectionedAddress structure to hold the function address
        object::SectionedAddress module_address;
        module_address.Address = addr;
        module_address.SectionIndex = object::SectionedAddress::UndefSection;

        /// Default function name if symbolization fails
        String function_name = UNKNOWN;

        /// Attempt to symbolize the function address (resolve its name)
        if (auto res_or_err = symbolizer.symbolizeCode(binary_path, module_address))
        {
            auto &di = *res_or_err;
            if (di.FunctionName != DILineInfo::BadString)
                function_name = di.FunctionName;
        }

        /// map function ID to its resolved name and vice versa
        if (function_name != UNKNOWN)
        {
            auto stripped_function_name = extractNearestNamespaceAndFunction(function_name);
            strippedFunctionNameToXRayID[stripped_function_name].push_back(func_id);
            functionNameToXRayID[function_name] = func_id;
            xrayIdToFunctionName[func_id] = stripped_function_name;
        }
    }
}

void XRayInstrumentationManager::sleep(int32_t func_id, XRayEntryType entry_type)
{
    static thread_local bool in_hook = false;
    if (in_hook || entry_type != XRayEntryType::ENTRY)
    {
        return;
    }
    in_hook = true;
    SCOPE_EXIT({ in_hook = false; });

    SharedLockGuard lock(shared_mutex);
    HandlerType type = HandlerType::Sleep;
    auto parameters_it = functionIdToHandlers[func_id].find(type);
    if (parameters_it == functionIdToHandlers[func_id].end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for sleep instrumentation");
    }
    auto & params_opt = parameters_it->second->parameters;
    if (!params_opt.has_value())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for sleep instrumentation");
    }
    const auto & params = params_opt.value();

    if (params.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected exactly one parameter for instrumentation, but got {}", params.size());
    }

    const auto & param = params[0];

    if (std::holds_alternative<Int64>(param))
    {
        Int64 seconds = std::get<Int64>(param);
        if (seconds < 0)
        {
            throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Sleep duration must be non-negative");
        }
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
    }
    else if (std::holds_alternative<Float64>(param))
    {
        Float64 seconds = std::get<Float64>(param);
        if (seconds < 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sleep duration must be non-negative");
        }
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::duration<double>(seconds));
        std::this_thread::sleep_for(duration);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected numeric parameter (Int64 or Float64) for sleep, but got something else");
    }
}

void XRayInstrumentationManager::log(int32_t func_id, XRayEntryType entry_type)
{
    static thread_local bool in_hook = false;
    if (in_hook || entry_type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    SCOPE_EXIT({ in_hook = false; });

    SharedLockGuard lock(shared_mutex);
    HandlerType type = HandlerType::Log;
    auto parameters_it = functionIdToHandlers[func_id].find(type);
    if (parameters_it == functionIdToHandlers[func_id].end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for log instrumentation");
    }
    auto & params_opt = parameters_it->second->parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for log instrumentation");

    const auto & params = params_opt.value();

    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected exactly one parameter for instrumentation, but got {}", params.size());

    const auto & param = params[0];

    if (std::holds_alternative<String>(param))
    {
        String logger_info = std::get<String>(param);
        auto function_name = parameters_it->second->function_name;
        LOG_DEBUG(getLogger("XRayInstrumentationManager::log"), "{}: {}", function_name, logger_info);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String for log, but got something else");
    }
}

void XRayInstrumentationManager::profile(int32_t func_id, XRayEntryType entry_type)
{
    static thread_local bool in_hook = false;
    if (in_hook)
    {
        return;
    }

    in_hook = true;
    SCOPE_EXIT({ in_hook = false; });

    SharedLockGuard lock(shared_mutex);
    LOG_DEBUG(getLogger("XRayInstrumentationManager::profile"), "function with id {}", toString(func_id));
    HandlerType type = HandlerType::Profile;
    static thread_local std::unordered_map<int32_t, XRayInstrumentationProfilingLogElement> active_elements;
    if (entry_type == XRayEntryType::ENTRY)
    {
        auto parameters_it = functionIdToHandlers[func_id].find(type);
        if (parameters_it == functionIdToHandlers[func_id].end())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing null parameter for profiling instrumentation");
        }
        auto & params_opt = parameters_it->second->parameters;
        if (params_opt.has_value())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected parameters for profiling instrumentation");
        }
        auto context_it = functionIdToHandlers[func_id].find(type);
        if (context_it == functionIdToHandlers[func_id].end())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No context for profiling instrumentation");
        }
        auto & context = context_it->second->context;
        if (!context)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No context for profiling instrumentation");
        }
        XRayInstrumentationProfilingLogElement element;
        element.function_name = xrayIdToFunctionName[func_id];
        element.tid = getThreadId();
        using namespace std::chrono;

        auto now = system_clock::now();
        auto now_us = duration_cast<microseconds>(now.time_since_epoch()).count();

        element.event_time = time_t(duration_cast<seconds>(now.time_since_epoch()).count());
        element.event_time_microseconds = Decimal64(now_us);

        element.query_id = CurrentThread::isInitialized() ? CurrentThread::getQueryId() : "";
        element.function_id = func_id;

        active_elements[func_id] = std::move(element);
    }
    else if (entry_type == XRayEntryType::EXIT)
    {
        auto it = active_elements.find(func_id);
        if (it != active_elements.end())
        {
            auto & element = it->second;
            auto now = std::chrono::system_clock::now();
            auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

            auto start_us = Int64(element.event_time_microseconds);
            element.duration_microseconds = Decimal64(now_us - start_us);

            auto context_it = functionIdToHandlers[func_id].find(type);
            if (context_it == functionIdToHandlers[func_id].end())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No context for profiling instrumentation");
            }
            auto & context = context_it->second->context;
            if (context)
            {
                if (auto log = context->getInstrumentationProfilingLog())
                {
                    log->add(std::move(element));
                }
            }
            active_elements.erase(it);
        }
    }
}

std::string_view XRayInstrumentationManager::removeTemplateArgs(std::string_view input)
{
    std::string_view result = input;
    size_t pos = result.find('<');
    if (pos == std::string_view::npos)
        return result;

    return result.substr(0, pos);
}

String XRayInstrumentationManager::extractNearestNamespaceAndFunction(std::string_view signature)
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
        size_t last_space = before_args.rfind(' ');
        size_t method_name = second_last_colon;

        if (last_space != std::string_view::npos && second_last_colon != std::string_view::npos && last_space > second_last_colon)
            method_name = last_space - 1;

        if (method_name != std::string_view::npos)
            class_or_namespace_name = before_args.substr(method_name + 2, last_colon - (method_name + 2));
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

    String result;
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
