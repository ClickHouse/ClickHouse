#include <Interpreters/XRayInstrumentationManager.h>

#if USE_XRAY

#include <cstdint>
#include <string>
#include <filesystem>
#include <print>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <variant>
#include <iterator>
#include <latch>

#include <base/getThreadId.h>
#include <Interpreters/XRayInstrumentationProfilingLog.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Common/SharedLockGuard.h>
#include <Common/ThreadStatus.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>

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

auto logger = getLogger("XRayInstrumentationManager");

void XRayInstrumentationManager::registerHandler(const String & name, XRayHandlerFunction handler)
{
    handler_name_to_function[name] = handler;
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

HandlerType XRayInstrumentationManager::getHandlerType(const String & handler_name)
{
    String name_lower = Poco::toLower(handler_name);
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
    auto handler_name_lower = Poco::toLower(handler_name);
    auto handler_it = handler_name_to_function.find(handler_name_lower);
    if (handler_it == handler_name_to_function.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown XRay handler: ({})", handler_name);

    int64_t function_id;
    auto fn_it = functions_container.get<FunctionName>().find(function_name);
    if (fn_it != functions_container.get<FunctionName>().end())
        function_id = fn_it->function_id;
    else
    {
        auto stripped_it = functions_container.get<StrippedFunctionName>().find(function_name);
        if (stripped_it != functions_container.get<StrippedFunctionName>().end())
            function_id = stripped_it->function_id;
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

    instrumented_functions.emplace_front(instrumentation_point_ids, function_id, function_name, handler_name_lower, parameters, context);
    functionIdToHandlers[function_id][type] = instrumented_functions.begin();
    instrumentation_point_ids++;
}


void XRayInstrumentationManager::unpatchFunction(std::variant<uint64_t, bool> id)
{
    SharedLockGuard lock(shared_mutex);

    auto remove_function = [this](const InstrumentedFunctionInfo & instrumented_function) TSA_REQUIRES_SHARED(shared_mutex)
    {
        HandlerType type = getHandlerType(instrumented_function.handler_name);
        instrumented_functions.erase(functionIdToHandlers[instrumented_function.function_id][type]);
        functionIdToHandlers[instrumented_function.function_id].erase(type);
        if (functionIdToHandlers[instrumented_function.function_id].empty())
        {
            functionIdToHandlers.erase(instrumented_function.function_id);
            __xray_unpatch_function(instrumented_function.function_id);
        }
    };

    if (std::holds_alternative<bool>(id))
    {
        for (const auto & instrumented_function : instrumented_functions)
            remove_function(instrumented_function);
    }
    else
    {
        std::optional<InstrumentedFunctionInfo> function;
        for (const auto & function_info : instrumented_functions)
        {
            if (function_info.id == std::get<uint64_t>(id))
            {
                function = function_info;
                break;
            }
        }

        if (!function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown instrumentation point id to remove: ({})", std::get<uint64_t>(id));

        remove_function(function.value());
    }
}

XRayInstrumentationManager::InstrumentedFunctions XRayInstrumentationManager::getInstrumentedFunctions()
{
    SharedLockGuard lock(shared_mutex);
    return instrumented_functions;
}

XRayHandlerFunction XRayInstrumentationManager::getHandler(const String & name) const
{
    auto it = handler_name_to_function.find(name);
    if (it == handler_name_to_function.end())
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
        auto handler_it = handler_name_to_function.find(ip_it->handler_name);
        if (handler_it == handler_name_to_function.end())
        {
            LOG_ERROR(logger, "Handler not found");
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
                LOG_ERROR(logger, "Exception in handler '{}': {}", ip_it->handler_name, e.what());
            }
        }
        else
        {
            LOG_ERROR(logger, "Handler not found");
        }
    }
}


/// Takes path to the elf-binary file(that should contain xray_instr_map section),
/// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
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

    functions_container.reserve(function_addresses.size());

    const auto & context = CurrentThread::getQueryContext();
    const auto num_jobs = std::thread::hardware_concurrency();

    LOG_DEBUG(logger, "Starting to parse the XRay instrumentation map using {} jobs. This takes a few seconds...", num_jobs);

    struct Job
    {
        std::pair<uint64_t, uint64_t> range;
        FunctionsContainer functions_container;
        BackgroundSchedulePool::TaskHolder task;
    };
    std::latch work_done(num_jobs);

    auto work = [&function_addresses, &binary_path, &work_done](Job & job)
    {
        LOG_TRACE(logger, "Start work job with range [{}, {})", job.range.first, job.range.second);

        /// Initialize the LLVM symbolizer to resolve function names
        LLVMSymbolizer symbolizer;

        /// Iterate over all instrumented functions
        for (uint64_t i = job.range.first; i < job.range.second; ++i)
        {
            auto it = std::next(function_addresses.begin(), i);

            auto func_id = it->first;
            auto addr = it->second;

            /// Create a SectionedAddress structure to hold the function address
            object::SectionedAddress module_address;
            module_address.Address = addr;
            module_address.SectionIndex = object::SectionedAddress::UndefSection;

            /// Default function name if symbolization fails
            String function_name = UNKNOWN;

            /// Attempt to symbolize the function address (resolve its name)
            if (auto res_or_err = symbolizer.symbolizeCode(binary_path, module_address))
            {
                auto & di = *res_or_err;
                if (di.FunctionName != DILineInfo::BadString)
                    function_name = di.FunctionName;
            }

            /// map function ID to its resolved name and vice versa
            if (function_name != UNKNOWN)
            {
                auto stripped_function_name = extractNearestNamespaceAndFunction(function_name);
                job.functions_container.get<FunctionId>().emplace(func_id, function_name, stripped_function_name);
            }
        }

        LOG_TRACE(logger, "Finish work job with range [{}, {})", job.range.first, job.range.second);
        work_done.count_down();
    };

    std::vector<Job> jobs(num_jobs);
    auto chunk_size = function_addresses.size() / num_jobs;
    auto remainder = function_addresses.size() % num_jobs;

    LOG_DEBUG(logger, "Dividing the work to parse {} symbols into {} parallel jobs. Chunk size: {}, remainder for last job: {}", function_addresses.size(), num_jobs, chunk_size, remainder);
    for (size_t i = 0; i < jobs.size(); ++i)
    {
        auto & job = jobs[i];
        job.range.first = i * chunk_size;
        job.range.second = job.range.first + chunk_size;
        if (i == (jobs.size() - 1))
            job.range.second += remainder;

        job.task = context->getSchedulePool().createTask(fmt::format("{}_{}", "ParserXRayFunctions", i), [work, &job]() { work(job); });
        job.task->schedule();
    }

    work_done.wait();
    for (const auto & job : jobs)
    {
        for (const auto & function_info : job.functions_container)
            functions_container.get<FunctionId>().emplace(function_info);
    }

    LOG_DEBUG(logger, "Finished parsing the XRay instrumentation map");
}

void XRayInstrumentationManager::sleep(int32_t func_id, XRayEntryType entry_type)
{
    if (entry_type != XRayEntryType::ENTRY)
        return;

    SharedLockGuard lock(shared_mutex);
    HandlerType type = HandlerType::Sleep;
    auto parameters_it = functionIdToHandlers[func_id].find(type);
    if (parameters_it == functionIdToHandlers[func_id].end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for sleep instrumentation");
    }
    const auto params_opt = parameters_it->second->parameters;
    lock.unlock();

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
    if (entry_type != XRayEntryType::ENTRY)
        return;

    SharedLockGuard lock(shared_mutex);
    HandlerType type = HandlerType::Log;
    auto parameters_it = functionIdToHandlers[func_id].find(type);
    if (parameters_it == functionIdToHandlers[func_id].end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for log instrumentation");
    }
    const auto params_opt = parameters_it->second->parameters;
    lock.unlock();

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
        LOG_DEBUG(logger, "{}: {}\nStack trace:\n{}", function_name, logger_info, StackTrace().toString());
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
    LOG_DEBUG(logger, "Profile: function with id {}", toString(func_id));
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
        element.function_name = functions_container.get<FunctionId>().find(func_id)->stripped_function_name;
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
