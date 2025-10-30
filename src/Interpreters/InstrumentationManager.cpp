#include <Interpreters/InstrumentationManager.h>

#if USE_XRAY

#include <filesystem>
#include <print>
#include <thread>
#include <unistd.h>
#include <variant>
#include <iterator>
#include <latch>

#include <base/getThreadId.h>
#include <Interpreters/InstrumentationTraceLog.h>
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
extern const int CANNOT_READ_ALL_DATA;
extern const int LOGICAL_ERROR;
}

static constexpr String SLEEP_HANDLER = "sleep";
static constexpr String LOG_HANDLER = "log";
static constexpr String PROFILE_HANDLER = "profile";

static constexpr String UNKNOWN = "<unknown>";

auto logger = getLogger("InstrumentationManager");

void InstrumentationManager::registerHandler(const String & name, XRayHandlerFunction handler)
{
    handler_name_to_function.emplace_back(std::make_pair(name, handler));
}

InstrumentationManager::InstrumentationManager()
{
    /// The order in which handlers are registered is important because they will be executed in that same order.
    registerHandler(LOG_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { log(entry_type, ip); });
    registerHandler(PROFILE_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { profile(entry_type, ip); });
    registerHandler(SLEEP_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { sleep(entry_type, ip); });
}

InstrumentationManager & InstrumentationManager::instance()
{
    static InstrumentationManager instance;
    return instance;
}


void InstrumentationManager::patchFunctionIfNeeded(Int32 function_id)
{
    if (instrumented_functions.contains(function_id))
    {
        instrumented_functions[function_id]++;
    }
    else
    {
        instrumented_functions.emplace(std::make_pair(function_id, 1));
        __xray_patch_function(function_id);
    }
}

void InstrumentationManager::unpatchFunctionIfNeeded(Int32 function_id)
{
    if (!instrumented_functions.contains(function_id))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function id {} to unpatch not previously patched", function_id);

    if (--instrumented_functions[function_id] <= 0)
    {
        __xray_unpatch_function(function_id);
        instrumented_functions.erase(function_id);
    }
}

void InstrumentationManager::patchFunction(ContextPtr context, const String & function_name, const String & handler_name, std::optional<XRayEntryType> entry_type, std::optional<std::vector<InstrumentedParameter>> & parameters)
{
    auto handler_name_lower = Poco::toLower(handler_name);

    bool found = false;
    for (const auto & [name, _] : handler_name_to_function)
    {
        if (handler_name_lower == name)
        {
            found = true;
            break;
        }
    }

    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown XRay handler: ({})", handler_name);

    /// Lazy load the XRay instrumentation map only once we need to set up a handler
    if (initialization_status != InitializationStatus::INITIALIZED)
    {
        auto expected_status = InitializationStatus::UNINITIALIZED;
        if (initialization_status.compare_exchange_strong(expected_status, InitializationStatus::INITIALIZING))
        {
            LOG_DEBUG(logger, "Initializing InstrumentationManager by reading the instrumentation map");
            parseInstrumentationMap();
            __xray_set_handler(&InstrumentationManager::dispatchHandler);
            initialization_status = InitializationStatus::INITIALIZED;
            LOG_DEBUG(logger, "InstrumentationManager initialized in this thread. Setting up handler");
            initialization_status.notify_all();
        }
        else
        {
            LOG_DEBUG(logger, "InstrumentationManager is initializing. Waiting for it");
            initialization_status.wait(InitializationStatus::INITIALIZING);
            LOG_DEBUG(logger, "InstrumentationManager initialized by some other query. Setting up handler");
        }
    }

    Int32 function_id;
    auto fn_it = functions_container.get<FunctionName>().find(function_name);
    if (fn_it != functions_container.get<FunctionName>().end())
        function_id = fn_it->function_id;
    else
    {
        auto stripped_it = functions_container.get<StrippedFunctionName>().find(function_name);
        if (stripped_it != functions_container.get<StrippedFunctionName>().end())
            function_id = stripped_it->function_id;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown function to instrument: '{}'. XRay instruments by default only functions of at least 200 instructions. "
                "You can change that threshold with '-fxray-instruction-threshold=1'. You can also force the instrumentation of specific functions decorating them with '[[clang::xray_always_instrument]]' "
                "and making sure they are not decorated with '[[clang::xray_never_instrument]]'", function_name);
    }

    std::lock_guard lock(shared_mutex);
    auto it = instrumented_points.find(InstrumentedPointKey{function_id, entry_type, handler_name_lower});
    if (it != instrumented_points.end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler of this type is already installed for function_id '{}', function name '{}'",
            function_id, function_name);
    }

    patchFunctionIfNeeded(function_id);

    InstrumentedPointInfo info{context, instrumentation_point_ids, function_id, function_name, handler_name_lower, entry_type, parameters};
    LOG_DEBUG(logger, "Adding instrumentation point for {}", info.toString());
    instrumented_points.emplace(std::make_pair(
        InstrumentedPointKey{function_id, entry_type, handler_name_lower},
        std::move(info)));
    instrumentation_point_ids++;
}

void InstrumentationManager::unpatchFunction(std::variant<UInt64, bool> id)
{
    std::lock_guard lock(shared_mutex);

    if (std::holds_alternative<bool>(id))
    {
        LOG_DEBUG(logger, "Removing all instrumented functions");
        for (const auto & [function_id, info] : instrumented_points)
            unpatchFunctionIfNeeded(info.function_id);
        instrumented_points.clear();
    }
    else
    {
        std::optional<InstrumentedPointKey> function_key;
        InstrumentedPointInfo instrumented_info;
        for (const auto & [key, info] : instrumented_points)
        {
            if (info.id == std::get<UInt64>(id))
            {
                function_key = key;
                instrumented_info = info;
                break;
            }
        }

        if (!function_key.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown instrumentation point id to remove: ({})", std::get<UInt64>(id));

        LOG_DEBUG(logger, "Removing instrumented function {}", instrumented_info.toString());
        unpatchFunctionIfNeeded(function_key->function_id);
        instrumented_points.erase(function_key.value());
    }
}

InstrumentationManager::InstrumentedPoints InstrumentationManager::getInstrumentedPoints()
{
    SharedLockGuard lock(shared_mutex);
    InstrumentedPoints points;
    points.reserve(instrumented_points.size());

    for (const auto & [key, info] : instrumented_points)
        points.emplace_back(info);

    return points;
}

void InstrumentationManager::dispatchHandler(Int32 func_id, XRayEntryType entry_type)
{
    InstrumentationManager::instance().dispatchHandlerImpl(func_id, entry_type);
}

void InstrumentationManager::dispatchHandlerImpl(Int32 func_id, XRayEntryType entry_type)
{
    /// We don't need to distinguish between a normal EXIT and a TAIL EXIT, so we convert
    /// the latter to the former to simplify the rest of the logic.
    if (entry_type == XRayEntryType::TAIL)
        entry_type = XRayEntryType::EXIT;

    for (const auto & [handler_name, handler_function] : handler_name_to_function)
    {
        SharedLockGuard lock(shared_mutex);
        auto ip = instrumented_points.find(InstrumentedPointKey{func_id, entry_type, handler_name});

        /// If the key couldn't be found for entry/exit type, let's try without any entry type for those handlers that don't need it.
        if (ip == instrumented_points.end())
            ip = instrumented_points.find(InstrumentedPointKey{func_id, std::nullopt, handler_name});

        if (ip != instrumented_points.end())
        {
            const auto info = ip->second;
            lock.unlock();
            try
            {
                handler_function(entry_type, info);
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(logger, "Exception in handler '{}': {}", info.handler_name, e.what());
            }
        }
    }
}

/// Takes path to the elf-binary file(that should contain xray_instr_map section),
/// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
void InstrumentationManager::parseInstrumentationMap()
{
    auto binary_path = std::filesystem::canonical(std::filesystem::path("/proc/self/exe")).string();

    /// Load the XRay instrumentation map from the binary
    auto instr_map_or_error = loadInstrumentationMap(binary_path);
    if (!instr_map_or_error)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to load instrumentation map: {}", toString(instr_map_or_error.takeError()));

    auto & instr_map = *instr_map_or_error;

    /// Retrieve the mapping of function IDs to addresses
    auto function_addresses = instr_map.getFunctionAddresses();

    functions_container.reserve(function_addresses.size());

    const auto & context = CurrentThread::getQueryContext();
    const auto num_jobs = std::thread::hardware_concurrency();

    LOG_DEBUG(logger, "Starting to parse the XRay instrumentation map using {} jobs. This takes a few seconds...", num_jobs);

    struct Job
    {
        std::pair<UInt64, UInt64> range;
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
        for (UInt64 i = job.range.first; i < job.range.second; ++i)
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

void InstrumentationManager::sleep([[maybe_unused]] XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    const auto & params_opt = instrumented_point.parameters;
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
        LOG_TRACE(logger, "Sleeping for {}s", seconds);
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
        LOG_TRACE(logger, "Sleeping for {}ms", duration.count());
        std::this_thread::sleep_for(duration);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected numeric parameter (Int64 or Float64) for sleep, but got something else");
    }
}

void InstrumentationManager::log(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    const auto & params_opt = instrumented_point.parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for log instrumentation");

    const auto & params = params_opt.value();

    if (params.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected exactly one parameter for instrumentation, but got {}", params.size());

    const auto & param = params[0];

    if (std::holds_alternative<String>(param))
    {
        String logger_info = std::get<String>(param);
        LOG_INFO(logger, "{} ({}): {}\nStack trace:\n{}",
            instrumented_point.function_name, entry_type == XRayEntryType::ENTRY ? "entry" : "exit", logger_info, StackTrace().toString());
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String for log, but got something else");
    }
}

void InstrumentationManager::profile(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    static thread_local std::unordered_map<Int32, InstrumentationTraceLogElement> active_elements;

    LOG_TRACE(logger, "Profile: function with id {}", instrumented_point.function_id);
    if (entry_type == XRayEntryType::ENTRY)
    {
        InstrumentationTraceLogElement element;
        element.function_name = functions_container.get<FunctionId>().find(instrumented_point.function_id)->stripped_function_name;
        element.tid = getThreadId();
        using namespace std::chrono;

        auto now = system_clock::now();
        auto now_us = duration_cast<microseconds>(now.time_since_epoch()).count();

        element.event_time = time_t(duration_cast<seconds>(now.time_since_epoch()).count());
        element.event_time_microseconds = Decimal64(now_us);

        element.query_id = CurrentThread::isInitialized() ? CurrentThread::getQueryId() : "";
        element.function_id = instrumented_point.function_id;

        active_elements[instrumented_point.function_id] = std::move(element);
    }
    else if (entry_type == XRayEntryType::EXIT)
    {
        auto it = active_elements.find(instrumented_point.function_id);
        if (it != active_elements.end())
        {
            auto & element = it->second;
            auto now = std::chrono::system_clock::now();
            auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

            auto start_us = Int64(element.event_time_microseconds);
            element.duration_microseconds = Decimal64(now_us - start_us);

            if (instrumented_point.context)
            {
                if (auto log = instrumented_point.context->getInstrumentationTraceLog())
                    log->add(std::move(element));
            }
            active_elements.erase(it);
        }
    }
}

std::string_view InstrumentationManager::removeTemplateArgs(std::string_view input)
{
    std::string_view result = input;
    size_t pos = result.find('<');
    if (pos == std::string_view::npos)
        return result;

    return result.substr(0, pos);
}

String InstrumentationManager::extractNearestNamespaceAndFunction(std::string_view signature)
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
