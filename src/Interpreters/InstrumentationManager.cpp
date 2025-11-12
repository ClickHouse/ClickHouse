#include <Interpreters/InstrumentationManager.h>

#if USE_XRAY

#include <chrono>
#include <filesystem>
#include <print>
#include <thread>
#include <sched.h>
#include <unistd.h>
#include <variant>

#include <base/getThreadId.h>
#include <base/scope_guard.h>
#include <Interpreters/TraceLog.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>
#include <Common/SharedLockGuard.h>
#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadStatus.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>
#include <pcg_random.hpp>

#include <xray/xray_interface.h>
#include <llvm/XRay/InstrumentationMap.h>

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

auto logger = getLogger("InstrumentationManager");

InstrumentationManager::InstrumentationManager()
{
    registerHandler(LOG_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { log(entry_type, ip); });
    registerHandler(PROFILE_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { profile(entry_type, ip); });
    registerHandler(SLEEP_HANDLER, [this](XRayEntryType entry_type, const InstrumentedPointInfo & ip) { sleep(entry_type, ip); });
}

InstrumentationManager & InstrumentationManager::instance()
{
    static InstrumentationManager instance;
    return instance;
}

void InstrumentationManager::registerHandler(const String & name, XRayHandlerFunction handler)
{
    handler_name_to_function.emplace(name, handler);
}

void InstrumentationManager::ensureInitialization()
{
    callOnce(initialized, [this]()
    {
        parseInstrumentationMap();
        __xray_set_handler(&InstrumentationManager::dispatchHandler);
    });
}

void InstrumentationManager::patchFunctionIfNeeded(Int32 function_id)
{
    if (instrumented_points.get<FunctionId>().contains(function_id))
        return;
    __xray_patch_function(function_id);
}

void InstrumentationManager::unpatchFunctionIfNeeded(Int32 function_id)
{
    auto it = instrumented_points.get<FunctionId>().find(function_id);
    const auto end_it = instrumented_points.get<FunctionId>().end();
    if (it == end_it)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function id {} to unpatch not previously patched", function_id);

    size_t count = 0;
    while (it != end_it)
    {
        count++;
        it++;
    }

    if (count <= 1)
        __xray_unpatch_function(function_id);
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
    ensureInitialization();

    Int32 function_id = -1;
    String symbol;
    auto fn_it = functions_container.get<FunctionName>().find(function_name);

    /// First, assume the name provided is the full qualified name.
    if (fn_it != functions_container.get<FunctionName>().end())
    {
        function_id = fn_it->function_id;
        symbol = fn_it->function_name;
    }
    else
    {
        /// Otherwise, search if the provided function_name can be found as a substr of every member.
        for (const auto & [id, function] : functions_container)
        {
            if (function.find(function_name) != std::string::npos)
            {
                function_id = id;
                symbol = function;
                break;
            }
        }
    }

    if (function_id < 0 || symbol.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown function to instrument: '{}'. XRay instruments by default only functions of at least 200 instructions. "
            "You can change that threshold with '-fxray-instruction-threshold=1'. You can also force the instrumentation of specific functions decorating them with '[[clang::xray_always_instrument]]' "
            "and making sure they are not decorated with '[[clang::xray_never_instrument]]'", function_name);

    std::lock_guard lock(shared_mutex);
    patchFunctionIfNeeded(function_id);

    InstrumentedPointInfo info{context, instrumented_point_ids, function_id, function_name, handler_name_lower, entry_type, symbol, parameters};
    LOG_DEBUG(logger, "Adding instrumentation point for {}", info.toString());
    instrumented_points.emplace(std::move(info));
    instrumented_point_ids++;
}

void InstrumentationManager::unpatchFunction(std::variant<UInt64, bool> id)
{
    std::lock_guard lock(shared_mutex);

    if (std::holds_alternative<bool>(id))
    {
        LOG_DEBUG(logger, "Removing all instrumented functions");
        for (const auto & info : instrumented_points)
            unpatchFunctionIfNeeded(info.function_id);
        instrumented_points.clear();
    }
    else
    {
        const auto it = instrumented_points.get<Id>().find(std::get<UInt64>(id));
        if (it == instrumented_points.get<Id>().end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown instrumentation point id to remove: ({})", std::get<UInt64>(id));

        LOG_DEBUG(logger, "Removing instrumented function {}", it->toString());
        unpatchFunctionIfNeeded(it->function_id);
        instrumented_points.erase(it);
    }
}

InstrumentationManager::InstrumentedPoints InstrumentationManager::getInstrumentedPoints() const
{
    SharedLockGuard lock(shared_mutex);
    InstrumentedPoints points;
    points.reserve(instrumented_points.size());

    for (const auto & info : instrumented_points)
        points.emplace_back(info);

    return points;
}

const InstrumentationManager::FunctionsContainer & InstrumentationManager::getFunctions()
{
    ensureInitialization();
    return functions_container;
}

void InstrumentationManager::dispatchHandler(Int32 func_id, XRayEntryType entry_type)
{
    static thread_local bool dispatching = false;
    /// Prevent reentrancy.
    if (dispatching)
        return;

    dispatching = true;
    SCOPE_EXIT(dispatching = false);
    InstrumentationManager::instance().dispatchHandlerImpl(func_id, entry_type);
}

void InstrumentationManager::dispatchHandlerImpl(Int32 func_id, XRayEntryType entry_type)
{
    /// We don't need to distinguish between a normal EXIT and a TAIL EXIT, so we convert
    /// the latter to the former to simplify the rest of the logic.
    if (entry_type == XRayEntryType::TAIL)
        entry_type = XRayEntryType::EXIT;

    std::vector<InstrumentedPointInfo> func_ips;
    SharedLockGuard lock(shared_mutex);
    for (auto it = instrumented_points.get<FunctionId>().find(func_id); it != instrumented_points.get<FunctionId>().end(); ++it)
        func_ips.emplace_back(*it);
    lock.unlock();

    for (const auto & info : func_ips)
    {
        if (info.entry_type.has_value() && info.entry_type.value() != entry_type)
            continue;

        try
        {
            auto handler_function = handler_name_to_function.find(info.handler_name);
            if (handler_function == handler_name_to_function.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Handler {} not found when trying to run instrumentation point {}", info.handler_name, info.toString());
            handler_function->second(entry_type, info);
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(logger, "Exception in handler '{}': {}", info.handler_name, e.what());
        }
    }
}

/// Takes path to the elf-binary file(that should contain xray_instr_map section),
/// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
void InstrumentationManager::parseInstrumentationMap()
{
    auto binary_path = std::filesystem::canonical(std::filesystem::path("/proc/self/exe")).string();

    auto instr_map_or_error = llvm::xray::loadInstrumentationMap(binary_path);
    if (!instr_map_or_error)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to load instrumentation map: {}", toString(instr_map_or_error.takeError()));

    auto & instr_map = *instr_map_or_error;

    const auto function_addresses = instr_map.getFunctionAddresses();
    const auto & context = CurrentThread::getQueryContext();

    LOG_DEBUG(logger, "Starting to parse the XRay instrumentation map. This takes a few seconds...");

    functions_container.reserve(function_addresses.size());
    const SymbolIndex & symbol_index = SymbolIndex::instance();
    size_t errors = 0;

    for (const auto & [func_id, addr] : function_addresses)
    {
        const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(addr));
        if (symbol)
        {
            const auto symbol_demangled = demangle(symbol->name);
            functions_container.emplace(func_id, symbol_demangled);
        }
        else
        {
            errors++;
        }
    }

    LOG_DEBUG(logger, "Finished parsing the XRay instrumentation map: {} symbols parsed successfully, {} with errors", functions_container.size(), errors);
}

TraceLogElement InstrumentationManager::createTraceLogElement(const InstrumentedPointInfo & instrumented_point, XRayEntryType entry_type, std::chrono::system_clock::time_point event_time) const
{
    using namespace std::chrono;

    TraceLogElement element;

    auto event_time_us = duration_cast<microseconds>(event_time.time_since_epoch()).count();
    element.event_time = time_t(duration_cast<seconds>(event_time.time_since_epoch()).count());
    element.event_time_microseconds = Decimal64(event_time_us);
    element.timestamp_ns = duration_cast<nanoseconds>(event_time.time_since_epoch()).count();
    element.instrumented_point_id = instrumented_point.id;
    element.trace_type = TraceType::Instrumentation;
    element.cpu_id = sched_getcpu();
    element.thread_id = getThreadId();
    element.query_id = CurrentThread::isInitialized() ? CurrentThread::getQueryId() : "";
    element.function_id = instrumented_point.function_id;
    element.function_name = instrumented_point.symbol;
    element.handler = instrumented_point.handler_name;
    element.entry_type = entry_type;
    element.symbolize = true;

    const auto stack_trace = StackTrace();
    const auto frame_pointers = stack_trace.getFramePointers();

    element.trace.reserve(stack_trace.getSize() - stack_trace.getOffset());

#if defined(__ELF__) && !defined(OS_FREEBSD)
    const auto * object = SymbolIndex::instance().thisObject();
#endif

    for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
    {
        /// Addresses in the main object will be normalized to the physical file offsets for convenience and security.
        uintptr_t offset = 0;
        const uintptr_t addr = reinterpret_cast<uintptr_t>(frame_pointers[i]);
#if defined(__ELF__) && !defined(OS_FREEBSD)
        if (object && uintptr_t(object->address_begin) <= addr && addr < uintptr_t(object->address_end))
            offset = uintptr_t(object->address_begin);
#endif
        element.trace.emplace_back(addr - offset);
    }

    return element;
}

void InstrumentationManager::sleep([[maybe_unused]] XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    using namespace std::chrono;

    static thread_local pcg64_fast random_generator{randomSeed()};

    const auto & params_opt = instrumented_point.parameters;
    if (!params_opt.has_value())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing parameters for sleep instrumentation");

    const auto & params = params_opt.value();

    auto get_value = [](auto param)
    {
        if (std::holds_alternative<Int64>(param))
            return static_cast<Float64>(std::get<Int64>(param));
        else if (std::holds_alternative<Float64>(param))
            return std::get<Float64>(param);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected numeric parameter (Int64 or Float64) for sleep, but got something else");
    };

    Int64 duration_ms = -1;

    if (params.size() == 1)
    {
        duration_ms = static_cast<Int64>(1000 * get_value(params[0]));
    }
    else
    {
        auto min = get_value(params[0]);
        auto max = get_value(params[1]);

        std::uniform_real_distribution<> distrib(min, max);
        duration_ms = static_cast<Int64>(1000 * distrib(random_generator));
    }

    if (duration_ms < 0)
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Sleep duration must be non-negative");

    LOG_TRACE(logger, "Sleep ({}, function_id {}): sleeping for {} ms", instrumented_point.function_name, instrumented_point.function_id, duration_ms);
    auto now = std::chrono::system_clock::now();
    std::this_thread::sleep_for(duration<Float64, std::milli>(duration_ms));
    auto element = createTraceLogElement(instrumented_point, entry_type, now);
    if (instrumented_point.context)
    {
        if (auto log = instrumented_point.context->getTraceLog())
            log->add(std::move(element));
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
        StackTrace stack_trace;
        String stack_trace_str = StackTrace::toString(stack_trace.getFramePointers().data(), stack_trace.getOffset(), stack_trace.getSize() - stack_trace.getOffset());

        LOG_INFO(logger, "Log ({}, function_id {}, {}): {}\nStack trace:\n{}",
            instrumented_point.function_name, instrumented_point.function_id,
            entry_type == XRayEntryType::ENTRY ? "Entry" : "Exit", logger_info, stack_trace_str);

        auto element = createTraceLogElement(instrumented_point, entry_type, std::chrono::system_clock::now());
        if (instrumented_point.context)
        {
            if (auto log = instrumented_point.context->getTraceLog())
                log->add(std::move(element));
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String for log, but got something else");
    }
}

void InstrumentationManager::profile(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    using namespace std::chrono;

    /// This is the easiest way to do store the elements, because otherwise we'd need to have a mutex to protect a
    /// shared std::unordered_map. However, there might be a race condition in which this handler is already triggered
    /// on entry and the function is unpatched immediately afterwards. That's fine, since we're using the instrumented
    /// point ID to know for sure whether this execution is tight to the stored element or not.
    /// We also remove the element once we realize it's from a different generation, but it's not cleared until then.
    static thread_local std::unordered_map<Int32, TraceLogElement> active_elements;

    auto now = std::chrono::system_clock::now();

    LOG_TRACE(logger, "Profile ({}, function_id {})", instrumented_point.function_name, instrumented_point.function_id);

    if (entry_type == XRayEntryType::ENTRY)
    {
        auto element = createTraceLogElement(instrumented_point, entry_type, std::chrono::system_clock::now());
        if (instrumented_point.context)
        {
            if (auto log = instrumented_point.context->getTraceLog())
                log->add(element);
        }
        active_elements[instrumented_point.function_id] = std::move(element);
    }
    else if (entry_type == XRayEntryType::EXIT)
    {
        auto it = active_elements.find(instrumented_point.function_id);
        if (it != active_elements.end())
        {
            auto & element = it->second;

            if (element.instrumented_point_id != instrumented_point.id)
            {
                LOG_TRACE(logger, "Profile exit called for a different ID than the one set up. "
                    "Current ID is {}, but stored ID is {}. Instrumented point: {}",
                    instrumented_point.id, element.instrumented_point_id, instrumented_point.toString());
                active_elements.erase(it);
                return;
            }

            auto now_us = duration_cast<microseconds>(now.time_since_epoch()).count();
            auto start_us = Int64(element.event_time_microseconds);

            element.event_time = time_t(duration_cast<seconds>(now.time_since_epoch()).count());
            element.event_time_microseconds = Decimal64(now_us);
            element.timestamp_ns = duration_cast<nanoseconds>(now.time_since_epoch()).count();
            element.duration_microseconds = Decimal64(now_us - start_us);
            element.entry_type = XRayEntryType::EXIT;

            if (instrumented_point.context)
            {
                if (auto log = instrumented_point.context->getTraceLog())
                    log->add(std::move(element));
            }
            active_elements.erase(it);
        }
    }
}

}

#endif
