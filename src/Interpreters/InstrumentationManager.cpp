#include <Interpreters/InstrumentationManager.h>

#if USE_XRAY

#include <filesystem>
#include <print>
#include <thread>
#include <unistd.h>
#include <variant>

#include <base/getThreadId.h>
#include <Interpreters/InstrumentationTraceLog.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Common/SharedLockGuard.h>
#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>
#include <Common/ThreadStatus.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>

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

/// The offset of 4 is to remove all the frames added by the instrumentation itself:
/// StackTrace::StackTrace()
/// DB::InstrumentationManager::profile(XRayEntryType, DB::InstrumentationManager::InstrumentedPointInfo const&)
/// DB::InstrumentationManager::dispatchHandlerImpl(int, XRayEntryType)
/// __xray_FunctionEntry
static const auto STACK_OFFSET = 4;

auto logger = getLogger("InstrumentationManager");

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

void InstrumentationManager::registerHandler(const String & name, XRayHandlerFunction handler)
{
    handler_name_to_function.emplace_back(std::make_pair(name, handler));
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

    Int32 function_id;
    String symbol;
    auto fn_it = functions_container.get<FunctionName>().find(function_name);
    if (fn_it != functions_container.get<FunctionName>().end())
    {
        function_id = fn_it->function_id;
        symbol = fn_it->function_name;
    }
    else
    {
        auto stripped_it = functions_container.get<StrippedFunctionName>().find(function_name);
        if (stripped_it != functions_container.get<StrippedFunctionName>().end())
        {
            function_id = stripped_it->function_id;
            symbol = stripped_it->function_name;
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown function to instrument: '{}'. XRay instruments by default only functions of at least 200 instructions. "
                "You can change that threshold with '-fxray-instruction-threshold=1'. You can also force the instrumentation of specific functions decorating them with '[[clang::xray_always_instrument]]' "
                "and making sure they are not decorated with '[[clang::xray_never_instrument]]'", function_name);
    }

    std::lock_guard lock(shared_mutex);
    auto it = instrumented_points.get<InstrumentedPointKey>().find(InstrumentedPointKey{function_id, entry_type, handler_name_lower});
    if (it != instrumented_points.get<InstrumentedPointKey>().end())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler of this type is already installed for function_id '{}', function name '{}'",
            function_id, function_name);
    }

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
        std::optional<InstrumentedPointKey> function_key;
        InstrumentedPointInfo instrumented_info;
        for (const auto & info : instrumented_points)
        {
            if (info.id == std::get<UInt64>(id))
            {
                function_key = InstrumentedPointKeyExtractor()(info);
                instrumented_info = info;
                break;
            }
        }

        if (!function_key.has_value())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown instrumentation point id to remove: ({})", std::get<UInt64>(id));

        LOG_DEBUG(logger, "Removing instrumented function {}", instrumented_info.toString());
        unpatchFunctionIfNeeded(function_key->function_id);
        instrumented_points.get<InstrumentedPointKey>().erase(function_key.value());
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
        auto ip_info = instrumented_points.get<InstrumentedPointKey>().find(InstrumentedPointKey{func_id, entry_type, handler_name});

        /// If the key couldn't be found for entry/exit type, let's try without any entry type for those handlers that don't need it.
        if (ip_info == instrumented_points.get<InstrumentedPointKey>().end())
            ip_info = instrumented_points.get<InstrumentedPointKey>().find(InstrumentedPointKey{func_id, std::nullopt, handler_name});

        if (ip_info != instrumented_points.get<InstrumentedPointKey>().end())
        {
            auto info = *ip_info;
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
            auto stripped_function_name = extractNearestNamespaceAndFunction(symbol_demangled);
            functions_container.emplace(func_id, symbol_demangled, stripped_function_name);
        }
        else
        {
            errors++;
        }
    }

    LOG_DEBUG(logger, "Finished parsing the XRay instrumentation map: {} symbols parsed successfully, {} with errors", functions_container.size(), errors);
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
        LOG_TRACE(logger, "Sleep ({}, function_id {}): sleeping for {}s", instrumented_point.function_name, instrumented_point.function_id, seconds);
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
        StackTrace stack_trace;
        String stack_trace_str = StackTrace::toString(stack_trace.getFramePointers().data(), stack_trace.getOffset() + STACK_OFFSET, stack_trace.getSize() - STACK_OFFSET);

        LOG_INFO(logger, "Log ({}, function_id {}, {}): {}\nStack trace:\n{}",
            instrumented_point.function_name, instrumented_point.function_id,
            entry_type == XRayEntryType::ENTRY ? "entry" : "exit", logger_info, stack_trace_str);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String for log, but got something else");
    }
}

void InstrumentationManager::profile(XRayEntryType entry_type, const InstrumentedPointInfo & instrumented_point)
{
    /// This is the easiest way to do store the elements, because otherwise we'd need to have a mutex to protect a
    /// shared std::unordered_map. However, there might be a race condition in which this handler is already triggered
    /// on entry and the function is unpatched immediately afterwards. That's fine, since we're using the instrumented
    /// point ID to know for sure whether this execution is tight to the stored element or not.
    /// We also remove the element once we realize it's from a different generation, but it's not cleared until then.
    static thread_local std::unordered_map<Int32, InstrumentationTraceLogElement> active_elements;

    using namespace std::chrono;
    auto now = system_clock::now();

    LOG_TRACE(logger, "Profile ({}, function_id {})", instrumented_point.function_name, instrumented_point.function_id);

    if (entry_type == XRayEntryType::ENTRY)
    {
        InstrumentationTraceLogElement element;
        element.instrumented_point_id = instrumented_point.id;
        element.function_name = functions_container.get<FunctionId>().find(instrumented_point.function_id)->stripped_function_name;
        element.tid = getThreadId();
        element.query_id = CurrentThread::isInitialized() ? CurrentThread::getQueryId() : "";
        element.function_id = instrumented_point.function_id;
        const auto stack_trace = StackTrace();
        const auto frame_pointers = stack_trace.getFramePointers();

        element.trace.reserve(stack_trace.getSize() - STACK_OFFSET);

#if defined(__ELF__) && !defined(OS_FREEBSD)
        const auto * object = SymbolIndex::instance().thisObject();
#endif

        for (size_t i = stack_trace.getOffset() + STACK_OFFSET; i < stack_trace.getSize(); ++i)
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

        now = system_clock::now();
        auto now_us = duration_cast<microseconds>(now.time_since_epoch()).count();
        element.event_time = time_t(duration_cast<seconds>(now.time_since_epoch()).count());
        element.event_time_microseconds = Decimal64(now_us);

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
