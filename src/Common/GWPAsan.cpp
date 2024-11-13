#include <Common/GWPAsan.h>

#if USE_GWP_ASAN
#    include <IO/ReadHelpers.h>
#    include <gwp_asan/common.h>
#    include <gwp_asan/crash_handler.h>
#    include <gwp_asan/guarded_pool_allocator.h>
#    include <gwp_asan/optional/options_parser.h>
#    include <Common/ErrorCodes.h>
#    include <Common/Exception.h>
#    include <Common/Logger.h>
#    include <Common/StackTrace.h>
#    include <Common/logger_useful.h>

#    include <atomic>
#    include <iostream>

namespace GWPAsan
{

namespace
{
size_t getBackTrace(uintptr_t * trace_buffer, size_t buffer_size)
{
    StackTrace stacktrace;
    auto trace_size = std::min(buffer_size, stacktrace.getSize());
    const auto & frame_pointers = stacktrace.getFramePointers();
    memcpy(trace_buffer, frame_pointers.data(), trace_size * sizeof(uintptr_t));
    return trace_size;
}

__attribute__((__format__ (__printf__, 1, 0)))
void printString(const char * format, ...) // NOLINT(cert-dcl50-cpp)
{
    std::array<char, 1024> formatted;
    va_list args;
    va_start(args, format);

    if (vsnprintf(formatted.data(), formatted.size(), format, args) > 0)
        std::cerr << formatted.data() << std::endl;

    va_end(args);
}

}

gwp_asan::GuardedPoolAllocator GuardedAlloc;

static bool guarded_alloc_initialized = []
{
    const char * env_options_raw = std::getenv("GWP_ASAN_OPTIONS"); // NOLINT(concurrency-mt-unsafe)
    if (env_options_raw)
        gwp_asan::options::initOptions(env_options_raw, printString);

    auto & opts = gwp_asan::options::getOptions();
    if (!env_options_raw || !std::string_view{env_options_raw}.contains("MaxSimultaneousAllocations"))
        opts.MaxSimultaneousAllocations = 1024;

    if (!env_options_raw || !std::string_view{env_options_raw}.contains("SampleRate"))
        opts.SampleRate = 0;

    const char * collect_stacktraces = std::getenv("GWP_ASAN_COLLECT_STACKTRACES"); // NOLINT(concurrency-mt-unsafe)
    if (collect_stacktraces && std::string_view{collect_stacktraces} == "1")
        opts.Backtrace = getBackTrace;

    GuardedAlloc.init(opts);

    return true;
}();

bool isGWPAsanError(uintptr_t fault_address)
{
    const auto * state = GuardedAlloc.getAllocatorState();
    if (state->FailureType != gwp_asan::Error::UNKNOWN && state->FailureAddress != 0)
        return true;

    return fault_address < state->GuardedPagePoolEnd && state->GuardedPagePool <= fault_address;
}

namespace
{

struct ScopedEndOfReportDecorator
{
    explicit ScopedEndOfReportDecorator(Poco::LoggerPtr log_) : log(std::move(log_)) { }
    ~ScopedEndOfReportDecorator() { LOG_FATAL(log, "*** End GWP-ASan report ***"); }
    Poco::LoggerPtr log;
};

// Prints the provided error and metadata information.
void printHeader(gwp_asan::Error error, uintptr_t fault_address, const gwp_asan::AllocationMetadata * allocation_meta, Poco::LoggerPtr log)
{
    bool access_was_in_bounds = false;
    std::string description;
    if (error != gwp_asan::Error::UNKNOWN && allocation_meta != nullptr)
    {
        uintptr_t address = __gwp_asan_get_allocation_address(allocation_meta);
        size_t size = __gwp_asan_get_allocation_size(allocation_meta);
        if (fault_address < address)
        {
            description = fmt::format(
                "({} byte{} to the left of a {}-byte allocation at 0x{}) ",
                address - fault_address,
                (address - fault_address == 1) ? "" : "s",
                size,
                address);
        }
        else if (fault_address > address)
        {
            description = fmt::format(
                "({} byte{} to the right of a {}-byte allocation at 0x{}) ",
                fault_address - address,
                (fault_address - address == 1) ? "" : "s",
                size,
                address);
        }
        else if (error == gwp_asan::Error::DOUBLE_FREE)
        {
            description = fmt::format("(a {}-byte allocation) ", size);
        }
        else
        {
            access_was_in_bounds = true;
            description = fmt::format(
                "({} byte{} into a {}-byte allocation at 0x{}) ",
                fault_address - address,
                (fault_address - address == 1) ? "" : "s",
                size,
                address);
        }
    }

    uint64_t thread_id = gwp_asan::getThreadID();
    std::string thread_id_string = thread_id == gwp_asan::kInvalidThreadID ? "<unknown" : fmt::format("{}", thread_id);

    std::string_view out_of_bounds_and_use_after_free_warning;
    if (error == gwp_asan::Error::USE_AFTER_FREE && !access_was_in_bounds)
    {
        out_of_bounds_and_use_after_free_warning = " (warning: buffer overflow/underflow detected on a free()'d "
                                                   "allocation. This either means you have a buffer-overflow and a "
                                                   "use-after-free at the same time, or you have a long-lived "
                                                   "use-after-free bug where the allocation/deallocation metadata below "
                                                   "has already been overwritten and is likely bogus)";
    }

    LOG_FATAL(
     log,
        "{}{} at 0x{} {}by thread {} here:",
        gwp_asan::ErrorToString(error),
        out_of_bounds_and_use_after_free_warning,
        fault_address,
        description,
        thread_id_string);
}

}

void printReport([[maybe_unused]] uintptr_t fault_address)
{
    const auto logger = getLogger("GWPAsan");
    const auto * state = GuardedAlloc.getAllocatorState();
    if (uintptr_t internal_error_ptr = __gwp_asan_get_internal_crash_address(state); internal_error_ptr)
        fault_address = internal_error_ptr;

    const gwp_asan::AllocationMetadata * allocation_meta = __gwp_asan_get_metadata(state, GuardedAlloc.getMetadataRegion(), fault_address);

    static constexpr std::string_view unknown_crash_text =
        "GWP-ASan cannot provide any more information about this error. This may "
        "occur due to a wild memory access into the GWP-ASan pool, or an "
        "overflow/underflow that is > 512B in length.\n";

    if (allocation_meta == nullptr)
    {
        LOG_FATAL(logger, "*** GWP-ASan detected a memory error ***");
        ScopedEndOfReportDecorator decorator(logger);
        LOG_FATAL(logger, fmt::runtime(unknown_crash_text));
        return;
    }

    LOG_FATAL(logger, "*** GWP-ASan detected a memory error ***");
    ScopedEndOfReportDecorator decorator(logger);

    gwp_asan::Error error = __gwp_asan_diagnose_error(state, allocation_meta, fault_address);
    if (error == gwp_asan::Error::UNKNOWN)
    {
        LOG_FATAL(logger, fmt::runtime(unknown_crash_text));
        return;
    }

    // Print the error header.
    printHeader(error, fault_address, allocation_meta, logger);

    static constexpr size_t maximum_stack_frames = 512;
    std::array<uintptr_t, maximum_stack_frames> trace;

    // Maybe print the deallocation trace.
    if (__gwp_asan_is_deallocated(allocation_meta))
    {
        uint64_t thread_id = __gwp_asan_get_deallocation_thread_id(allocation_meta);
        if (thread_id == gwp_asan::kInvalidThreadID)
            LOG_FATAL(logger, "0x{} was deallocated by thread <unknown> here:", fault_address);
        else
            LOG_FATAL(logger, "0x{} was deallocated by thread {} here:", fault_address, thread_id);
        const auto trace_length = __gwp_asan_get_deallocation_trace(allocation_meta, trace.data(), maximum_stack_frames);
        StackTrace::toStringEveryLine(
            reinterpret_cast<void **>(trace.data()), 0, trace_length, [&](const auto line) { LOG_FATAL(logger, fmt::runtime(line)); });
    }

    // Print the allocation trace.
    uint64_t thread_id = __gwp_asan_get_allocation_thread_id(allocation_meta);
    if (thread_id == gwp_asan::kInvalidThreadID)
        LOG_FATAL(logger, "0x{} was allocated by thread <unknown> here:", fault_address);
    else
        LOG_FATAL(logger, "0x{} was allocated by thread {} here:", fault_address, thread_id);
    const auto trace_length = __gwp_asan_get_allocation_trace(allocation_meta, trace.data(), maximum_stack_frames);
    StackTrace::toStringEveryLine(
        reinterpret_cast<void **>(trace.data()), 0, trace_length, [&](const auto line) { LOG_FATAL(logger, fmt::runtime(line)); });
}

std::atomic<bool> init_finished = false;

void initFinished()
{
    init_finished.store(true, std::memory_order_relaxed);
}

std::atomic<double> force_sample_probability = 0.0;

void setForceSampleProbability(double value)
{
    force_sample_probability.store(value, std::memory_order_relaxed);
}

}

#endif
