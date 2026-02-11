#include <Common/Jemalloc.h>

#if USE_JEMALLOC

#include <Common/FramePointers.h>
#include <Common/StringUtils.h>
#include <Common/getExecutablePath.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/TraceSender.h>
#include <Common/MemoryTracker.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <ranges>
#include <base/hex.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
    extern const Event JemallocFailedAllocationSampleTracking;
    extern const Event JemallocFailedDeallocationSampleTracking;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Jemalloc
{

void purgeArenas()
{
    Stopwatch watch;
    mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

void checkProfilingEnabled()
{
    bool active = true;
    size_t active_size = sizeof(active);
    mallctl("opt.prof", &active, &active_size, nullptr, 0);

    if (!active)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ClickHouse was started without enabling profiling for jemalloc. To use jemalloc's profiler, following env variable should be "
            "set: MALLOC_CONF=background_thread:true,prof:true");
}

void setProfileActive(bool value)
{
    checkProfilingEnabled();
    bool active = true;
    size_t active_size = sizeof(active);
    mallctl("prof.active", &active, &active_size, nullptr, 0);
    if (active == value)
    {
        LOG_TRACE(getLogger("SystemJemalloc"), "Profiling is already {}", active ? "enabled" : "disabled");
        return;
    }

    setValue("prof.active", value);
    LOG_TRACE(getLogger("SystemJemalloc"), "Profiling is {}", value ? "enabled" : "disabled");
}

std::string_view flushProfile(const std::string & file_prefix)
{
    checkProfilingEnabled();
    char * prefix_buffer;
    size_t prefix_size = sizeof(prefix_buffer);
    int n = mallctl("opt.prof_prefix", &prefix_buffer, &prefix_size, nullptr, 0); // NOLINT
    if (!n && std::string_view(prefix_buffer) != "jeprof")
    {
        mallctl("prof.dump", nullptr, nullptr, nullptr, 0);
        return getLastFlushProfileForThread();
    }

    static std::atomic<size_t> profile_counter{0};
    std::string profile_dump_path = fmt::format("{}.{}.{}.heap", file_prefix, getpid(), profile_counter.fetch_add(1));
    const auto * profile_dump_path_str = profile_dump_path.c_str();

    mallctl("prof.dump", nullptr, nullptr, &profile_dump_path_str, sizeof(profile_dump_path_str)); // NOLINT
    return getLastFlushProfileForThread();
}

void setBackgroundThreads(bool enabled)
{
    setValue("background_thread", enabled);
}

void setMaxBackgroundThreads(size_t max_threads)
{
    setValue("max_background_threads", max_threads);
}

namespace
{

/// Helper to parse hex address from string_view and advance it
std::optional<UInt64> parseHexAddress(std::string_view & src)
{
    /// Skip "0x" prefix if present
    if (src.size() >= 2 && src[0] == '0' && (src[1] == 'x' || src[1] == 'X'))
        src.remove_prefix(2);

    if (src.empty())
        return std::nullopt;

    UInt64 address = 0;
    size_t processed = 0;

    /// Parse hex digits
    for (size_t i = 0; i < src.size() && processed < 16; ++i)
    {
        char c = src[i];
        if (isHexDigit(c))
        {
            address = (address << 4) | unhex(c);
            ++processed;
        }
        else
            break;
    }

    if (processed == 0)
        return std::nullopt;

    src.remove_prefix(processed);
    return address;
}

/// Collect unique addresses from a heap profile, also returning raw profile data
void collectAddressesFromHeapProfile(
    const std::string & input_filename,
    std::unordered_set<UInt64> & addresses,
    std::string & profile_data)
{
    ReadBufferFromFile in(input_filename);
    std::string line;

    while (!in.eof())
    {
        line.clear();
        readStringUntilNewlineInto(line, in);
        in.tryIgnore(1); /// skip EOL (if not EOF)

        profile_data += line;
        profile_data += '\n';

        if (line.empty())
            continue;

        /// Stack traces start with '@' followed by hex addresses
        if (line[0] == '@')
        {
            std::string_view line_addresses(line.data() + 1, line.size() - 1);

            bool first = true;
            while (!line_addresses.empty())
            {
                trimLeft(line_addresses);
                if (line_addresses.empty())
                    break;

                auto address = parseHexAddress(line_addresses);
                if (!address.has_value())
                    break;

                /// Need to subtract 1 for non first addresses to apply the same fix as in jeprof::FixCallerAddresses()
                addresses.insert(first ? address.value() : address.value() - 1);

                first = false;
            }
        }
    }
}

/// Symbolize a set of addresses into a map: address -> "sym1--sym2--..."
std::unordered_map<UInt64, std::string> symbolizeAddresses(const std::unordered_set<UInt64> & addresses)
{
    std::unordered_map<UInt64, std::string> result;
    result.reserve(addresses.size());

    for (UInt64 address : addresses)
    {
        FramePointers fp;
        fp[0] = reinterpret_cast<void *>(address);

        /// Note, callback calls inlines first, while jeprof needs the opposite
        /// Note, cannot use frame.virtual_addr since it is empty for inlines
        std::vector<std::string> symbols;
        auto symbolize_callback = [&](const StackTrace::Frame & frame)
        {
            symbols.push_back(frame.symbol.value_or("??"));
        };

        /// Note, @fatal (handling inlines) is slow (few milliseconds per address)
        StackTrace::forEachFrame(fp, 0, 1, symbolize_callback, /* fatal= */ true);

        /// Reverse, since forEachFrame() adds inline frames first
        std::string combined;
        std::string_view separator = "";
        for (const auto & symbol : std::ranges::reverse_view(symbols))
        {
            combined += separator;
            combined += symbol;
            separator = "--";
        }
        result[address] = std::move(combined);
    }

    return result;
}

/// Write a symbolized .sym file using a pre-built symbol map
void writeSymbolizedHeapProfile(
    const std::string & output_filename,
    const std::unordered_map<UInt64, std::string> & symbol_map,
    const std::string & profile_data)
{
    WriteBufferFromFile out(output_filename);

    /// Symbolized profile header
    writeString("--- symbol\n", out);
    if (auto binary_path = getExecutablePath(); !binary_path.empty())
    {
        writeString("binary=", out);
        writeString(binary_path, out);
        writeChar('\n', out);
    }

    /// Write symbol table: only addresses that appear in the profile_data
    /// (We write all symbols from the map; callers can share maps across files)
    for (const auto & [address, symbols] : symbol_map)
    {
        writePointerHex(reinterpret_cast<const void *>(address), out);
        writeChar(' ', out);
        writeString(symbols, out);
        writeChar('\n', out);
    }

    writeString("---\n", out);
    writeString("--- heap\n", out);
    writeString(profile_data, out);

    out.finalize();
}

}


void symbolizeHeapProfile(const std::string & input_filename, const std::string & output_filename)
{
    std::unordered_set<UInt64> addresses;
    std::string profile_data;

    collectAddressesFromHeapProfile(input_filename, addresses, profile_data);
    auto symbol_map = symbolizeAddresses(addresses);
    writeSymbolizedHeapProfile(output_filename, symbol_map, profile_data);
}


void symbolizeHeapProfilesBatch(const std::vector<std::pair<std::string, std::string>> & input_output_pairs)
{
    if (input_output_pairs.empty())
        return;

    /// Phase 1: Collect all unique addresses from ALL input files
    std::unordered_set<UInt64> all_addresses;
    std::vector<std::string> all_profile_data(input_output_pairs.size());

    for (size_t i = 0; i < input_output_pairs.size(); ++i)
    {
        collectAddressesFromHeapProfile(input_output_pairs[i].first, all_addresses, all_profile_data[i]);
    }

    /// Phase 2: Symbolize all unique addresses once
    auto symbol_map = symbolizeAddresses(all_addresses);

    /// Phase 3: Write all output files using the shared symbol map
    for (size_t i = 0; i < input_output_pairs.size(); ++i)
    {
        writeSymbolizedHeapProfile(input_output_pairs[i].second, symbol_map, all_profile_data[i]);
    }
}

namespace
{

std::atomic<bool> collect_global_profiles_in_trace_log = false;
thread_local bool collect_local_profiles_in_trace_log = false;

void jemallocAllocationTracker(const void * ptr, size_t /*size*/, void ** backtrace, unsigned backtrace_length, size_t usize)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!collect_local_profiles_in_trace_log && !collect_global_profiles_in_trace_log)
        return;

    try
    {
        FramePointers frame_pointers;
        auto stacktrace_size = std::min<size_t>(backtrace_length, frame_pointers.size());
        memcpy(frame_pointers.data(), backtrace, stacktrace_size * sizeof(void *)); // NOLINT(bugprone-bitwise-pointer-cast)
        TraceSender::send(
            TraceType::JemallocSample,
            StackTrace(std::move(frame_pointers), stacktrace_size),
            TraceSender::Extras{
                .size = static_cast<Int64>(usize),
                .ptr = const_cast<void *>(ptr),
                .memory_blocked_context = MemoryTrackerBlockerInThread::getLevel(),
            });
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::JemallocFailedAllocationSampleTracking);
    }
}

void jemallocDeallocationTracker(const void * ptr, unsigned usize)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    if (!collect_local_profiles_in_trace_log && !collect_global_profiles_in_trace_log)
        return;

    try
    {
        TraceSender::send(
            TraceType::JemallocSample,
            StackTrace(),
            TraceSender::Extras{
                .size = -static_cast<Int64>(usize),
                .ptr = const_cast<void *>(ptr),
                .memory_blocked_context = MemoryTrackerBlockerInThread::getLevel(),
            });
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::JemallocFailedDeallocationSampleTracking);
    }
}

thread_local std::array<char, 256> last_flush_profile_buffer;
thread_local std::string_view last_flush_profile;

void setLastFlushProfile(const char * filename)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto last_flush_profile_size = std::min(last_flush_profile_buffer.size(), strlen(filename));
    std::memcpy(last_flush_profile_buffer.data(), filename, last_flush_profile_size);
    last_flush_profile = std::string_view{last_flush_profile_buffer.data(), last_flush_profile_size};
}

}

void setCollectLocalProfileSamplesInTraceLog(bool value)
{
    collect_local_profiles_in_trace_log = value;
}

void setup(
    bool enable_global_profiler,
    bool enable_background_threads,
    size_t max_background_threads_num,
    bool collect_global_profile_samples_in_trace_log)
{
    if (enable_global_profiler)
    {
        getThreadProfileInitMib().setValue(true);
        getThreadProfileActiveMib().setValue(true);
    }

    setBackgroundThreads(enable_background_threads);

    if (max_background_threads_num)
        setValue("max_background_threads", max_background_threads_num);

    collect_global_profiles_in_trace_log = collect_global_profile_samples_in_trace_log;
    setValue("experimental.hooks.prof_sample", &jemallocAllocationTracker);
    setValue("experimental.hooks.prof_sample_free", &jemallocDeallocationTracker);
    setValue("experimental.hooks.prof_dump", &setLastFlushProfile);
}


const MibCache<bool> & getThreadProfileActiveMib()
{
    static MibCache<bool> thread_profile_active("thread.prof.active");
    return thread_profile_active;

}

const MibCache<bool> & getThreadProfileInitMib()
{
    static MibCache<bool> thread_profile_init("prof.thread_active_init");
    return thread_profile_init;
}

std::string_view getLastFlushProfileForThread()
{
    return last_flush_profile;
}

}

}

#endif
