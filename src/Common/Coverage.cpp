#include "Coverage.h"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/core.h>
#include <fmt/format.h>

#include <zlib.h>

#include "Common/ProfileEvents.h"

namespace detail
{
namespace
{
inline auto getInstanceAndInitGlobalCounters()
{
    /**
     * Writer is a singleton, so it initializes statically.
     * SymbolIndex uses a MMapReadBufferFromFile which uses ProfileEvents.
     * If no thread was found in the events profiler, a global variable global_counters is used.
     *
     * This variable may get initialized after Writer (static initialization order fiasco).
     * In fact, the __sanitizer_cov_trace_pc_guard_init is called before the global_counters init.
     *
     * We can't use constinit on that variable as it has a shared_ptr on it, so we just
     * ultimately initialize it before getting the instance.
     *
     * We can't initialize global_counters in ProfileEvents.cpp to nullptr as in that case it will become nullptr.
     * So we just initialize it twice (here and in ProfileEvents.cpp).
     */
    ProfileEvents::global_counters = ProfileEvents::Counters(ProfileEvents::global_counters_array);

    return SymbolIndex::instance();
}
}

Writer::Writer()
    : hardware_concurrency(std::thread::hardware_concurrency()),
      base_log(nullptr),
      coverage_dir(std::filesystem::current_path() / Writer::coverage_dir_relative_path),
      symbol_index(getInstanceAndInitGlobalCounters()),
      dwarf(symbol_index->getSelf()->elf),
      // 0 -- unlimited queue e.g. functor insertion to thread pool won't lock.
      // Set the initial pool size to all thread as we'll need all resources to symbolize fast.
      pool(hardware_concurrency, 1000, 0)
{
    //if (std::filesystem::exists(coverage_dir))
    //{
    //    size_t suffix = 1;
    //    const std::string dir_path = coverage_dir.string();

    //    while (std::filesystem::exists(dir_path + "_" + std::to_string(suffix)))
    //        ++suffix;

    //    const std::string dir_new_path = dir_path + "_" + std::to_string(suffix);
    //    std::filesystem::rename(coverage_dir, dir_new_path);
    //}

    // BUG Creating folder out of CH folder
    std::filesystem::remove_all(coverage_dir);
    std::filesystem::create_directory(coverage_dir);
}

void Writer::initializePCTable(const uintptr_t *pc_array, const uintptr_t *pc_array_end)
{
    const size_t edges_pairs = pc_array_end - pc_array;
    edges_count = edges_pairs / 2;

    edges_hit.resize(edges_count, 0);
    edges_cache.resize(edges_count);

    edges_to_addrs.resize(edges_count);
    edge_is_func_entry.resize(edges_count);

    for (size_t i = 0; i < edges_pairs; i += 2)
    {
        const bool is_function_entry = pc_array[i + 1] & 1;

        edge_is_func_entry[i / 2] = is_function_entry;

        functions_count += is_function_entry;
        addrs_count += !is_function_entry;

        const uintptr_t addr = is_function_entry
            ?  pc_array[i]
            /**
             * If we use this addr as is, the SymbolIndex won't be able to find the line for our address.
             * General assembly looks like this:
             *
             * 0x12dbca75 <+117>: callq  0x12dd2680                ; DB::AggregateFunctionFactory::registerFunction at AggregateFunctionFactory.cpp:39
             * 0x12dbca7a <+122>: jmp    0x12dbca7f                ; <+127> at AggregateFunctionCount.cpp << ADDRESS SHOULD POINT HERE
             * 0x12dbca7f <+127>: movabsq $0x15b4522c, %rax         ; imm = 0x15B4522C << BUT POINTS HERE
             * 0x12dbca89 <+137>: addq   $0x4, %rax
             * 0x12dbca8f <+143>: movq   %rax, %rdi
             * 0x12dbca92 <+146>: callq  0xb067180 ; ::__sanitizer_cov_trace_pc_guard(uint32_t *) at CoverageCallbacks.h:15
             *
             * The symbolizer (as well as lldb and gdb) thinks that instruction at 0x12dbca7f (that sets edge_index for the
             * callback) is located at line 0.
             * So we need a way to get the previous instruction (llvm's SanCov does it in default callbacks):
             *  https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L769
             * LLVM's SanCov uses internal arch information to do that:
             *  https://github.com/llvm/llvm-project/blob/main/llvm/tools/sancov/sancov.cpp#L690
             */
            :  pc_array[i] - 1;

        edges_to_addrs[i / 2] = reinterpret_cast<void*>(addr);
    }

    /// We don't symbolize the addresses right away, wait for CH application to load instead.
    /// If starting now, we won't be able to log to Poco or std::cout;
}

void Writer::symbolizeInstrumentedData()
{
    std::vector<EdgeIndex> function_indices(functions_count);
    std::vector<EdgeIndex> addr_indices(addrs_count);

    LocalCaches<FuncSym> func_caches(hardware_concurrency);
    LocalCaches<AddrSym> addr_caches(hardware_concurrency);

    for (size_t i = 0, j = 0, k = 0; i < edges_count; ++i)
        if (edge_is_func_entry[i])
            function_indices[j++] = i;
        else
            addr_indices[k++] = i;

    LOG_INFO(base_log,
        "Split addresses into function entries ({}) and normal ones ({}), {} total."
        "Started symbolizing addresses, using thread pool of size {}",
        functions_count, addrs_count, edges_to_addrs.size(), hardware_concurrency);

    scheduleSymbolizationJobs<true>(func_caches, function_indices);

    pool.wait();

    LOG_INFO(base_log, "Symbolized all functions, populating address cache");

    /// Pre-populate addr_caches with already found source files.
    for (const auto& local_func_cache : func_caches)
        for (const auto& [source_name, data] : local_func_cache)
            for (auto & local_addr_cache : addr_caches)
                local_addr_cache[source_name] = {data.full_path, {}};

    LOG_INFO(base_log, "Finished populating address caches");

    scheduleSymbolizationJobs<false>(addr_caches, addr_indices);

    /// Merge functions data while other threads process addresses.
    mergeDataToCaches<true>(func_caches);

    pool.wait();

    mergeDataToCaches<false>(addr_caches);

    pool.setMaxThreads(hardware_concurrency / 2);

    LOG_INFO(base_log, "Symbolized all addresses");
}

void Writer::dumpAndChangeTestName(std::string old_test_name)
{
    /// In Context.cpp :: setSetting() setting is set under a unique lock. That means the hook also calls this function
    /// under a lock, so no mutex is needed.
    /// Note: this function slows down setSetting, so it should be as fast as possible.

    /// https://godbolt.org/z/qqTYdr5Gz, explicit 0 state generates slightly better assembly than without it.
    EdgesHit edges_hit_swap(edges_count, 0);

    test.swap(old_test_name);

    if (old_test_name.empty())
        return;

    edges_hit.swap(edges_hit_swap);

    /// Can't copy by ref as current function's lifetime may end before evaluating the functor.
    /// Move is evaluated within current function's lifetime during function constructor call.
    /// Functor insertion itself is thread-safe.
    pool.scheduleOrThrowOnError(
        [this, test_name = std::move(old_test_name), edges_copied = std::move(edges_hit_swap)] () mutable
    {
        /// genhtml doesn't allow '.' in test names
        std::replace(test_name.begin(), test_name.end(), '.', '_');

        const Poco::Logger * log = &Poco::Logger::get(std::string{logger_base_name} + "." + test_name);

        prepareDataAndDump({test_name, log}, edges_copied);
    });
}

void Writer::prepareDataAndDump(TestInfo test_info, const EdgesHit& hits)
{
    LOG_INFO(test_info.log, "Started filling internal structures");

    TestData test_data(source_files_cache.size());

    for (size_t edge_index = 0; edge_index < edges_count; ++edge_index)
    {
        const CallCount hit = hits[edge_index];

        if (!hit)
            continue;

        if (const EdgeInfo& info = edges_cache[edge_index]; info.isFunctionEntry())
            setOrIncrement(test_data[info.index].functions_hit, edge_index, hit);
        else
            setOrIncrement(test_data[info.index].lines_hit, info.line, hit);
    }

    LOG_INFO(test_info.log, "Finished filling internal structures");
    convertToLCOVAndDump(test_info, test_data);
}

namespace
{
constexpr size_t BUFLEN = 16384;

extern "C" void gzip(FILE * out_file, void * buf, size_t len)
{
    z_stream strm;
    unsigned char out[BUFLEN];

    strm.zalloc = [](auto, auto n, auto m) { return calloc(n, m); };
    strm.zfree = [](auto, auto p) { free(p); };
    strm.opaque = Z_NULL;

    // idk why c++ warnings appear in c code
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
    deflateInit2(&strm, -1, 8, 15 + 16, 8, 0);
#pragma clang diagnostic pop
#pragma GCC diagnostic pop

    strm.next_in = static_cast<unsigned char *>(buf);
    strm.avail_in = len;

    do {
        strm.next_out = out;
        strm.avail_out = BUFLEN;

        deflate(&strm, Z_NO_FLUSH);
        fwrite(out, 1, BUFLEN - strm.avail_out, out_file);
    } while (strm.avail_out == 0);

    strm.next_in = Z_NULL;
    strm.avail_in = 0;

    do {
        strm.next_out = out;
        strm.avail_out = BUFLEN;

        deflate(&strm, Z_FINISH);
        fwrite(out, 1, BUFLEN - strm.avail_out, out_file);
    } while (strm.avail_out == 0);

    deflateEnd(&strm);
}
}

void Writer::convertToLCOVAndDump(TestInfo test_info, const TestData& test_data)
{
    /**
     * [incomplete] LCOV .info format reference, parsed from
     * https://github.com/linux-test-project/lcov/blob/master/bin/geninfo
     *
     * TN:<test name>
     *
     * for each source file:
     *     SF:<absolute path to the source file>
     *
     *     for each instrumented function:
     *         FN:<line number of function start>,<function name>
     *         FNDA:<call-count>, <function-name>
     *
     *     if >0 functions instrumented:
     *         FNF:<number of functions instrumented>
     *         FNH:<number of functions hit>
     *
     *     for each instrumented line:
     *         DA:<line number>,<execution count> [, <line checksum, we don't use it>]
     *
     *     LF:<number of lines instrumented>
     *     LH:<number of lines hit>
     *     end_of_record
     */

    LOG_INFO(test_info.log, "Started dumping");

    const std::string test_path =
        (coverage_dir / (std::string{test_info.name} + ".gz"))
        // common use case is to run CH from build/programs directory. All source files paths look like
        // /home/user/ch/build/programs../../src/, so we can slightly reduce their size by removing
        // /build/programs/../../
        .lexically_normal()
        .string();

    constexpr size_t formatted_buffer_size = 10 * (1 >> 21); //20 MB

    fmt::memory_buffer mb;
    mb.reserve(formatted_buffer_size);

    fmt::format_to(mb, "TN:{}\n", test_info.name);

    for (size_t i = 0; i < test_data.size(); ++i)
    {
        const auto& [funcs_hit, lines_hit] = test_data[i];
        const auto& [path, funcs_instrumented, lines_instrumented] = source_files_cache[i];

        fmt::format_to(mb, "SF:{}\n", path);

        for (EdgeIndex index : funcs_instrumented)
        {
            const EdgeInfo& func_info = edges_cache[index];
            const CallCount call_count = valueOr(funcs_hit, index, 0);

            fmt::format_to(mb, "FN:{0},{1}\nFNDA:{2},{1}\n", func_info.line, func_info.name, call_count);
        }

        fmt::format_to(mb, "FNF:{}\nFNH:{}\n", funcs_instrumented.size(), funcs_hit.size());

        for (size_t line : lines_instrumented)
        {
            const CallCount call_count = valueOr(lines_hit, line, 0);
            fmt::format_to(mb, "DA:{},{}\n", line, call_count);
        }

        fmt::format_to(mb, "LF:{}\nLH:{}\nend_of_record\n", lines_instrumented.size(), lines_hit.size());
    }

    LOG_INFO(test_info.log, "Finished writing to format buffer");

    FILE * const out_file = fopen(test_path.data(), "w");
    gzip(out_file, mb.begin(), mb.size());
    fclose(out_file);

    LOG_INFO(test_info.log, "Finished compressing");

    Poco::Logger::destroy(test_info.log->name());
}
}
