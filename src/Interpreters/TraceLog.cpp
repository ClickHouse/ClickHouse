#include <Columns/ColumnLowCardinality.h>
#include <Common/SystemTableDocumentation.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromArena.h>
#include <Interpreters/InstrumentationManager.h>
#include <Interpreters/TraceLog.h>
#include <base/demangle.h>
#include <base/getFQDNOrHostName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DateLUTImpl.h>
#include <Common/Dwarf.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SymbolIndex.h>

#include <filesystem>


namespace DB
{

using TraceDataType = TraceLogElement::TraceDataType;
const TraceDataType::Values TraceLogElement::trace_values =
{
    {"Real", static_cast<UInt8>(TraceType::Real)},
    {"CPU", static_cast<UInt8>(TraceType::CPU)},
    {"Memory", static_cast<UInt8>(TraceType::Memory)},
    {"MemorySample", static_cast<UInt8>(TraceType::MemorySample)},
    {"MemoryPeak", static_cast<UInt8>(TraceType::MemoryPeak)},
    {"ProfileEvent", static_cast<UInt8>(TraceType::ProfileEvent)},
    {"JemallocSample", static_cast<UInt8>(TraceType::JemallocSample)},
    {"MemoryAllocatedWithoutCheck", static_cast<UInt8>(TraceType::MemoryAllocatedWithoutCheck)},
    {"Instrumentation", static_cast<UInt8>(TraceType::Instrumentation)},
};

static_assert(TraceSender::MEMORY_CONTEXT_UNKNOWN == -1);
using ContextDataType = TraceLogElement::ContextDataType;
const ContextDataType::Values TraceLogElement::context_values =
{
    {"Unknown", static_cast<Int8>(TraceSender::MEMORY_CONTEXT_UNKNOWN)},
    {"Global", static_cast<Int8>(VariableContext::Global)},
    {"User", static_cast<Int8>(VariableContext::User)},
    {"Process", static_cast<Int8>(VariableContext::Process)},
    {"Thread", static_cast<Int8>(VariableContext::Thread)},
    /// Only for MemoryTrackerBlockerInThread, Max means inactive.
    {"Max", static_cast<Int8>(VariableContext::Max)},
};

ColumnsDescription TraceLogElement::getColumnsDescription()
{
    DataTypePtr symbolized_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));

    constexpr std::string_view context_description =
        "`Unknown` context is not defined for this trace_type. "
        "`Global` represents server context. "
        "`User` represents user/merge context. "
        "`Process` represents process (i.e. query) context. "
        "`Thread` represents thread (thread of particular process) context. "
        "`Max` this is a special value means that memory tracker is not blocked (for blocked_context column). ";

    auto entry_type_enum = std::make_shared<DataTypeEnum8> (
        DataTypeEnum8::Values
        {
            {"Entry", static_cast<Int8>(Instrumentation::EntryType::ENTRY)},
            {"Exit", static_cast<Int8>(Instrumentation::EntryType::EXIT)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of sampling moment."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the sampling moment."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Timestamp of the sampling moment with microseconds precision."},
        {"timestamp_ns", std::make_shared<DataTypeUInt64>(), "Timestamp of the sampling moment in nanoseconds."},
        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse server build revision. When connecting to the server by `clickhouse-client`, you see a string similar to `Connected to ClickHouse server version 19.18.1.`. This field contains the `revision`, but not the `version` of a server."},
        {"trace_type", std::make_shared<TraceDataType>(trace_values), "Trace type: "
            "`Real` represents collecting stack traces by wall-clock time. "
            "`CPU` represents collecting stack traces by CPU time. "
            "`Memory` represents collecting allocations and deallocations when memory allocation exceeds the subsequent watermark. "
            "`MemorySample` represents collecting random allocations and deallocations. "
            "`MemoryPeak` represents collecting updates of peak memory usage. "
            "`ProfileEvent` represents collecting of increments of profile events. "
            "`JemallocSample` represents collecting of jemalloc samples. "
            "`MemoryAllocatedWithoutCheck` represents collection of significant allocations (>16MiB) that is done with ignoring any memory limits (for ClickHouse developers only)."
            "`Instrumentation` represents traces collected by the instrumentation performed through XRay."
        },
        {"cpu_id", std::make_shared<DataTypeUInt64>(), "CPU identifier."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "Thread identifier."},
        {"thread_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Thread name."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier that can be used to get details about a query that was running from the query_log system table."},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Stack trace at the moment of sampling. "
            "For profiler-collected trace types, on ELF platforms except FreeBSD, addresses inside the main ClickHouse binary are stored as physical file offsets, "
            "and other addresses are virtual memory addresses inside the ClickHouse server process. "
            "Instrumentation trace rows are an exception: they store raw virtual memory addresses."},
        {"size", std::make_shared<DataTypeInt64>(), "For trace types Memory, MemorySample, MemoryAllocatedWithoutCheck or MemoryPeak is the amount of memory allocated, for other trace types is 0."},
        {"ptr", std::make_shared<DataTypeUInt64>(), "The address of the allocated chunk."},
        {"memory_context", std::make_shared<ContextDataType>(context_values), fmt::format("Memory Tracker context (only for Memory/MemoryPeak): {}", context_description)},
        {"memory_blocked_context", std::make_shared<ContextDataType>(context_values), fmt::format("Context for which memory tracker is blocked (for ClickHouse developers only): {}", context_description)},
        {"event", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "For trace type ProfileEvent is the name of updated profile event, for other trace types is an empty string."},
        {"increment", std::make_shared<DataTypeInt64>(), "For trace type ProfileEvent is the amount of increment of profile event, for other trace types is 0."},
        {"symbols", symbolized_type, "If the symbolization is enabled, contains demangled symbol names, corresponding to the `trace`. Symbolization can be enabled or disabled in the `symbolize` setting under `trace_log` in the server configuration file; the setting applies to profiler-collected trace types, while rows with the `Instrumentation` trace type are symbolized regardless of it. Symbolization is supported on ELF platforms (such as Linux) and macOS; on FreeBSD this column is always empty."},
        {"lines", symbolized_type, "If the symbolization is enabled, contains strings with file names with line numbers, corresponding to the `trace`. The `symbolize` setting applies to profiler-collected trace types, while rows with the `Instrumentation` trace type are symbolized regardless of it. Symbolization is supported on ELF platforms (such as Linux) and macOS; on FreeBSD this column is always empty. Source locations are best-effort: they require debug info (a `.dSYM` bundle on macOS) and, on ELF platforms, are resolved only for frames inside the main ClickHouse binary; unresolved frames have empty entries."},
        {"function_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "For trace type Instrumentation, ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "For trace type Instrumentation, name of the instrumented function."},
        {"handler", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "For trace type Instrumentation, handler of the instrumented function."},
        {"entry_type", std::make_shared<DataTypeNullable>(entry_type_enum), "For trace type Instrumentation, entry type of the instrumented function."},
        {"duration_nanoseconds", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "For trace type Instrumentation, time the function was running for in nanoseconds."},
    };
}

NamesAndAliases TraceLogElement::getNamesAndAliases()
{
    String build_id_hex;
#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
    build_id_hex = SymbolIndex::instance().getBuildIDHex();
#endif
    return
    {
        {"build_id", std::make_shared<DataTypeString>(), "\'" + build_id_hex + "\'"},
    };
}


#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
namespace
{
    class AddressToLineCache
    {
    private:
        Arena arena;
        using Map = HashMap<uintptr_t, std::string_view>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;

        void setResult(std::string_view & result, const Dwarf::LocationInfo & location, const VectorWithMemoryTracking<Dwarf::SymbolizedFrame> &)
        {
            const char * arena_begin = nullptr;
            WriteBufferFromArena out(arena, arena_begin);

            writeString(location.file.toString(), out);
            writeChar(':', out);
            writeIntText(location.line, out);
            writeChar(':', out);
            writeIntText(location.column, out);

            out.finalize();
            result = out.complete();
        }

        std::string_view impl(uintptr_t addr)
        {
            const SymbolIndex & symbol_index = SymbolIndex::instance();

#if defined(OS_DARWIN)
            /// DWARF for source locations lives in a .dSYM bundle on macOS (the Mach-O linker leaves it
            /// out of the binary). Without a dSYM there is no file:line info, so `lines` stays empty for
            /// this frame (the `symbols` column is still filled from the symbol table by the caller).
            const auto * object = symbol_index.findObject(reinterpret_cast<const void *>(addr));
            if (!object || !object->dsym)
                return {};
            auto dwarf_it = dwarfs.try_emplace(object->name, object->dsym).first;
            /// Convert the runtime address to the linked (pre-ASLR) address the dSYM's DWARF uses.
            const uintptr_t dwarf_addr = addr - object->slide;
#else
            const auto * object = symbol_index.thisObject();
            if (!object || !std::filesystem::exists(object->name))
                return {};
            auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
            const uintptr_t dwarf_addr = addr;
#endif
            Dwarf::LocationInfo location;
            VectorWithMemoryTracking<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
            std::string_view result;
            if (dwarf_it->second.findAddress(dwarf_addr, location, Dwarf::LocationInfoMode::FAST, frames))
            {
                setResult(result, location, frames);
                return result;
            }
            /// `lines` holds source locations only; an unresolved frame stays empty rather than
            /// borrowing the object path (that would violate the file:line:col column contract).
            return {};
        }

        std::string_view implCached(uintptr_t addr)
        {
            typename Map::LookupResult it = nullptr;
            bool inserted = false;
            map.emplace(addr, it, inserted);
            if (inserted)
                it->getMapped() = impl(addr);
            return it->getMapped();
        }

    public:
        static std::string_view get(uintptr_t addr)
        {
            static AddressToLineCache cache;
            return cache.implCached(addr);
        }
    };
}
#endif


void TraceLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    const auto & hostname = getFQDNOrHostName();
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(hostname.data(), hostname.size());
    typeid_cast<ColumnUInt16 &>(*columns[i++]).getData().push_back(static_cast<UInt16>(DateLUT::instance().toDayNum(event_time).toUnderType()));
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(static_cast<UInt32>(event_time));
    typeid_cast<ColumnDateTime64 &>(*columns[i++]).getData().push_back(event_time_microseconds);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(timestamp_ns);
    typeid_cast<ColumnUInt32 &>(*columns[i++]).getData().push_back(ClickHouseRevision::getVersionRevision());
    typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<UInt8>(trace_type));
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(cpu_id);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(thread_id);
    auto thread_name_str = toString(thread_name);
    typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(thread_name_str.data(), thread_name_str.size());
    typeid_cast<ColumnString &>(*columns[i++]).insertData(query_id.data(), query_id.size());

    auto & column_trace = typeid_cast<ColumnArray &>(*columns[i++]);
    auto & column_trace_inner = typeid_cast<ColumnUInt64 &>(column_trace.getData());
    column_trace_inner.getData().insert(column_trace_inner.getData().end(), trace.begin(), trace.end());
    auto & offsets = column_trace.getOffsets();
    offsets.push_back(offsets.back() + trace.size());

    typeid_cast<ColumnInt64 &>(*columns[i++]).getData().push_back(size);
    typeid_cast<ColumnUInt64 &>(*columns[i++]).getData().push_back(ptr);
    if (memory_context.has_value())
        typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(memory_context.value()));
    else
        typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(TraceSender::MEMORY_CONTEXT_UNKNOWN));
    if (memory_blocked_context.has_value())
        typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(memory_blocked_context.value()));
    else
        typeid_cast<ColumnInt8 &>(*columns[i++]).getData().push_back(static_cast<Int8>(TraceSender::MEMORY_CONTEXT_UNKNOWN));

    if (event != ProfileEvents::end())
    {
        auto event_name = ProfileEvents::getName(event);
        typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertData(event_name.data(), event_name.size());
    }
    else
    {
        typeid_cast<ColumnLowCardinality &>(*columns[i++]).insertDefault();
    }

    typeid_cast<ColumnInt64 &>(*columns[i++]).getData().push_back(increment);

#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
    if (symbolize)
    {
        auto & column_symbols = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_symbols_inner = typeid_cast<ColumnLowCardinality &>(column_symbols.getData());

        auto & column_lines = typeid_cast<ColumnArray &>(*columns[i++]);
        auto & column_lines_inner = typeid_cast<ColumnLowCardinality &>(column_lines.getData());

        const SymbolIndex & symbol_index = SymbolIndex::instance();
        size_t num_frames = trace.size();
        for (size_t frame = 0; frame < num_frames; ++frame)
        {
            if (const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(trace[frame])))
            {
                auto demangled = tryDemangle(symbol->name);
                if (demangled)
                    column_symbols_inner.insertData(demangled.get(), strlen(demangled.get()));
                else
                    column_symbols_inner.insertData(symbol->name, strlen(symbol->name));

                column_lines_inner.insert(AddressToLineCache::get(trace[frame]));
            }
            else
            {
                column_symbols_inner.insertDefault();
                column_lines_inner.insertDefault();
            }
        }

        column_symbols.getOffsets().push_back(column_symbols.getOffsets().back() + num_frames);
        column_lines.getOffsets().push_back(column_lines.getOffsets().back() + num_frames);
    }
    else
#endif
    {
        typeid_cast<ColumnArray &>(*columns[i++]).insertDefault();
        typeid_cast<ColumnArray &>(*columns[i++]).insertDefault();
    }

    typeid_cast<ColumnNullable &>(*columns[i++])
        .insertData(function_id > 0 ? reinterpret_cast<const char *>(&function_id) : nullptr, sizeof(function_id));
    typeid_cast<ColumnNullable &>(*columns[i++])
        .insertData(!function_name.empty() > 0 ? function_name.data() : nullptr, function_name.size());
    typeid_cast<ColumnNullable &>(*columns[i++]).insertData(!handler.empty() > 0 ? handler.data() : nullptr, handler.size());
    typeid_cast<ColumnNullable &>(*columns[i++])
        .insertData(entry_type.has_value() ? reinterpret_cast<const char *>(&entry_type.value()) : nullptr, sizeof(Instrumentation::EntryType));
    typeid_cast<ColumnNullable &>(*columns[i++])
        .insertData(duration_nanoseconds.has_value() ? reinterpret_cast<const char *>(&duration_nanoseconds.value()) : nullptr, sizeof(UInt64));
}

}

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "trace_log",
    .description = R"DOCS_MD(
Contains stack traces collected by the [sampling query profiler](/concepts/features/performance/troubleshoot/sampling-query-profiler).

ClickHouse creates this table when the [trace_log](/reference/settings/server-settings/settings/other#trace_log) server configuration section is set. Also see settings: [query_profiler_real_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_real_time_period_ns), [query_profiler_cpu_time_period_ns](/reference/settings/session-settings/query-profiler#query_profiler_cpu_time_period_ns), [memory_profiler_step](/reference/settings/session-settings/memory-profiler#memory_profiler_step),
[memory_profiler_sample_probability](/reference/settings/session-settings/memory-profiler#memory_profiler_sample_probability), [trace_profile_events](/reference/settings/session-settings/trace-profile-events#trace_profile_events).

When symbolization is enabled (the default), the demangled function names and source locations are already available in the `symbols` and `lines` columns, so you can analyze the logs directly without introspection functions. The `symbolize` setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it. Symbolization is supported on ELF platforms (such as Linux) and macOS; on FreeBSD the `symbols` and `lines` columns are always empty. Function names in `symbols` come from the binary's symbol table and are available by default, while source locations in `lines` are best-effort: they require debug info (a `.dSYM` bundle on macOS) and, on ELF platforms, are resolved only for frames inside the main ClickHouse binary; unresolved frames have empty `lines` entries.
If symbolization is disabled, or you want to resolve the raw addresses in the `trace` column on the fly (for example, to expand inline frames), use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` introspection functions. These functions are available on the same platforms as symbolization (ELF platforms such as Linux, and macOS); on FreeBSD they are not compiled in either, so the addresses in `trace` have to be resolved outside the server.
)DOCS_MD",
    .columns_notes = R"DOCS_MD(
Symbolization can be enabled or disabled with the `symbolize` setting under `trace_log` in the server's configuration file. It is enabled by default. The setting applies to profiler-collected trace types; rows with the `Instrumentation` trace type are symbolized regardless of it.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-11
event_time:              2025-11-11 11:53:59
event_time_microseconds: 2025-11-11 11:53:59.128333
timestamp_ns:            1762862039128333000
revision:                54504
trace_type:              Instrumentation
cpu_id:                  19
thread_id:               3166432 -- 3.17 million
query_id:                ef462508-e189-4ea2-b231-4489506728e8
trace:                   [350594916,447733712,447742095,447727324,447726659,221642873,450882315,451852359,451905441,451885554,512404306,512509092,612861767,612863269,612466367,612455825,137631896259267,137631896856768]
size:                    0
ptr:                     0
memory_context:          Unknown
memory_blocked_context:  Unknown
event:
increment:               0
symbols:                 ['StackTrace::StackTrace()','DB::InstrumentationManager::createTraceLogElement(DB::InstrumentationManager::InstrumentedPointInfo const&, XRayEntryType, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>) const','DB::InstrumentationManager::profile(XRayEntryType, DB::InstrumentationManager::InstrumentedPointInfo const&)','DB::InstrumentationManager::dispatchHandlerImpl(int, XRayEntryType)','DB::InstrumentationManager::dispatchHandler(int, XRayEntryType)','__xray_FunctionEntry','DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)','DB::logQueryStart(std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>> const&, std::__1::shared_ptr<DB::Context> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, unsigned long, std::__1::shared_ptr<DB::IAST> const&, DB::QueryPipeline const&, DB::IInterpreter const*, bool, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, bool)','DB::executeQueryImpl(char const*, char const*, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum, std::__1::unique_ptr<DB::ReadBuffer, std::__1::default_delete<DB::ReadBuffer>>&, std::__1::shared_ptr<DB::IAST>&, std::__1::shared_ptr<DB::ImplicitTransactionControlExecutor>, std::__1::function<void ()>)','DB::executeQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum)','DB::TCPHandler::runImpl()','DB::TCPHandler::run()','Poco::Net::TCPServerConnection::start()','Poco::Net::TCPServerDispatcher::run()','Poco::PooledThread::run()','Poco::ThreadImpl::runnableEntry(void*)','start_thread','__clone3']
lines:                   ['./build/../src/Common/StackTrace.cpp:395','./src/Common/StackTrace.h:62','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:738','./build/./src/Interpreters/InstrumentationManager.cpp:257','./build/./src/Interpreters/InstrumentationManager.cpp:225','','./build/./src/Interpreters/QueryMetricLog.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:667','./build/./src/Interpreters/executeQuery.cpp:0','./build/./src/Interpreters/executeQuery.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:744','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:583','./build/../base/poco/Net/src/TCPServerConnection.cpp:54','../contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:80','./build/../base/poco/Foundation/src/ThreadPool.cpp:219','../base/poco/Foundation/include/Poco/AutoPtr.h:77','','']
function_id:             231255
function_name:           DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
handler:                 profile
entry_type:              Exit
duration_nanoseconds:   58435
```
)DOCS_MD",
    .additional_sections = R"DOCS_MD(
## Converting to Chrome Event Trace Format {#chrome-event-trace-format}

The profiling data can be converted to Chrome's Event Trace Format with the following query. Save the query to a `chrome_trace.sql` file:

```sql
WITH traces AS (
    SELECT * FROM system.trace_log
    WHERE event_date >= today() AND trace_type = 'Instrumentation' AND handler = 'profile'
    ORDER BY event_time, entry_type
)
SELECT
    format(
        '{{"traceEvents": [{}\n]}}',
        arrayStringConcat(
            groupArray(
                format(
                    '\n{{"name": "{}", "cat": "clickhouse", "ph": "{}", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", "cpu_id": {}, "stack": [{}]}}}},',
                    function_name,
                    if(entry_type = 0, 'B', 'E'),
                    timestamp_ns/1000,
                    toString(thread_id),
                    query_id,
                    cpu_id,
                    arrayStringConcat(arrayMap((x, y) -> concat('"', x, ': ', y, '", '), lines, symbols))
                )
            )
        )
    )
FROM traces;
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
echo $(clickhouse client --query "$(cat chrome_trace.sql)") > trace.json
```

We can omit the stack part if we want a more compact but less informative trace.
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.instrumentation](/reference/system-tables/instrumentation) — Inspect instrumented points.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD")

}
