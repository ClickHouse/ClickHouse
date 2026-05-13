#include <Columns/ColumnLowCardinality.h>
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
        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse server build revision."},
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
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process."},
        {"size", std::make_shared<DataTypeInt64>(), "For trace types Memory, MemorySample, MemoryAllocatedWithoutCheck or MemoryPeak is the amount of memory allocated, for other trace types is 0."},
        {"ptr", std::make_shared<DataTypeUInt64>(), "The address of the allocated chunk."},
        {"memory_context", std::make_shared<ContextDataType>(context_values), fmt::format("Memory Tracker context (only for Memory/MemoryPeak): {}", context_description)},
        {"memory_blocked_context", std::make_shared<ContextDataType>(context_values), fmt::format("Context for which memory tracker is blocked (for ClickHouse developers only): {}", context_description)},
        {"event", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "For trace type ProfileEvent is the name of updated profile event, for other trace types is an empty string."},
        {"increment", std::make_shared<DataTypeInt64>(), "For trace type ProfileEvent is the amount of increment of profile event, for other trace types is 0."},
        {"symbols", symbolized_type, "If the symbolization is enabled, contains demangled symbol names, corresponding to the `trace`."},
        {"lines", symbolized_type, "If the symbolization is enabled, contains strings with file names with line numbers, corresponding to the `trace`."},
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
#if defined(__ELF__) && !defined(OS_FREEBSD)
    build_id_hex = SymbolIndex::instance().getBuildIDHex();
#endif
    return
    {
        {"build_id", std::make_shared<DataTypeString>(), "\'" + build_id_hex + "\'"},
    };
}


#if defined(__ELF__) && !defined(OS_FREEBSD)
namespace
{
    class AddressToLineCache
    {
    private:
        Arena arena;
        using Map = HashMap<uintptr_t, std::string_view>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;

        void setResult(std::string_view & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &)
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

            if (const auto * object = symbol_index.thisObject())
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
                if (!std::filesystem::exists(object->name))
                    return {};

                Dwarf::LocationInfo location;
                std::vector<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
                std::string_view result;
                if (dwarf_it->second.findAddress(addr, location, Dwarf::LocationInfoMode::FAST, frames))
                {
                    setResult(result, location, frames);
                    return result;
                }
                return object->name;
            }
            return {};
        }

        std::string_view implCached(uintptr_t addr)
        {
            typename Map::LookupResult it;
            bool inserted;
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

#if defined(__ELF__) && !defined(OS_FREEBSD)
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
