#include <base/getFQDNOrHostName.h>
#include <base/demangle.h>
#include <Common/DateLUTImpl.h>
#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/ClickHouseRevision.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <IO/WriteBufferFromArena.h>

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
        },
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
        using Map = HashMap<uintptr_t, StringRef>;
        Map map;
        std::unordered_map<std::string, Dwarf> dwarfs;

        void setResult(StringRef & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &)
        {
            const char * arena_begin = nullptr;
            WriteBufferFromArena out(arena, arena_begin);

            writeString(location.file.toString(), out);
            writeChar(':', out);
            writeIntText(location.line, out);

            out.finalize();
            result = out.complete();
        }

        StringRef impl(uintptr_t addr)
        {
            const SymbolIndex & symbol_index = SymbolIndex::instance();

            if (const auto * object = symbol_index.thisObject())
            {
                auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
                if (!std::filesystem::exists(object->name))
                    return {};

                Dwarf::LocationInfo location;
                std::vector<Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
                StringRef result;
                if (dwarf_it->second.findAddress(addr, location, Dwarf::LocationInfoMode::FAST, frames))
                {
                    setResult(result, location, frames);
                    return result;
                }
                return {object->name};
            }
            return {};
        }

        StringRef implCached(uintptr_t addr)
        {
            typename Map::LookupResult it;
            bool inserted;
            map.emplace(addr, it, inserted);
            if (inserted)
                it->getMapped() = impl(addr);
            return it->getMapped();
        }

    public:
        static StringRef get(uintptr_t addr)
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

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(timestamp_ns);
    columns[i++]->insert(ClickHouseRevision::getVersionRevision());
    columns[i++]->insert(static_cast<UInt8>(trace_type));
    columns[i++]->insert(thread_id);
    auto thread_name_str = toString(thread_name);
    columns[i++]->insertData(thread_name_str.data(), thread_name_str.size());
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(Array(trace.begin(), trace.end()));
    columns[i++]->insert(size);
    columns[i++]->insert(ptr);
    if (memory_context.has_value())
        columns[i++]->insert(static_cast<Int8>(memory_context.value()));
    else
        columns[i++]->insert(static_cast<Int8>(TraceSender::MEMORY_CONTEXT_UNKNOWN));
    if (memory_blocked_context.has_value())
        columns[i++]->insert(static_cast<Int8>(memory_blocked_context.value()));
    else
        columns[i++]->insert(static_cast<Int8>(TraceSender::MEMORY_CONTEXT_UNKNOWN));

    String event_name;
    if (event != ProfileEvents::end())
        event_name = ProfileEvents::getName(event);

    columns[i++]->insert(event_name);
    columns[i++]->insert(increment);

#if defined(__ELF__) && !defined(OS_FREEBSD)
    if (symbolize)
    {
        Array symbols;
        Array lines;
        size_t num_frames = trace.size();
        symbols.reserve(num_frames);
        lines.reserve(num_frames);

        const SymbolIndex & symbol_index = SymbolIndex::instance();

        for (size_t frame = 0; frame < num_frames; ++frame)
        {
            if (const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(trace[frame])))
            {
                std::string_view mangled_symbol(symbol->name);

                auto demangled = tryDemangle(symbol->name);
                if (demangled)
                    symbols.emplace_back(std::string_view(demangled.get()));
                else
                    symbols.emplace_back(std::string_view(symbol->name));

                lines.emplace_back(AddressToLineCache::get(trace[frame]).toView());
            }
            else
            {
                symbols.emplace_back(String());
                lines.emplace_back(String());
            }
        }

        columns[i++]->insert(symbols);
        columns[i++]->insert(lines);
    }
    else
#endif
    {
        columns[i++]->insertDefault();
        columns[i++]->insertDefault();
    }
}

}
