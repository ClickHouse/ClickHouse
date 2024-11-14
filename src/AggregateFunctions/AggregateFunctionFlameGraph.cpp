#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SymbolIndex.h>
#include <Common/ArenaAllocator.h>
#include <Core/Settings.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <filesystem>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_introspection_functions;
}


namespace ErrorCodes
{
    extern const int FUNCTION_NOT_ALLOWED;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct AggregateFunctionFlameGraphTree
{
    struct ListNode;

    struct TreeNode
    {
        TreeNode * parent = nullptr;
        ListNode * children = nullptr;
        UInt64 ptr = 0;
        size_t allocated = 0;
    };

    struct ListNode
    {
        ListNode * next = nullptr;
        TreeNode * child = nullptr;
    };

    TreeNode root;

    static ListNode * createChild(TreeNode * parent, UInt64 ptr, Arena * arena)
    {

        ListNode * list_node = reinterpret_cast<ListNode *>(arena->alloc(sizeof(ListNode)));
        TreeNode * tree_node = reinterpret_cast<TreeNode *>(arena->alloc(sizeof(TreeNode)));

        list_node->child = tree_node;
        list_node->next = nullptr;

        tree_node->parent =parent;
        tree_node->children = nullptr;
        tree_node->ptr = ptr;
        tree_node->allocated = 0;

        return list_node;
    }

    TreeNode * find(const UInt64 * stack, size_t stack_size, Arena * arena)
    {
        TreeNode * node = &root;
        for (size_t i = 0; i < stack_size; ++i)
        {
            UInt64 ptr = stack[i];
            if (ptr == 0)
                break;

            if (!node->children)
            {
                node->children = createChild(node, ptr, arena);
                node = node->children->child;
            }
            else
            {
                ListNode * list = node->children;
                while (list->child->ptr != ptr && list->next)
                    list = list->next;

                if (list->child->ptr != ptr)
                {
                    list->next = createChild(node, ptr, arena);
                    list = list->next;
                }

                node = list->child;
            }
        }

        return node;
    }

    static void append(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, std::vector<UInt64> & frame)
    {
        UInt64 prev = offsets.empty() ? 0 : offsets.back();
        offsets.push_back(prev + frame.size());
        for (UInt64 val : frame)
            values.push_back(val);
    }

    struct Trace
    {
        using Frames = std::vector<UInt64>;

        Frames frames;

        /// The total number of bytes allocated for traces with the same prefix.
        size_t allocated_total = 0;
        /// This counter is relevant in case we want to filter some traces with small amount of bytes.
        /// It shows the total number of bytes for *filtered* traces with the same prefix.
        /// This is the value which is used in flamegraph.
        size_t allocated_self = 0;
    };

    using Traces = std::vector<Trace>;

    Traces dump(size_t max_depth, size_t min_bytes) const
    {
        Traces traces;
        Trace::Frames frames;
        std::vector<size_t> allocated_total;
        std::vector<size_t> allocated_self;
        std::vector<ListNode *> nodes;

        nodes.push_back(root.children);
        allocated_total.push_back(root.allocated);
        allocated_self.push_back(root.allocated);

        while (!nodes.empty())
        {
            if (nodes.back() == nullptr)
            {
                traces.push_back({frames, allocated_total.back(), allocated_self.back()});

                nodes.pop_back();
                allocated_total.pop_back();
                allocated_self.pop_back();

                /// We don't have root's frame so framers are empty in the end.
                if (!frames.empty())
                    frames.pop_back();

                continue;
            }

            TreeNode * current = nodes.back()->child;
            nodes.back() = nodes.back()->next;

            bool enough_bytes = current->allocated >= min_bytes;
            bool enough_depth = max_depth == 0 || nodes.size() < max_depth;

            if (enough_bytes)
            {
                frames.push_back(current->ptr);
                allocated_self.back() -= current->allocated;

                if (enough_depth)
                {
                    allocated_total.push_back(current->allocated);
                    allocated_self.push_back(current->allocated);
                    nodes.push_back(current->children);
                }
                else
                {
                    traces.push_back({frames, current->allocated, current->allocated});
                    frames.pop_back();
                }
            }
        }

        return traces;
    }
};

static void insertData(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, const char * pos, size_t length)
{
    const size_t old_size = chars.size();
    const size_t new_size = old_size + length + 1;

    chars.resize(new_size);
    if (length)
        memcpy(chars.data() + old_size, pos, length);
    chars[old_size + length] = 0;
    offsets.push_back(new_size);
}

/// Split str by line feed and write as separate row to ColumnString.
static void fillColumn(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, const std::string & str)
{
    size_t start = 0;
    size_t end = 0;
    size_t size = str.size();

    while (end < size)
    {
        if (str[end] == '\n')
        {
            insertData(chars, offsets, str.data() + start, end - start);
            start = end + 1;
        }

        ++end;
    }

    if (start < end)
        insertData(chars, offsets, str.data() + start, end - start);
}

void dumpFlameGraph(
    const AggregateFunctionFlameGraphTree::Traces & traces,
    DB::PaddedPODArray<UInt8> & chars,
    DB::PaddedPODArray<UInt64> & offsets)
{
    DB::WriteBufferFromOwnString out;

    std::unordered_map<uintptr_t, size_t> mapping;

#if defined(__ELF__) && !defined(OS_FREEBSD)
    const DB::SymbolIndex & symbol_index = DB::SymbolIndex::instance();
#endif

    for (const auto & trace : traces)
    {
        if (trace.allocated_self == 0)
            continue;

        for (size_t i = 0; i < trace.frames.size(); ++i)
        {
            if (i)
                out << ";";

            const void * ptr = reinterpret_cast<const void *>(trace.frames[i]);

#if defined(__ELF__) && !defined(OS_FREEBSD)
            if (const auto * symbol = symbol_index.findSymbol(ptr))
                writeString(demangle(symbol->name), out);
            else
                DB::writePointerHex(ptr, out);
#else
            DB::writePointerHex(ptr, out);
#endif
        }

        out << ' ' << trace.allocated_self << "\n";
    }

    fillColumn(chars, offsets, out.str());
}

struct AggregateFunctionFlameGraphData
{
    struct Entry
    {
        AggregateFunctionFlameGraphTree::TreeNode * trace;
        UInt64 size;
        Entry * next = nullptr;
    };

    struct Pair
    {
        Entry * allocation = nullptr;
        Entry * deallocation = nullptr;
    };

    using Entries = HashMap<UInt64, Pair>;

    Entries entries;
    Entry * free_list = nullptr;
    AggregateFunctionFlameGraphTree tree;

    Entry * alloc(Arena * arena)
    {
        if (free_list)
        {
            auto * res = free_list;
            free_list = free_list->next;
            return res;
        }

        return reinterpret_cast<Entry *>(arena->alloc(sizeof(Entry)));
    }

    void release(Entry * entry)
    {
        entry->next = free_list;
        free_list = entry;
    }

    static void track(Entry * allocation)
    {
        auto * node = allocation->trace;
        while (node)
        {
            node->allocated += allocation->size;
            node = node->parent;
        }
    }

    static void untrack(Entry * allocation)
    {
        auto * node = allocation->trace;
        while (node)
        {
            node->allocated -= allocation->size;
            node = node->parent;
        }
    }

    static Entry * tryFindMatchAndRemove(Entry *& list, UInt64 size)
    {
        if (!list)
            return nullptr;

        if (list->size == size)
        {
            Entry * entry = list;
            list = list->next;
            return entry;
        }

        Entry * parent = list;
        while (parent->next && parent->next->size != size)
            parent = parent->next;

        if (parent->next && parent->next->size == size)
        {
            Entry * entry = parent->next;
            parent->next = entry->next;
            return entry;
        }

        return nullptr;
    }

    void add(UInt64 ptr, Int64 size, const UInt64 * stack, size_t stack_size, Arena * arena)
    {
        /// In case if argument is nullptr, only track allocations.
        if (ptr == 0)
        {
            if (size > 0)
            {
                auto * node = tree.find(stack, stack_size, arena);
                Entry entry{.trace = node, .size = UInt64(size)};
                track(&entry);
            }

            return;
        }

        auto & place = entries[ptr];
        if (size > 0)
        {
            if (auto * deallocation = tryFindMatchAndRemove(place.deallocation, size))
            {
                release(deallocation);
            }
            else
            {
                auto * node = tree.find(stack, stack_size, arena);

                auto * allocation = alloc(arena);
                allocation->size = UInt64(size);
                allocation->trace = node;

                track(allocation);

                allocation->next = place.allocation;
                place.allocation = allocation;
            }
        }
        else if (size < 0)
        {
            UInt64 abs_size = -size;
            if (auto * allocation = tryFindMatchAndRemove(place.allocation, abs_size))
            {
                untrack(allocation);
                release(allocation);
            }
            else
            {
                auto * deallocation = alloc(arena);
                deallocation->size = abs_size;

                deallocation->next = place.deallocation;
                place.deallocation = deallocation;
            }
        }
    }

    void merge(const AggregateFunctionFlameGraphTree & other_tree, Arena * arena)
    {
        AggregateFunctionFlameGraphTree::Trace::Frames frames;
        std::vector<AggregateFunctionFlameGraphTree::ListNode *> nodes;

        nodes.push_back(other_tree.root.children);

        while (!nodes.empty())
        {
            if (nodes.back() == nullptr)
            {
                nodes.pop_back();

                /// We don't have root's frame so framers are empty in the end.
                if (!frames.empty())
                    frames.pop_back();

                continue;
            }

            AggregateFunctionFlameGraphTree::TreeNode * current = nodes.back()->child;
            nodes.back() = nodes.back()->next;

            frames.push_back(current->ptr);

            if (current->children)
                nodes.push_back(current->children);
            else
            {
                if (current->allocated)
                    add(0, current->allocated, frames.data(), frames.size(), arena);

                frames.pop_back();
            }
        }
    }

    void merge(const AggregateFunctionFlameGraphData & other, Arena * arena)
    {
        AggregateFunctionFlameGraphTree::Trace::Frames frames;
        for (const auto & entry : other.entries)
        {
            for (auto * allocation = entry.value.second.allocation; allocation; allocation = allocation->next)
            {
                frames.clear();
                const auto * node = allocation->trace;
                while (node->ptr)
                {
                    frames.push_back(node->ptr);
                    node = node->parent;
                }

                std::reverse(frames.begin(), frames.end());
                add(entry.value.first, allocation->size, frames.data(), frames.size(), arena);
                untrack(allocation);
            }

            for (auto * deallocation = entry.value.second.deallocation; deallocation; deallocation = deallocation->next)
            {
                add(entry.value.first, -Int64(deallocation->size), nullptr, 0, arena);
            }
        }

        merge(other.tree, arena);
    }

    void dumpFlameGraph(
        DB::PaddedPODArray<UInt8> & chars,
        DB::PaddedPODArray<UInt64> & offsets,
        size_t max_depth, size_t min_bytes) const
    {
        DB::dumpFlameGraph(tree.dump(max_depth, min_bytes), chars, offsets);
    }
};

/// Aggregate function which builds a flamegraph using the list of stacktraces.
/// The output is an array of strings which can be used by flamegraph.pl util.
/// See https://github.com/brendangregg/FlameGraph
///
/// Syntax: flameGraph(traces, [size = 1], [ptr = 0])
/// - trace : Array(UInt64), a stacktrace
/// - size  : Int64, an allocation size (for memory profiling)
/// - ptr   : UInt64, an allocation address
/// In case if ptr != 0, a flameGraph will map allocations (size > 0) and deallocations (size < 0) with the same size and ptr.
/// Only allocations which were not freed are shown. Not mapped deallocations are ignored.
///
/// Usage:
///
/// * Build a flamegraph based on CPU query profiler
/// set query_profiler_cpu_time_period_ns=10000000;
/// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
/// clickhouse client --allow_introspection_functions=1
///     -q "select arrayJoin(flameGraph(arrayReverse(trace))) from system.trace_log where trace_type = 'CPU' and query_id = 'xxx'"
///     | ~/dev/FlameGraph/flamegraph.pl  > flame_cpu.svg
///
/// * Build a flamegraph based on memory query profiler, showing all allocations
/// set memory_profiler_sample_probability=1, max_untracked_memory=1;
/// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
/// clickhouse client --allow_introspection_functions=1
///     -q "select arrayJoin(flameGraph(trace, size)) from system.trace_log where trace_type = 'MemorySample' and query_id = 'xxx'"
///     | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem.svg
///
/// * Build a flamegraph based on memory query profiler, showing allocations which were not deallocated in query context
/// set memory_profiler_sample_probability=1, max_untracked_memory=1, use_uncompressed_cache=1, merge_tree_max_rows_to_use_cache=100000000000, merge_tree_max_bytes_to_use_cache=1000000000000;
/// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
/// clickhouse client --allow_introspection_functions=1
///     -q "select arrayJoin(flameGraph(trace, size, ptr)) from system.trace_log where trace_type = 'MemorySample' and query_id = 'xxx'"
///     | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_untracked.svg
///
/// *  Build a flamegraph based on memory query profiler, showing active allocations at the fixed point of time
/// set memory_profiler_sample_probability=1, max_untracked_memory=1;
/// SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
/// 1. Memory usage per second
/// select event_time, m, formatReadableSize(max(s) as m) from (select event_time, sum(size) over (order by event_time) as s from system.trace_log where query_id = 'xxx' and trace_type = 'MemorySample') group by event_time order by event_time;
/// 2. Find a time point with maximal memory usage
/// select argMax(event_time, s), max(s) from (select event_time, sum(size) over (order by event_time) as s from system.trace_log where query_id = 'xxx' and trace_type = 'MemorySample');
/// 3. Fix active allocations at fixed point of time
/// clickhouse client --allow_introspection_functions=1
///      -q "select arrayJoin(flameGraph(trace, size, ptr)) from (select * from system.trace_log where trace_type = 'MemorySample' and query_id = 'xxx' and event_time <= 'yyy' order by event_time)"
///      | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_pos.svg
/// 4. Find deallocations at fixed point of time
/// clickhouse client --allow_introspection_functions=1
///      -q "select arrayJoin(flameGraph(trace, -size, ptr)) from (select * from system.trace_log where trace_type = 'MemorySample' and query_id = 'xxx' and event_time > 'yyy' order by event_time desc)"
///      | ~/dev/FlameGraph/flamegraph.pl --countname=bytes --color=mem > flame_mem_time_point_neg.svg
class AggregateFunctionFlameGraph final : public IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>
{
public:
    explicit AggregateFunctionFlameGraph(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>(argument_types_, {}, createResultType())
    {}

    String getName() const override { return "flameGraph"; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & trace = assert_cast<const ColumnArray &>(*columns[0]);

        const auto & trace_offsets = trace.getOffsets();
        const auto & trace_values = assert_cast<const ColumnUInt64 &>(trace.getData()).getData();
        UInt64 prev_offset = 0;
        if (row_num)
            prev_offset = trace_offsets[row_num - 1];
        UInt64 trace_size = trace_offsets[row_num] - prev_offset;

        Int64 allocated = 1;
        if (argument_types.size() >= 2)
        {
            const auto & sizes = assert_cast<const ColumnInt64 &>(*columns[1]).getData();
            allocated = sizes[row_num];
        }

        UInt64 ptr = 0;
        if (argument_types.size() >= 3)
        {
            const auto & ptrs = assert_cast<const ColumnUInt64 &>(*columns[2]).getData();
            ptr = ptrs[row_num];
        }

        data(place).add(ptr, allocated, trace_values.data() + prev_offset, trace_size, arena);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer &, std::optional<size_t> /* version */) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization for function flameGraph is not implemented.");
    }

    void deserialize(AggregateDataPtr __restrict, ReadBuffer &, std::optional<size_t> /* version */, Arena *) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Deserialization for function flameGraph is not implemented.");
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & array = assert_cast<ColumnArray &>(to);
        auto & str = assert_cast<ColumnString &>(array.getData());

        data(place).dumpFlameGraph(str.getChars(), str.getOffsets(), 0, 0);

        array.getOffsets().push_back(str.size());
    }
};

static void check(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.empty() || argument_types.size() > 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 1 to 3 arguments : trace, [size = 1], [ptr = 0]",
            name);

    auto ptr_type = std::make_shared<DataTypeUInt64>();
    auto trace_type = std::make_shared<DataTypeArray>(ptr_type);
    auto size_type = std::make_shared<DataTypeInt64>();

    if (!argument_types[0]->equals(*trace_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument (trace) for function {} must be Array(UInt64), but it has type {}",
            name, argument_types[0]->getName());

    if (argument_types.size() >= 2 && !argument_types[1]->equals(*size_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument (size) for function {} must be Int64, but it has type {}",
            name, argument_types[1]->getName());

    if (argument_types.size() >= 3 && !argument_types[2]->equals(*ptr_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument (ptr) for function {} must be UInt64, but it has type {}",
            name, argument_types[2]->getName());
}

AggregateFunctionPtr createAggregateFunctionFlameGraph(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
{
    if (!(*settings)[Setting::allow_introspection_functions])
        throw Exception(ErrorCodes::FUNCTION_NOT_ALLOWED,
        "Introspection functions are disabled, because setting 'allow_introspection_functions' is set to 0");

    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types);
}

void registerAggregateFunctionFlameGraph(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("flameGraph", { createAggregateFunctionFlameGraph, properties });
}

}
