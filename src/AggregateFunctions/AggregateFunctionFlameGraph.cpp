#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SymbolIndex.h>
#include <Common/ArenaAllocator.h>
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
    auto symbol_index_ptr = DB::SymbolIndex::instance();
    const DB::SymbolIndex & symbol_index = *symbol_index_ptr;
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

    AggregateFunctionFlameGraphTree tree;
    Entries entries;
    Entry * free_list = nullptr;

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
        else
        {
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

            if(current->children)
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

class AggregateFunctionFlameGraph final : public IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>
{
public:
    explicit AggregateFunctionFlameGraph(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>(argument_types_, {})
    {}

    String getName() const override { return "flameGraph"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto * trace = typeid_cast<const ColumnArray *>(columns[0]);
        const auto & sizes = typeid_cast<const ColumnInt64 *>(columns[1])->getData();
        const auto & ptrs = typeid_cast<const ColumnUInt64 *>(columns[2])->getData();

        const auto & trace_offsets = trace->getOffsets();
        const auto & trace_values = typeid_cast<const ColumnUInt64 *>(&trace->getData())->getData();
        UInt64 prev_offset = 0;
        if (row_num)
            prev_offset = trace_offsets[row_num - 1];
        UInt64 trace_size = trace_offsets[row_num] - prev_offset;

        this->data(place).add(ptrs[row_num], sizes[row_num], trace_values.data() + prev_offset, trace_size, arena);
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
        this->data(place).merge(this->data(rhs), arena);
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

        this->data(place).dumpFlameGraph(str.getChars(), str.getOffsets(), 0, 0);

        array.getOffsets().push_back(str.size());
    }
};

static void check(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 3 arguments : trace, size, ptr",
            name);

    auto ptr_type = std::make_shared<DataTypeUInt64>();
    auto trace_type = std::make_shared<DataTypeArray>(ptr_type);
    auto size_type = std::make_shared<DataTypeInt64>();

    if (!argument_types[0]->equals(*trace_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument (trace) for function {} must be Array(UInt64), but it has type {}",
            name, argument_types[0]->getName());

    if (!argument_types[1]->equals(*size_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument (size) for function {} must be Int64, but it has type {}",
            name, argument_types[1]->getName());

    if (!argument_types[2]->equals(*ptr_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument (ptr) for function {} must be UInt64, but it has type {}",
            name, argument_types[2]->getName());
}

AggregateFunctionPtr createAggregateFunctionFlameGraph(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types);
}

void registerAggregateFunctionFlameGraph(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("flameGraph", { createAggregateFunctionFlameGraph, properties });
}

}
