#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
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
        /// Skip first 2 frames which are always the same:
        /// src/Common/StackTrace.cpp
        /// src/Common/MemoryAllocationTracker.cpp
        const size_t offset = 0;

        if (!root.ptr && stack_size > 1)
            root.ptr = stack[1];

        TreeNode * node = &root;
        for (size_t i = offset; i < stack_size; ++i)
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
    struct Allocation
    {
        AggregateFunctionFlameGraphTree::TreeNode * trace;
        UInt64 ptr;
        size_t size;
    };

    struct Deallocation
    {
        UInt64 ptr;
        size_t size;
    };

    using Allocator = AlignedArenaAllocator<alignof(Allocation)>;
    using Allocations = PODArray<Allocation, 32, Allocator>;
    using Deallocations = PODArray<Deallocation, 32, Allocator>;

    Allocations allocations;
    Deallocations deallocations;
    AggregateFunctionFlameGraphTree tree;

    void add(UInt64 ptr, Int64 size, const UInt64 * stack, size_t stack_size, Arena * arena)
    {
        if (size > 0)
        {
            auto * node = tree.find(stack, stack_size, arena);
            allocations.push_back(Allocation{node, ptr, UInt64(size)}, arena);

            while (node)
            {
                node->allocated += size;
                node = node->parent;
            }
        }
        else if (size < 0)
        {
            deallocations.push_back(Deallocation{ptr, UInt64(-size)}, arena);
        }
    }

    void merge(const AggregateFunctionFlameGraphData & other, Arena * arena)
    {
        std::vector<UInt64> trace;
        for (const auto & allocation : other.allocations)
        {
            trace.clear();
            const auto * node = allocation.trace;
            while (node && node->ptr)
            {
                trace.push_back(node->ptr);
                node = node->parent;
            }

            std::reverse(trace.begin(), trace.end());
            add(allocation.ptr, allocation.size, trace.data(), trace.size(), arena);
        }

        deallocations.insert(other.deallocations.begin(), other.deallocations.end(), arena);
    }

    void processDeallocations() const
    {
        std::unordered_map<UInt64, std::list<const Allocation *>> allocations_map;
        for (const auto & allocation : allocations)
            allocations_map[allocation.ptr].push_back(&allocation);

        for (const auto & deallocation : deallocations)
        {
            auto it = allocations_map.find(deallocation.ptr);
            if (it != allocations_map.end())
            {
                for (auto jt = it->second.begin(); jt != it->second.end(); ++jt)
                {
                    if ((*jt)->size == deallocation.size)
                    {
                        auto * node = (*jt)->trace;
                        it->second.erase(jt);

                        while (node)
                        {
                            node->allocated -= deallocation.size;
                            node = node->parent;
                        }

                        break;
                    }
                }
            }
        }
    }

    void write(WriteBuffer & /*buf*/) const
    {

    }

    void read(ReadBuffer & /*buf*/) {}

    void dumpFlameGraph(
        DB::PaddedPODArray<UInt8> & chars,
        DB::PaddedPODArray<UInt64> & offsets,
        size_t max_depth, size_t min_bytes) const
    {
        processDeallocations();
        DB::dumpFlameGraph(tree.dump(max_depth, min_bytes), chars, offsets);
    }
};

class AggregateFunctionFlameGraph final : public IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>
{
private:
    bool dump_tree;
public:
    AggregateFunctionFlameGraph(const DataTypes & argument_types_, bool dump_tree_)
        : IAggregateFunctionDataHelper<AggregateFunctionFlameGraphData, AggregateFunctionFlameGraph>(argument_types_, {})
        , dump_tree(dump_tree_)
    {}

    String getName() const override { return dump_tree ? "allocationsTree" : "allocationsFlameGraph"; }

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

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
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

AggregateFunctionPtr createAggregateFunctionAllocationsFlameGraph(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, false);
}

AggregateFunctionPtr createAggregateFunctionAllocationsTree(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, true);
}

void registerAggregateFunctionFlameGraph(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("allocationsFlameGraph", { createAggregateFunctionAllocationsFlameGraph, properties });
    factory.registerFunction("allocationsTree", { createAggregateFunctionAllocationsTree, properties });
}

}
