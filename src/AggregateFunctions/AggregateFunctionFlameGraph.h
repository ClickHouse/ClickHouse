#pragma once
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <Common/ArenaAllocator.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
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

    Traces dump(size_t max_depth, size_t max_bytes) const
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

            bool enough_bytes = current->allocated >= max_bytes;
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

    struct DumpTree
    {
        struct Node
        {
            uintptr_t id{};
            UInt64 ptr{};
            size_t allocated{};
        };

        struct Edge
        {
            uintptr_t from{};
            uintptr_t to{};
        };

        using Nodes = std::vector<Node>;
        using Edges = std::vector<Edge>;

        Nodes nodes;
        Edges edges;
    };

    DumpTree dumpAllocationsTree(size_t max_depth, size_t max_bytes) const
    {
        max_bytes = std::max<size_t>(max_bytes, 1);

        DumpTree tree;
        std::vector<ListNode *> nodes;

        nodes.push_back(root.children);
        tree.nodes.emplace_back(DumpTree::Node{uintptr_t(&root), root.ptr, root.allocated});

        while (!nodes.empty())
        {
            if (nodes.back() == nullptr)
            {
                nodes.pop_back();
                continue;
            }

            TreeNode * current = nodes.back()->child;
            nodes.back() = nodes.back()->next;

            bool enough_bytes = current->allocated >= max_bytes;
            bool enough_depth = max_depth == 0 || nodes.size() < max_depth;

            if (enough_bytes)
            {
                tree.nodes.emplace_back(DumpTree::Node{uintptr_t(current), current->ptr, current->allocated});
                tree.edges.emplace_back(DumpTree::Edge{uintptr_t(current->parent), uintptr_t(current)});

                if (enough_depth)
                    nodes.push_back(current->children);
            }
        }

        return tree;
    }
};

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
                    }
                }
            }
        }
    }

    void write(WriteBuffer & /*buf*/) const {}
    void read(ReadBuffer & /*buf*/) {}


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

    #if defined(__ELF__) && !defined(OS_FREEBSD)

    static void writeWrappedString(std::string_view ref, size_t wrap_size, DB::WriteBuffer & out)
    {
        size_t start = 0;
        while (start + wrap_size < ref.size())
        {
            writeString(ref.substr(start, wrap_size), out);
            writeString("\n", out);
            start += wrap_size;
        }

        writeString(ref.substr(start, std::string_view::npos), out);
    }

    static void addressToLine(
        DB::WriteBuffer & out,
        const DB::SymbolIndex & symbol_index,
        std::unordered_map<std::string, DB::Dwarf> & dwarfs,
        const void * addr)
    {
        if (const auto * symbol = symbol_index.findSymbol(addr))
        {
            writeWrappedString(demangle(symbol->name), 100, out);
            writeString("\n", out);
        }
        if (const auto * object = symbol_index.findObject(addr))
        {
            auto dwarf_it = dwarfs.try_emplace(object->name, object->elf).first;
            if (!std::filesystem::exists(object->name))
                return;

            DB::Dwarf::LocationInfo location;
            std::vector<DB::Dwarf::SymbolizedFrame> frames; // NOTE: not used in FAST mode.
            if (dwarf_it->second.findAddress(uintptr_t(addr) - uintptr_t(object->address_begin), location, DB::Dwarf::LocationInfoMode::FAST, frames))
            {
                writeString(location.file.toString(), out);
                writeChar(':', out);
                writeIntText(location.line, out);
            }
            else
                writeString(object->name, out);
        }
    }

    #endif

    static void dumpNode(
        const AggregateFunctionFlameGraphTree::DumpTree::Node & node,
        std::unordered_map<uintptr_t, size_t> & mapping,
    #if defined(__ELF__) && !defined(OS_FREEBSD)
        std::unordered_map<std::string, DB::Dwarf> & dwarfs,
        const DB::SymbolIndex & symbol_index,
    #endif
        DB::WriteBuffer & out)
    {
        size_t id = mapping.emplace(node.id, mapping.size()).first->second;

        out << "    n" << id << "[label=\"Allocated: "
            << formatReadableSizeWithBinarySuffix(node.allocated) << " (" << node.allocated << ")\n";

    #if defined(__ELF__) && !defined(OS_FREEBSD)
        addressToLine(out, symbol_index, dwarfs, reinterpret_cast<void *>(node.ptr));
    #else
        DB::writePointerHex(node.ptr, out);
    #endif

        out << "\"];\n";
    }

    void dumpAllocationsTree(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes) const
    {
        processDeallocations();
        auto alloc_tree = tree.dumpAllocationsTree(max_depth, max_bytes);

        DB::WriteBufferFromOwnString out;
        std::unordered_map<uintptr_t, size_t> mapping;

    #if defined(__ELF__) && !defined(OS_FREEBSD)
        std::unordered_map<std::string, DB::Dwarf> dwarfs;
        auto symbol_index_ptr = DB::SymbolIndex::instance();
        const DB::SymbolIndex & symbol_index = *symbol_index_ptr;
    #endif

        out << "digraph\n{\n";
        out << "  rankdir=\"LR\";\n";
        out << "  { node [shape = rect]\n";

        for (const auto & node : alloc_tree.nodes)
    #if defined(__ELF__) && !defined(OS_FREEBSD)
            dumpNode(node, mapping, dwarfs, symbol_index, out);
    #else
            dumpNode(node, mapping, out);
    #endif
        out << "  }\n";

        for (const auto & edge : alloc_tree.edges)
            out << "  n" << mapping[edge.from]
                << " -> n" << mapping[edge.to] << ";\n";

        out << "}\n";

        fillColumn(chars, offsets, out.str());
    }

    void dumpAllocationsFlamegraph(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes) const
    {
        processDeallocations();
        auto traces = tree.dump(max_depth, max_bytes);

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

        if (dump_tree)
            this->data(place).dumpAllocationsTree(str.getChars(), str.getOffsets(), 0, 0);
        else
            this->data(place).dumpAllocationsFlamegraph(str.getChars(), str.getOffsets(), 0, 0);

        array.getOffsets().push_back(str.size());
    }
};

}
