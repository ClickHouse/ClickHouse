#include <filesystem>
#include <map>
#include <mutex>
#include <base/defines.h>
#include <sys/mman.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/MemoryAllocationTracker.h>
#include <Common/formatReadable.h>
#include <Common/SymbolIndex.h>
#include <Common/Dwarf.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

namespace MemoryAllocationTracker
{

struct StackAllocatorMemory
{
    char * data = nullptr;
    constexpr static size_t size = 1024 * 1024;
    size_t offset = size;

    void * allocate(size_t num_bytes)
    {
        if (unlikely(offset + num_bytes > size))
        {
            void * vp = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (MAP_FAILED == vp)
                DB::throwFromErrno(fmt::format("StackAllocatorWithFreeList: Cannot mmap {}.", ReadableSize(size)), DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY);

            *static_cast<char **>(vp) = data;
            data = static_cast<char *>(vp);
            offset = sizeof(char *);
        }

        void * res = data + offset;
        offset += num_bytes;
        return res;
    }

    ~StackAllocatorMemory()
    {
        while (data)
        {
            void * to_delete = static_cast<void *>(data);
            data = *static_cast<char **>(to_delete);

            ::munmap(to_delete, size);
        }
    }
};

template<typename T>
struct StackAllocatorFreeList
{
    static_assert(sizeof(T) >= sizeof(void*));
    T * top = nullptr;

    void put(T * ptr)
    {
        *reinterpret_cast<T **>(ptr) = top;
        top = ptr;
    }

    T * pop()
    {
        T * res = top;

        if (top)
            top = *reinterpret_cast<T **>(top);

        return res;
    }
};


template<typename T>
struct StackAllocatorWithFreeList
{
    using value_type = T;

    explicit StackAllocatorWithFreeList(StackAllocatorMemory & memory_)
        : memory(memory_)
    {
    }

    template <typename U>
    explicit StackAllocatorWithFreeList(const StackAllocatorWithFreeList<U> & other) noexcept
        : memory(other.memory)
    {
    }

    value_type * allocate(std::size_t n)
    {
        if (n != 1)
            std::terminate();

        if (value_type * entry = free_list.pop())
            return entry;

        return static_cast<value_type *>(memory.allocate(sizeof(value_type)));
    }

    void deallocate(value_type * ptr, std::size_t n)
    {
        if (n != 1)
            std::terminate();

        free_list.put(ptr);
    }

    template< class U > struct rebind // NOLINT(readability-identifier-naming)
    {
        using other = StackAllocatorWithFreeList<U>;
    };

    StackAllocatorMemory & memory;
    StackAllocatorFreeList<T> free_list;
};

struct Tree
{
    struct ListNode;

    struct TreeNode
    {
        TreeNode * parent;
        ListNode * children;
        void * ptr;
        size_t allocated;
    };

    struct ListNode
    {
        ListNode * next;
        TreeNode * child;
    };

    StackAllocatorMemory & allocator;
    TreeNode root;

    explicit Tree(StackAllocatorMemory & allocator_) : allocator(allocator_) {}


    ListNode * createChild(TreeNode * parent, void * ptr)
    {

        ListNode * list_node = static_cast<ListNode *>(allocator.allocate(sizeof(ListNode)));
        TreeNode * tree_node = static_cast<TreeNode *>(allocator.allocate(sizeof(TreeNode)));

        list_node->child = tree_node;
        list_node->next = nullptr;

        tree_node->parent =parent;
        tree_node->children = nullptr;
        tree_node->ptr = ptr;
        tree_node->allocated = 0;

        return list_node;
    }

    TreeNode * find(const StackTrace & trace)
    {
        const auto & stack = trace.getFramePointers();
        size_t stack_size = trace.getSize();
        /// Skip first 2 frames which are always the same:
        /// src/Common/StackTrace.cpp
        /// src/Common/MemoryAllocationTracker.cpp
        const size_t offset = 2;

        if (!root.ptr && stack_size > 1)
            root.ptr = stack[1];

        TreeNode * node = &root;
        for (size_t i = offset; i < stack_size; ++i)
        {
            auto * ptr = stack[i];
            if (ptr == nullptr)
                break;

            if (!node->children)
            {
                node->children = createChild(node, ptr);
                node = node->children->child;
            }
            else
            {
                ListNode * list = node->children;
                while (list->child->ptr != ptr && list->next)
                    list = list->next;

                if (list->child->ptr != ptr)
                {
                    list->next = createChild(node, ptr);
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
                frames.push_back(reinterpret_cast<intptr_t>(current->ptr));
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

struct AllocationTracker
{
    AllocationTracker() : allocator(memory), tree(memory), allocations(allocator)
    {
    }

    void add(void * ptr, const StackTrace & trace, size_t size)
    {
        Tree::TreeNode * node = tree.find(trace);
        allocations.insert({ptr, node});

        while (node)
        {
            node->allocated += size;
            node = node->parent;
        }
    }

    void release(void * ptr, size_t size)
    {
        auto it = allocations.find(ptr);
        if (it == allocations.end())
            return;

        Tree::TreeNode * node = it->second;
        allocations.erase(it);

        while (node)
        {
            node->allocated -= size;
            node = node->parent;
        }
    }

    void clear()
    {
        for (auto [_, node] : allocations)
        {
            while (node)
            {
                node->allocated = 0;
                node = node->parent;
            }
        }

        allocations.clear();
    }

    Traces dump(size_t max_depth, size_t max_bytes) const
    {
        return tree.dump(max_depth, max_bytes);
    }

    DumpTree dumpAllocationsTree(size_t max_depth, size_t max_bytes) const
    {
        return tree.dumpAllocationsTree(max_depth, max_bytes);
    }

    using value_type = std::pair<void * const, Tree::TreeNode *>;

    StackAllocatorMemory memory;
    StackAllocatorWithFreeList<value_type> allocator;

    Tree tree;
    std::map<void *, Tree::TreeNode *, std::less<>, StackAllocatorWithFreeList<value_type>> allocations;
};

struct AllocationTrackerData
{
    AllocationTracker tracker;
    std::atomic_bool is_enabled = false;
    std::mutex mutex;
};

static AllocationTrackerData & getTrackerData()
{
    static AllocationTrackerData tracker;
    return tracker;
}

thread_local bool recursive_call_flag = false;

struct RecursiveCallGuard
{
    explicit RecursiveCallGuard(bool & flag_) : flag(flag_)
    {
        flag = true;
    }

    ~RecursiveCallGuard()
    {
        flag = false;
    }

    bool & flag;
};

void enable_allocation_tracker()
{
    /// Just in case
    if (recursive_call_flag)
        return;

    /// If clearing causes deallocations
    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    std::lock_guard lock(data.mutex);
    data.is_enabled = true;
}

void disable_allocation_tracker()
{
    /// Just in case
    if (recursive_call_flag)
        return;

    /// If clearing causes deallocations
    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    std::lock_guard lock(data.mutex);

    data.tracker.clear();
    data.is_enabled = false;
}

void track_alloc(void * ptr, std::size_t size)
{
    if (recursive_call_flag)
        return;

    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    if (data.is_enabled)
    {
        std::lock_guard lock(data.mutex);
        if (data.is_enabled)
        {
            StackTrace trace;
            data.tracker.add(ptr, trace, size);
        }
    }
}

void track_free(void * ptr, std::size_t size)
{
    if (recursive_call_flag)
        return;

    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    if (data.is_enabled)
    {
        std::lock_guard lock(data.mutex);
        if (data.is_enabled)
        {
            data.tracker.release(ptr, size);
        }
    }
}

Traces dump_allocations(size_t max_depth, size_t max_bytes)
{
    if (recursive_call_flag)
        return {};

    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    if (data.is_enabled)
    {
        std::lock_guard lock(data.mutex);
        if (data.is_enabled)
            return data.tracker.dump(max_depth, max_bytes);
    }

    return {};
}

DumpTree dump_allocations_tree(size_t max_depth, size_t max_bytes)
{
    if (recursive_call_flag)
        return {};

    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    if (data.is_enabled)
    {
        std::lock_guard lock(data.mutex);
        if (data.is_enabled)
            return data.tracker.dumpAllocationsTree(max_depth, max_bytes);
    }

    return {};
}


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

#if defined(__ELF__) && !defined(OS_FREEBSD)

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
    const DumpTree::Node & node,
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
    addressToLine(out, symbol_index, dwarfs, node.ptr);
#else
    DB::writePointerHex(node.ptr, out);
#endif

    out << "\"];\n";
}

void dump_allocations_tree(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes)
{
    auto tree = dump_allocations_tree(max_depth, max_bytes);

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

    for (const auto & node : tree.nodes)
#if defined(__ELF__) && !defined(OS_FREEBSD)
        dumpNode(node, mapping, dwarfs, symbol_index, out);
#else
        dumpNode(node, mapping, out);
#endif
    out << "  }\n";

    for (const auto & edge : tree.edges)
        out << "  n" << mapping[edge.from]
            << " -> n" << mapping[edge.to] << ";\n";

    out << "}\n";

    fillColumn(chars, offsets, out.str());
}

void dump_allocations_flamegraph(DB::PaddedPODArray<UInt8> & chars, DB::PaddedPODArray<UInt64> & offsets, size_t max_depth, size_t max_bytes)
{
    auto traces = dump_allocations(max_depth, max_bytes);

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

}
