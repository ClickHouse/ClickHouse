#include <map>
#include <mutex>
#include <base/defines.h>
#include <sys/mman.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/StackTrace.h>
#include <Common/PODArray.h>

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
        while(data)
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

    void dump(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, DB::PaddedPODArray<UInt64> & bytes) const
    {
        std::vector<UInt64> frame;
        std::vector<ListNode *> nodes;

        nodes.push_back(root.children);
        append(values, offsets, frame);
        bytes.push_back(root.allocated);
        while (!nodes.empty())
        {
            if (nodes.back() == nullptr)
            {
                nodes.pop_back();
                if (!frame.empty())
                    frame.pop_back();
                continue;
            }

            TreeNode * current = nodes.back()->child;
            nodes.back() = nodes.back()->next;

            nodes.push_back(current->children);
            frame.push_back(reinterpret_cast<intptr_t>(current->ptr));
            append(values, offsets, frame);
            bytes.push_back(current->allocated);
        }
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

    void dump(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, DB::PaddedPODArray<UInt64> & bytes) const
    {
        tree.dump(values, offsets, bytes);
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

void enable_alocation_tracker(bool enable)
{
    /// Just in case
    if (recursive_call_flag)
        return;

    /// If clearing causes deallocations
    RecursiveCallGuard guard(recursive_call_flag);

    auto & data = getTrackerData();
    std::lock_guard lock(data.mutex);

    if (!enable)
        data.tracker.clear();

    data.is_enabled = enable;
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

void dump_allocations(DB::PaddedPODArray<UInt64> & values, DB::PaddedPODArray<UInt64> & offsets, DB::PaddedPODArray<UInt64> & bytes)
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
            data.tracker.dump(values, offsets, bytes);
        }
    }
}

}
