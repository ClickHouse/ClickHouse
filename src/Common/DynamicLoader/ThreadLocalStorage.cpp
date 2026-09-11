#include <Common/DynamicLoader/ThreadLocalStorage.h>

#include <Common/Exception.h>

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

namespace DynamicLinker
{

namespace
{
    /// One registered module's thread-local template: the bytes to copy into every thread's fresh block.
    struct ThreadLocalModule
    {
        const std::byte * template_data = nullptr;
        size_t template_size = 0;       /// The initialized part (.tdata).
        size_t total_size = 0;          /// Including the zero-filled part (.tbss).
        size_t alignment = 1;
        bool alive = false;
    };

    /// The process-global module registry. Index 0 is unused so a module id is always >= 1.
    /// Consulted only the first time a given thread touches a given module, so a plain mutex is fine.
    std::mutex modules_mutex;
    std::vector<ThreadLocalModule> modules{1};

    /// One thread's dynamic thread vector: block pointers indexed by module id, plus a list to free at exit.
    struct DynamicThreadVector
    {
        std::vector<void *> blocks;
        std::vector<void *> owned_allocations;

        ~DynamicThreadVector()
        {
            for (void * allocation : owned_allocations)
                std::free(allocation);
        }
    };

    DynamicThreadVector & threadVector()
    {
        thread_local DynamicThreadVector vector;
        return vector;
    }

    void * allocateAligned(size_t size, size_t alignment)
    {
        /// aligned_alloc requires a power-of-two alignment of at least sizeof(void*) and a size that is a
        /// multiple of the alignment.
        if (alignment < sizeof(void *))
            alignment = sizeof(void *);
        size_t rounded_size = (size + alignment - 1) & ~(alignment - 1);
        if (rounded_size == 0)
            rounded_size = alignment;
        void * allocation = std::aligned_alloc(alignment, rounded_size);
        if (allocation == nullptr)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Cannot allocate {} bytes for a thread-local storage block", rounded_size);
        return allocation;
    }
}


uint64_t registerThreadLocalModule(std::span<const std::byte> template_bytes, size_t total_size, size_t alignment)
{
    std::lock_guard lock(modules_mutex);
    modules.push_back(ThreadLocalModule{template_bytes.data(), template_bytes.size(), total_size, alignment, true});
    return modules.size() - 1;
}


void unregisterThreadLocalModule(uint64_t module_id)
{
    std::lock_guard lock(modules_mutex);
    if (module_id < modules.size())
        modules[module_id].alive = false;
    /// The id is never reused; blocks already allocated in live threads are freed when those threads exit.
}


void * getThreadLocalAddress(const ThreadLocalStorageIndex & index)
{
    DynamicThreadVector & vector = threadVector();

    if (index.module_id >= vector.blocks.size())
        vector.blocks.resize(index.module_id + 1, nullptr);

    void * block = vector.blocks[index.module_id];
    if (block == nullptr)
    {
        /// First access to this module from this thread: allocate and initialize the block from the template.
        ThreadLocalModule module;
        {
            std::lock_guard lock(modules_mutex);
            if (index.module_id == 0 || index.module_id >= modules.size() || !modules[index.module_id].alive)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Thread-local storage requested for unknown module id {}", index.module_id);
            module = modules[index.module_id];
        }

        block = allocateAligned(module.total_size, module.alignment);
        std::memcpy(block, module.template_data, module.template_size);
        std::memset(static_cast<std::byte *>(block) + module.template_size, 0, module.total_size - module.template_size);

        vector.blocks[index.module_id] = block;
        vector.owned_allocations.push_back(block);
    }

    return static_cast<std::byte *>(block) + index.offset;
}


extern "C" void * clickhouseDynamicLoaderThreadLocalAccessor(ThreadLocalStorageIndex * index)
{
    return getThreadLocalAddress(*index);
}

void * threadLocalStorageAccessor()
{
    return reinterpret_cast<void *>(&clickhouseDynamicLoaderThreadLocalAccessor);
}

}

}
