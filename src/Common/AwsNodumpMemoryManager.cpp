#include <Common/AwsNodumpMemoryManager.h>
#include <Common/JemallocNodumpAllocatorImpl.h>

#if USE_AWS_S3
#if USE_JEMALLOC

namespace DB
{
    void * AwsNodumpMemoryManager::AllocateMemory(std::size_t block_size, std::size_t alignment, const char * /*allocation_tag*/)
    {
        return DB::JemallocNodumpAllocatorImpl::instance().allocate(block_size, alignment);
    }

    void AwsNodumpMemoryManager::FreeMemory(void * memory_ptr)
    {
        DB::JemallocNodumpAllocatorImpl::instance().deallocate(memory_ptr);
    }

    void AwsNodumpMemoryManager::Begin() {}

    void AwsNodumpMemoryManager::End() {}
}

#endif
#endif
