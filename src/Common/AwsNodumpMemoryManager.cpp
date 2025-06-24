#ifdef USE_AWS_S3

#include <Common/AwsNodumpMemoryManager.h>

namespace DB
{
    void * AwsNodumpMemoryManager::AllocateMemory(std::size_t blockSize, std::size_t alignment, const char * /*allocationTag*/)
    {
        return DB::JemallocNodumpAllocatorImpl::instance().allocate(blockSize, alignment);
    }

    void AwsNodumpMemoryManager::FreeMemory(void * memoryPtr)
    {
        DB::JemallocNodumpAllocatorImpl::instance().deallocate(memoryPtr);
    }

    void AwsNodumpMemoryManager::Begin() {}

    void AwsNodumpMemoryManager::End() {}
}

#endif
