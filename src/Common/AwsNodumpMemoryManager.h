#pragma once

#ifdef USE_AWS_S3

#include <Common/JemallocNodumpAllocatorImpl.h>
#include <aws/core/utils/memory/MemorySystemInterface.h>

namespace DB
{

class AwsNodumpMemoryManager : public Aws::Utils::Memory::MemorySystemInterface
{
public:
    void * AllocateMemory(std::size_t blockSize, std::size_t alignment, const char * /*allocationTag*/) override;
    void FreeMemory(void * memoryPtr) override;
    void Begin() override;
    void End() override;
};

}

#endif
