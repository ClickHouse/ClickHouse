#pragma once

#include "config.h"

#if USE_AWS_S3
#if USE_JEMALLOC

#include <aws/core/utils/memory/MemorySystemInterface.h>

namespace DB
{

class AwsNodumpMemoryManager : public Aws::Utils::Memory::MemorySystemInterface
{
public:
    void * AllocateMemory(std::size_t block_size, std::size_t alignment, const char * /*allocation_tag*/) override;
    void FreeMemory(void * memory_ptr) override;
    void Begin() override;
    void End() override;
};

}

#endif
#endif
