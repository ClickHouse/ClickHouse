#pragma once

#include <string.h>
#include "util/system/compiler.h"

namespace NMalloc {
    struct TMallocInfo {
        TMallocInfo();

        const char* Name;

        bool (*SetParam)(const char* param, const char* value);
        const char* (*GetParam)(const char* param);

        bool (*CheckParam)(const char* param, bool defaultValue);
    };

    extern volatile bool IsAllocatorCorrupted;
    void AbortFromCorruptedAllocator();

    // this function should be implemented by malloc implementations
    TMallocInfo MallocInfo();

    struct TAllocHeader {
        void* Block;
        size_t AllocSize;
        void Y_FORCE_INLINE Encode(void* block, size_t size, size_t signature) {
            Block = block;
            AllocSize = size | signature;
        }
    };
}
