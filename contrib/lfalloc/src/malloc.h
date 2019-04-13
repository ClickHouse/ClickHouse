#pragma once

#include <string.h>
#include <stdlib.h>
#include "util/system/compiler.h"

namespace NMalloc {
    volatile inline bool IsAllocatorCorrupted = false;

    static inline void AbortFromCorruptedAllocator() {
        IsAllocatorCorrupted = true;
        abort();
    }

    struct TAllocHeader {
        void* Block;
        size_t AllocSize;
        void Y_FORCE_INLINE Encode(void* block, size_t size, size_t signature) {
            Block = block;
            AllocSize = size | signature;
        }
    };
}
