#include <stdlib.h>

#include "malloc.h"

namespace {
    static bool SetEmptyParam(const char*, const char*) {
        return false;
    }

    static const char* GetEmptyParam(const char*) {
        return nullptr;
    }

    static bool CheckEmptyParam(const char*, bool defaultValue) {
        return defaultValue;
    }
}

namespace NMalloc {
    volatile bool IsAllocatorCorrupted = false;

    TMallocInfo::TMallocInfo()
        : Name()
        , SetParam(SetEmptyParam)
        , GetParam(GetEmptyParam)
        , CheckParam(CheckEmptyParam)
    {
    }

    void AbortFromCorruptedAllocator() {
        IsAllocatorCorrupted = true;
        abort();
    }
}
