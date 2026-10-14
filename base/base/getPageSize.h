#pragma once

#include <base/defines.h>
#include <base/types.h>

extern Int64 staticPageSize;

/// Get memory page size
Int64 inline ALWAYS_INLINE getPageSize()
{
    return staticPageSize;
}
