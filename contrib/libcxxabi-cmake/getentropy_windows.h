#pragma once

/// `cxa_thread_atexit.cpp` initializes the secret that mangles the `thread_local` destructor pointers
/// from `getentropy` everywhere but on Linux. Windows has no `getentropy`. Its system random number
/// generator is `RtlGenRandom`, which `advapi32` exports under the name `SystemFunction036`.

#include <stddef.h>

extern "C" __declspec(dllimport) unsigned char __stdcall SystemFunction036(void * buffer, unsigned long size);

static inline int getentropy(void * buffer, size_t size)
{
    return SystemFunction036(buffer, static_cast<unsigned long>(size)) ? 0 : -1;
}
