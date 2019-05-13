#pragma once

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

#include "lfmalloc.h"

#include "util/system/compiler.h"
#include "util/system/types.h"
#include <random>

#ifdef _MSC_VER
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif
#ifdef _M_X64
#define _64_
#endif
#include <intrin.h>
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#pragma intrinsic(_InterlockedCompareExchange)
#pragma intrinsic(_InterlockedExchangeAdd)

#include <new>
#include <assert.h>
#include <errno.h>

#define PERTHREAD __declspec(thread)
#define _win_
#define Y_FORCE_INLINE __forceinline

using TAtomic = volatile long;

static inline long AtomicAdd(TAtomic& a, long b) {
    return _InterlockedExchangeAdd(&a, b) + b;
}

static inline long AtomicSub(TAtomic& a, long b) {
    return AtomicAdd(a, -b);
}

#define Y_ASSERT_NOBT(x) ((void)0)

#else

#include "util/system/defaults.h"
#include "util/system/atomic.h"
#include <cassert>

#if !defined(NDEBUG) && !defined(__GCCXML__)
#define Y_ASSERT_NOBT(a)                       \
    do {                                       \
        if (Y_UNLIKELY(!(a))) {                \
            assert(false && (a));              \
        }                                      \
    } while (0)
#else
#define Y_ASSERT_NOBT(a)                       \
    do {                                       \
        if (false) {                           \
            bool __xxx = static_cast<bool>(a); \
            Y_UNUSED(__xxx);                   \
        }                                      \
    } while (0)
#endif

#include <pthread.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <memory.h>
#include <new>
#include <errno.h>

#if defined(_linux_)
#if !defined(MADV_HUGEPAGE)
#define MADV_HUGEPAGE 14
#endif
#if !defined(MAP_HUGETLB)
#define MAP_HUGETLB 0x40000
#endif
#endif

#define PERTHREAD __thread

#endif

#ifndef _darwin_

#ifndef Y_ARRAY_SIZE
#define Y_ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))
#endif

#ifndef NDEBUG
#define DBG_FILL_MEMORY
static bool FillMemoryOnAllocation = true;
#endif

static bool TransparentHugePages = false; // force MADV_HUGEPAGE for large allocs
static bool MapHugeTLB = false;           // force MAP_HUGETLB for small allocs
static bool EnableDefrag = true;

// Buffers that are larger than this size will not be filled with 0xcf
#ifndef DBG_FILL_MAX_SIZE
#define DBG_FILL_MAX_SIZE 0x01000000000000ULL
#endif

template <class T>
inline T* DoCas(T* volatile* target, T* exchange, T* compare) {
#if defined(_linux_)
    return __sync_val_compare_and_swap(target, compare, exchange);
#elif defined(_WIN32)
#ifdef _64_
    return (T*)_InterlockedCompareExchange64((__int64*)target, (__int64)exchange, (__int64)compare);
#else
    //return (T*)InterlockedCompareExchangePointer(targetVoidP, exchange, compare);
    return (T*)_InterlockedCompareExchange((LONG*)target, (LONG)exchange, (LONG)compare);
#endif
#elif defined(__i386) || defined(__x86_64__)
    union {
        T* volatile* NP;
        void* volatile* VoidP;
    } gccSucks;
    gccSucks.NP = target;
    void* volatile* targetVoidP = gccSucks.VoidP;

    __asm__ __volatile__(
        "lock\n\t"
        "cmpxchg %2,%0\n\t"
        : "+m"(*(targetVoidP)), "+a"(compare)
        : "r"(exchange)
        : "cc", "memory");
    return compare;
#else
#error inline_cas not defined for this platform
#endif
}

#ifdef _64_
const uintptr_t N_MAX_WORKSET_SIZE = 0x100000000ll * 200;
const uintptr_t N_HUGE_AREA_FINISH = 0x700000000000ll;
#ifndef _freebsd_
const uintptr_t LINUX_MMAP_AREA_START = 0x100000000ll;
static uintptr_t volatile linuxAllocPointer = LINUX_MMAP_AREA_START;
static uintptr_t volatile linuxAllocPointerHuge = LINUX_MMAP_AREA_START + N_MAX_WORKSET_SIZE;
#endif
#else
const uintptr_t N_MAX_WORKSET_SIZE = 0xffffffff;
#endif
#define ALLOC_START ((char*)0)

const size_t N_CHUNK_SIZE = 1024 * 1024;
const size_t N_CHUNKS = N_MAX_WORKSET_SIZE / N_CHUNK_SIZE;
const size_t N_LARGE_ALLOC_SIZE = N_CHUNK_SIZE * 128;

// map size idx to size in bytes
#ifdef LFALLOC_YT
const int N_SIZES = 27;
#else
const int N_SIZES = 25;
#endif
const int nSizeIdxToSize[N_SIZES] = {
    -1,
#if defined(_64_)
    16, 16, 32, 32, 48, 64, 96, 128,
#else
    8,
    16,
    24,
    32,
    48,
    64,
    96,
    128,
#endif
    192, 256, 384, 512, 768, 1024, 1536, 2048,
    3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768,
#ifdef LFALLOC_YT
    49152, 65536
#endif
};
#ifdef LFALLOC_YT
const size_t N_MAX_FAST_SIZE = 65536;
#else
const size_t N_MAX_FAST_SIZE = 32768;
#endif
const unsigned char size2idxArr1[64 + 1] = {
    1,
#if defined(_64_)
    2, 2, 4, 4, // 16, 16, 32, 32
#else
    1, 2, 3, 4, // 8, 16, 24, 32
#endif
    5, 5, 6, 6,                                                     // 48, 64
    7, 7, 7, 7, 8, 8, 8, 8,                                         // 96, 128
    9, 9, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 10, 10, 10,         // 192, 256
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, // 384
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12  // 512
};
#ifdef LFALLOC_YT
const unsigned char size2idxArr2[256] = {
#else
const unsigned char size2idxArr2[128] = {
#endif
    12, 12, 13, 14,                                                 // 512, 512, 768, 1024
    15, 15, 16, 16,                                                 // 1536, 2048
    17, 17, 17, 17, 18, 18, 18, 18,                                 // 3072, 4096
    19, 19, 19, 19, 19, 19, 19, 19, 20, 20, 20, 20, 20, 20, 20, 20, // 6144, 8192
    21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, 21, // 12288
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, // 16384
    23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23,
    23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, // 24576
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, // 32768
#ifdef LFALLOC_YT
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25,
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, // 49152
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26,
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26,
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26,
    26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, 26, // 65536
#endif
};

// map entry number to size idx
// special size idx's: 0 = not used, -1 = mem locked, but not allocated
static volatile char chunkSizeIdx[N_CHUNKS];
const int FREE_CHUNK_ARR_BUF = 0x20000; // this is effectively 128G of free memory (with 1M chunks), should not be exhausted actually
static volatile uintptr_t freeChunkArr[FREE_CHUNK_ARR_BUF];
static volatile int freeChunkCount;

static void AddFreeChunk(uintptr_t chunkId) {
    chunkSizeIdx[chunkId] = -1;
    if (Y_UNLIKELY(freeChunkCount == FREE_CHUNK_ARR_BUF))
        NMalloc::AbortFromCorruptedAllocator(); // free chunks arrray overflowed
    freeChunkArr[freeChunkCount++] = chunkId;
}

static bool GetFreeChunk(uintptr_t* res) {
    if (freeChunkCount == 0) {
        *res = 0;
        return false;
    }
    *res = freeChunkArr[--freeChunkCount];
    return true;
}

//////////////////////////////////////////////////////////////////////////
enum ELFAllocCounter {
    CT_USER_ALLOC,     // accumulated size requested by user code
    CT_MMAP,           // accumulated mmapped size
    CT_MMAP_CNT,       // number of mmapped regions
    CT_MUNMAP,         // accumulated unmmapped size
    CT_MUNMAP_CNT,     // number of munmaped regions
    CT_SYSTEM_ALLOC,   // accumulated allocated size for internal lfalloc needs
    CT_SYSTEM_FREE,    // accumulated deallocated size for internal lfalloc needs
    CT_SMALL_ALLOC,    // accumulated allocated size for fixed-size blocks
    CT_SMALL_FREE,     // accumulated deallocated size for fixed-size blocks
    CT_LARGE_ALLOC,    // accumulated allocated size for large blocks
    CT_LARGE_FREE,     // accumulated deallocated size for large blocks
    CT_SLOW_ALLOC_CNT, // number of slow (not LF) allocations
    CT_DEGRAGMENT_CNT, // number of memory defragmentations
    CT_MAX
};

static Y_FORCE_INLINE void IncrementCounter(ELFAllocCounter counter, size_t value);

//////////////////////////////////////////////////////////////////////////
enum EMMapMode {
    MM_NORMAL, // memory for small allocs
    MM_HUGE    // memory for large allocs
};

#ifndef _MSC_VER
inline void VerifyMmapResult(void* result) {
    if (Y_UNLIKELY(result == MAP_FAILED))
        NMalloc::AbortFromCorruptedAllocator(); // negative size requested? or just out of mem
}
#endif

#if !defined(_MSC_VER) && !defined(_freebsd_) && defined(_64_)
static char* AllocWithMMapLinuxImpl(uintptr_t sz, EMMapMode mode) {
    char* volatile* areaPtr;
    char* areaStart;
    uintptr_t areaFinish;

    int mapProt = PROT_READ | PROT_WRITE;
    int mapFlags = MAP_PRIVATE | MAP_ANON;

    if (mode == MM_HUGE) {
        areaPtr = reinterpret_cast<char* volatile*>(&linuxAllocPointerHuge);
        areaStart = reinterpret_cast<char*>(LINUX_MMAP_AREA_START + N_MAX_WORKSET_SIZE);
        areaFinish = N_HUGE_AREA_FINISH;
    } else {
        areaPtr = reinterpret_cast<char* volatile*>(&linuxAllocPointer);
        areaStart = reinterpret_cast<char*>(LINUX_MMAP_AREA_START);
        areaFinish = N_MAX_WORKSET_SIZE;

        if (MapHugeTLB) {
            mapFlags |= MAP_HUGETLB;
        }
    }

    bool wrapped = false;
    for (;;) {
        char* prevAllocPtr = *areaPtr;
        char* nextAllocPtr = prevAllocPtr + sz;
        if (uintptr_t(nextAllocPtr - (char*)nullptr) >= areaFinish) {
            if (Y_UNLIKELY(wrapped)) {
                // virtual memory is over fragmented
                NMalloc::AbortFromCorruptedAllocator();
            }
            // wrap after all area is used
            DoCas(areaPtr, areaStart, prevAllocPtr);
            wrapped = true;
            continue;
        }

        if (DoCas(areaPtr, nextAllocPtr, prevAllocPtr) != prevAllocPtr)
            continue;

        char* largeBlock = (char*)mmap(prevAllocPtr, sz, mapProt, mapFlags, -1, 0);
        VerifyMmapResult(largeBlock);
        if (largeBlock == prevAllocPtr)
            return largeBlock;
        if (largeBlock)
            munmap(largeBlock, sz);

        if (sz < 0x80000) {
            // skip utilized area with big steps
            DoCas(areaPtr, nextAllocPtr + 0x10 * 0x10000, nextAllocPtr);
        }
    }
}
#endif

static char* AllocWithMMap(uintptr_t sz, EMMapMode mode) {
    (void)mode;
#ifdef _MSC_VER
    char* largeBlock = (char*)VirtualAlloc(0, sz, MEM_RESERVE, PAGE_READWRITE);
    if (Y_UNLIKELY(largeBlock == nullptr))
        NMalloc::AbortFromCorruptedAllocator(); // out of memory
    if (Y_UNLIKELY(uintptr_t(((char*)largeBlock - ALLOC_START) + sz) >= N_MAX_WORKSET_SIZE))
        NMalloc::AbortFromCorruptedAllocator(); // out of working set, something has broken
#else
#if defined(_freebsd_) || !defined(_64_) || defined(USE_LFALLOC_RANDOM_HINT)
    uintptr_t areaStart;
    uintptr_t areaFinish;
    if (mode == MM_HUGE) {
        areaStart = LINUX_MMAP_AREA_START + N_MAX_WORKSET_SIZE;
        areaFinish = N_HUGE_AREA_FINISH;
    } else {
        areaStart = LINUX_MMAP_AREA_START;
        areaFinish = N_MAX_WORKSET_SIZE;
    }
#if defined(USE_LFALLOC_RANDOM_HINT)
    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<intptr_t> distr(areaStart, areaFinish - sz - 1);
    char* largeBlock;
    static constexpr size_t MaxAttempts = 100;
    size_t attempt = 0;
    do
    {
        largeBlock = (char*)mmap(reinterpret_cast<void*>(distr(generator)), sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
        ++attempt;
    } while (uintptr_t(((char*)largeBlock - ALLOC_START) + sz) >= areaFinish && attempt < MaxAttempts && munmap(largeBlock, sz) == 0);
#else
    char* largeBlock = (char*)mmap(0, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
#endif
    VerifyMmapResult(largeBlock);
    if (Y_UNLIKELY(uintptr_t(((char*)largeBlock - ALLOC_START) + sz) >= areaFinish))
        NMalloc::AbortFromCorruptedAllocator(); // out of working set, something has broken
#else
    char* largeBlock = AllocWithMMapLinuxImpl(sz, mode);
    if (TransparentHugePages) {
        madvise(largeBlock, sz, MADV_HUGEPAGE);
    }
#endif
#endif
    Y_ASSERT_NOBT(largeBlock);
    IncrementCounter(CT_MMAP, sz);
    IncrementCounter(CT_MMAP_CNT, 1);
    return largeBlock;
}

enum class ELarge : ui8 {
    Free = 0,   // block in free cache
    Alloc = 1,  // block is allocated
    Gone = 2,   // block was unmapped
};

struct TLargeBlk {

    static TLargeBlk* As(void *raw) {
        return reinterpret_cast<TLargeBlk*>((char*)raw - 4096ll);
    }

    static const TLargeBlk* As(const void *raw) {
        return reinterpret_cast<const TLargeBlk*>((const char*)raw - 4096ll);
    }

    void SetSize(size_t bytes, size_t pages) {
        Pages = pages;
        Bytes = bytes;
    }

    void Mark(ELarge state) {
        const ui64 marks[] = {
            0x8b38aa5ca4953c98, // ELarge::Free
            0xf916d33584eb5087, // ELarge::Alloc
            0xd33b0eca7651bc3f  // ELarge::Gone
        };

        Token = size_t(marks[ui8(state)]);
    }

    size_t Pages; // Total pages allocated with mmap like call
    size_t Bytes; // Actually requested bytes by user
    size_t Token; // Block state token, see ELarge enum.
};


static void LargeBlockUnmap(void* p, size_t pages) {
    const auto bytes = (pages + 1) * uintptr_t(4096);

    IncrementCounter(CT_MUNMAP, bytes);
    IncrementCounter(CT_MUNMAP_CNT, 1);
#ifdef _MSC_VER
    Y_ASSERT_NOBT(0);
#else
    TLargeBlk::As(p)->Mark(ELarge::Gone);
    munmap((char*)p - 4096ll, bytes);
#endif
}

//////////////////////////////////////////////////////////////////////////
const size_t LB_BUF_SIZE = 250;
const size_t LB_BUF_HASH = 977;
static int LB_LIMIT_TOTAL_SIZE = 500 * 1024 * 1024 / 4096; // do not keep more then this mem total in lbFreePtrs[]
static void* volatile lbFreePtrs[LB_BUF_HASH][LB_BUF_SIZE];
static TAtomic lbFreePageCount;


static void* LargeBlockAlloc(size_t _nSize, ELFAllocCounter counter) {
    size_t pgCount = (_nSize + 4095) / 4096;
#ifdef _MSC_VER
    char* pRes = (char*)VirtualAlloc(0, (pgCount + 1) * 4096ll, MEM_COMMIT, PAGE_READWRITE);
    if (Y_UNLIKELY(pRes == 0)) {
        NMalloc::AbortFromCorruptedAllocator(); // out of memory
    }
#else

    IncrementCounter(counter, pgCount * 4096ll);
    IncrementCounter(CT_SYSTEM_ALLOC, 4096ll);

    int lbHash = pgCount % LB_BUF_HASH;
    for (int i = 0; i < LB_BUF_SIZE; ++i) {
        void* p = lbFreePtrs[lbHash][i];
        if (p == nullptr)
            continue;
        if (DoCas(&lbFreePtrs[lbHash][i], (void*)nullptr, p) == p) {
            size_t realPageCount = TLargeBlk::As(p)->Pages;
            if (realPageCount == pgCount) {
                AtomicAdd(lbFreePageCount, -pgCount);
                TLargeBlk::As(p)->Mark(ELarge::Alloc);
                return p;
            } else {
                if (DoCas(&lbFreePtrs[lbHash][i], p, (void*)nullptr) != (void*)nullptr) {
                    // block was freed while we were busy
                    AtomicAdd(lbFreePageCount, -realPageCount);
                    LargeBlockUnmap(p, realPageCount);
                    --i;
                }
            }
        }
    }
    char* pRes = AllocWithMMap((pgCount + 1) * 4096ll, MM_HUGE);
#endif
    pRes += 4096ll;
    TLargeBlk::As(pRes)->SetSize(_nSize, pgCount);
    TLargeBlk::As(pRes)->Mark(ELarge::Alloc);

    return pRes;
}

#ifndef _MSC_VER
static void FreeAllLargeBlockMem() {
    for (auto& lbFreePtr : lbFreePtrs) {
        for (int i = 0; i < LB_BUF_SIZE; ++i) {
            void* p = lbFreePtr[i];
            if (p == nullptr)
                continue;
            if (DoCas(&lbFreePtr[i], (void*)nullptr, p) == p) {
                int pgCount = TLargeBlk::As(p)->Pages;
                AtomicAdd(lbFreePageCount, -pgCount);
                LargeBlockUnmap(p, pgCount);
            }
        }
    }
}
#endif

static void LargeBlockFree(void* p, ELFAllocCounter counter) {
    if (p == nullptr)
        return;
#ifdef _MSC_VER
    VirtualFree((char*)p - 4096ll, 0, MEM_RELEASE);
#else
    size_t pgCount = TLargeBlk::As(p)->Pages;

    TLargeBlk::As(p)->Mark(ELarge::Free);
    IncrementCounter(counter, pgCount * 4096ll);
    IncrementCounter(CT_SYSTEM_FREE, 4096ll);

    if (lbFreePageCount > LB_LIMIT_TOTAL_SIZE)
        FreeAllLargeBlockMem();
    int lbHash = pgCount % LB_BUF_HASH;
    for (int i = 0; i < LB_BUF_SIZE; ++i) {
        if (lbFreePtrs[lbHash][i] == nullptr) {
            if (DoCas(&lbFreePtrs[lbHash][i], p, (void*)nullptr) == nullptr) {
                AtomicAdd(lbFreePageCount, pgCount);
                return;
            }
        }
    }

    LargeBlockUnmap(p, pgCount);
#endif
}

static void* SystemAlloc(size_t _nSize) {
    //HeapAlloc(GetProcessHeap(), HEAP_GENERATE_EXCEPTIONS, _nSize);
    return LargeBlockAlloc(_nSize, CT_SYSTEM_ALLOC);
}
static void SystemFree(void* p) {
    //HeapFree(GetProcessHeap(), 0, p);
    LargeBlockFree(p, CT_SYSTEM_FREE);
}

//////////////////////////////////////////////////////////////////////////
static int* volatile nLock = nullptr;
static int nLockVar;
inline void RealEnterCriticalDefault(int* volatile* lockPtr) {
    while (DoCas(lockPtr, &nLockVar, (int*)nullptr) != nullptr)
        ; //pthread_yield();
}
inline void RealLeaveCriticalDefault(int* volatile* lockPtr) {
    *lockPtr = nullptr;
}
static void (*RealEnterCritical)(int* volatile* lockPtr) = RealEnterCriticalDefault;
static void (*RealLeaveCritical)(int* volatile* lockPtr) = RealLeaveCriticalDefault;
static void (*BeforeLFAllocGlobalLockAcquired)() = nullptr;
static void (*AfterLFAllocGlobalLockReleased)() = nullptr;
class CCriticalSectionLockMMgr {
public:
    CCriticalSectionLockMMgr() {
        if (BeforeLFAllocGlobalLockAcquired) {
            BeforeLFAllocGlobalLockAcquired();
        }
        RealEnterCritical(&nLock);
    }
    ~CCriticalSectionLockMMgr() {
        RealLeaveCritical(&nLock);
        if (AfterLFAllocGlobalLockReleased) {
            AfterLFAllocGlobalLockReleased();
        }
    }
};

//////////////////////////////////////////////////////////////////////////
class TLFAllocFreeList {
    struct TNode {
        TNode* Next;
    };

    TNode* volatile Head;
    TNode* volatile Pending;
    TAtomic PendingToFreeListCounter;
    TAtomic AllocCount;

    static Y_FORCE_INLINE void Enqueue(TNode* volatile* headPtr, TNode* n) {
        for (;;) {
            TNode* volatile prevHead = *headPtr;
            n->Next = prevHead;
            if (DoCas(headPtr, n, prevHead) == prevHead)
                break;
        }
    }
    Y_FORCE_INLINE void* DoAlloc() {
        TNode* res;
        for (res = Head; res; res = Head) {
            TNode* keepNext = res->Next;
            if (DoCas(&Head, keepNext, res) == res) {
                //Y_VERIFY(keepNext == res->Next);
                break;
            }
        }
        return res;
    }
    void FreeList(TNode* fl) {
        if (!fl)
            return;
        TNode* flTail = fl;
        while (flTail->Next)
            flTail = flTail->Next;
        for (;;) {
            TNode* volatile prevHead = Head;
            flTail->Next = prevHead;
            if (DoCas(&Head, fl, prevHead) == prevHead)
                break;
        }
    }

public:
    Y_FORCE_INLINE void Free(void* ptr) {
        TNode* newFree = (TNode*)ptr;
        if (AtomicAdd(AllocCount, 0) == 0)
            Enqueue(&Head, newFree);
        else
            Enqueue(&Pending, newFree);
    }
    Y_FORCE_INLINE void* Alloc() {
        TAtomic keepCounter = AtomicAdd(PendingToFreeListCounter, 0);
        TNode* fl = Pending;
        if (AtomicAdd(AllocCount, 1) == 1) {
            // No other allocs in progress.
            // If (keepCounter == PendingToFreeListCounter) then Pending was not freed by other threads.
            // Hence Pending is not used in any concurrent DoAlloc() atm and can be safely moved to FreeList
            if (fl && keepCounter == AtomicAdd(PendingToFreeListCounter, 0) && DoCas(&Pending, (TNode*)nullptr, fl) == fl) {
                // pick first element from Pending and return it
                void* res = fl;
                fl = fl->Next;
                // if there are other elements in Pending list, add them to main free list
                FreeList(fl);
                AtomicAdd(PendingToFreeListCounter, 1);
                AtomicAdd(AllocCount, -1);
                return res;
            }
        }
        void* res = DoAlloc();
        AtomicAdd(AllocCount, -1);
        return res;
    }
    void* GetWholeList() {
        TNode* res;
        for (res = Head; res; res = Head) {
            if (DoCas(&Head, (TNode*)nullptr, res) == res)
                break;
        }
        return res;
    }
    void ReturnWholeList(void* ptr) {
        while (AtomicAdd(AllocCount, 0) != 0) // theoretically can run into problems with parallel DoAlloc()
            ;                                 //ThreadYield();
        for (;;) {
            TNode* prevHead = Head;
            if (DoCas(&Head, (TNode*)ptr, prevHead) == prevHead) {
                FreeList(prevHead);
                break;
            }
        }
    }
};

/////////////////////////////////////////////////////////////////////////
static TLFAllocFreeList globalFreeLists[N_SIZES];
static char* volatile globalCurrentPtr[N_SIZES];
static TLFAllocFreeList blockFreeList;

// globalFreeLists[] contains TFreeListGroup, each of them points up to 15 free blocks
const int FL_GROUP_SIZE = 15;
struct TFreeListGroup {
    TFreeListGroup* Next;
    char* Ptrs[FL_GROUP_SIZE];
};
#ifdef _64_
const int FREE_LIST_GROUP_SIZEIDX = 8;
#else
const int FREE_LIST_GROUP_SIZEIDX = 6;
#endif

//////////////////////////////////////////////////////////////////////////
// find free chunks and reset chunk size so they can be reused by different sized allocations
// do not look at blockFreeList (TFreeListGroup has same size for any allocations)
static bool DefragmentMem() {
    if (!EnableDefrag) {
        return false;
    }

    IncrementCounter(CT_DEGRAGMENT_CNT, 1);

    int* nFreeCount = (int*)SystemAlloc(N_CHUNKS * sizeof(int));
    if (Y_UNLIKELY(!nFreeCount)) {
        //__debugbreak();
        NMalloc::AbortFromCorruptedAllocator();
    }
    memset(nFreeCount, 0, N_CHUNKS * sizeof(int));

    TFreeListGroup* wholeLists[N_SIZES];
    for (int nSizeIdx = 0; nSizeIdx < N_SIZES; ++nSizeIdx) {
        wholeLists[nSizeIdx] = (TFreeListGroup*)globalFreeLists[nSizeIdx].GetWholeList();
        for (TFreeListGroup* g = wholeLists[nSizeIdx]; g; g = g->Next) {
            for (auto pData : g->Ptrs) {
                if (pData) {
                    uintptr_t nChunk = (pData - ALLOC_START) / N_CHUNK_SIZE;
                    ++nFreeCount[nChunk];
                    Y_ASSERT_NOBT(chunkSizeIdx[nChunk] == nSizeIdx);
                }
            }
        }
    }

    bool bRes = false;
    for (size_t nChunk = 0; nChunk < N_CHUNKS; ++nChunk) {
        int fc = nFreeCount[nChunk];
        nFreeCount[nChunk] = 0;
        if (chunkSizeIdx[nChunk] <= 0)
            continue;
        int nEntries = N_CHUNK_SIZE / nSizeIdxToSize[static_cast<int>(chunkSizeIdx[nChunk])];
        Y_ASSERT_NOBT(fc <= nEntries); // can not have more free blocks then total count
        if (fc == nEntries) {
            bRes = true;
            nFreeCount[nChunk] = 1;
        }
    }
    if (bRes) {
        for (auto& wholeList : wholeLists) {
            TFreeListGroup** ppPtr = &wholeList;
            while (*ppPtr) {
                TFreeListGroup* g = *ppPtr;
                int dst = 0;
                for (auto pData : g->Ptrs) {
                    if (pData) {
                        uintptr_t nChunk = (pData - ALLOC_START) / N_CHUNK_SIZE;
                        if (nFreeCount[nChunk] == 0)
                            g->Ptrs[dst++] = pData; // block is not freed, keep pointer
                    }
                }
                if (dst == 0) {
                    // no valid pointers in group, free it
                    *ppPtr = g->Next;
                    blockFreeList.Free(g);
                } else {
                    // reset invalid pointers to 0
                    for (int i = dst; i < FL_GROUP_SIZE; ++i)
                        g->Ptrs[i] = nullptr;
                    ppPtr = &g->Next;
                }
            }
        }
        for (uintptr_t nChunk = 0; nChunk < N_CHUNKS; ++nChunk) {
            if (!nFreeCount[nChunk])
                continue;
            char* pStart = ALLOC_START + nChunk * N_CHUNK_SIZE;
#ifdef _win_
            VirtualFree(pStart, N_CHUNK_SIZE, MEM_DECOMMIT);
#elif defined(_freebsd_)
            madvise(pStart, N_CHUNK_SIZE, MADV_FREE);
#else
            madvise(pStart, N_CHUNK_SIZE, MADV_DONTNEED);
#endif
            AddFreeChunk(nChunk);
        }
    }

    for (int nSizeIdx = 0; nSizeIdx < N_SIZES; ++nSizeIdx)
        globalFreeLists[nSizeIdx].ReturnWholeList(wholeLists[nSizeIdx]);

    SystemFree(nFreeCount);
    return bRes;
}

static Y_FORCE_INLINE void* LFAllocFromCurrentChunk(int nSizeIdx, int blockSize, int count) {
    char* volatile* pFreeArray = &globalCurrentPtr[nSizeIdx];
    while (char* newBlock = *pFreeArray) {
        char* nextFree = newBlock + blockSize * count;

        // check if there is space in chunk
        char* globalEndPtr = ALLOC_START + ((newBlock - ALLOC_START) & ~((uintptr_t)N_CHUNK_SIZE - 1)) + N_CHUNK_SIZE;
        if (nextFree >= globalEndPtr) {
            if (nextFree > globalEndPtr)
                break;
            nextFree = nullptr; // it was last block in chunk
        }
        if (DoCas(pFreeArray, nextFree, newBlock) == newBlock)
            return newBlock;
    }
    return nullptr;
}

enum EDefrag {
    MEM_DEFRAG,
    NO_MEM_DEFRAG,
};

static void* SlowLFAlloc(int nSizeIdx, int blockSize, EDefrag defrag) {
    IncrementCounter(CT_SLOW_ALLOC_CNT, 1);

    CCriticalSectionLockMMgr ls;
    void* res = LFAllocFromCurrentChunk(nSizeIdx, blockSize, 1);
    if (res)
        return res; // might happen when other thread allocated new current chunk

    for (;;) {
        uintptr_t nChunk;
        if (GetFreeChunk(&nChunk)) {
            char* newPlace = ALLOC_START + nChunk * N_CHUNK_SIZE;
#ifdef _MSC_VER
            void* pTest = VirtualAlloc(newPlace, N_CHUNK_SIZE, MEM_COMMIT, PAGE_READWRITE);
            Y_ASSERT_NOBT(pTest == newPlace);
#endif
            chunkSizeIdx[nChunk] = (char)nSizeIdx;
            globalCurrentPtr[nSizeIdx] = newPlace + blockSize;
            return newPlace;
        }

        // out of luck, try to defrag
        if (defrag == MEM_DEFRAG && DefragmentMem()) {
            continue;
        }

        char* largeBlock = AllocWithMMap(N_LARGE_ALLOC_SIZE, MM_NORMAL);
        uintptr_t addr = ((largeBlock - ALLOC_START) + N_CHUNK_SIZE - 1) & (~(N_CHUNK_SIZE - 1));
        uintptr_t endAddr = ((largeBlock - ALLOC_START) + N_LARGE_ALLOC_SIZE) & (~(N_CHUNK_SIZE - 1));
        for (uintptr_t p = addr; p < endAddr; p += N_CHUNK_SIZE) {
            uintptr_t chunk = p / N_CHUNK_SIZE;
            Y_ASSERT_NOBT(chunk * N_CHUNK_SIZE == p);
            Y_ASSERT_NOBT(chunkSizeIdx[chunk] == 0);
            AddFreeChunk(chunk);
        }
    }
    return nullptr;
}

// allocate single block
static Y_FORCE_INLINE void* LFAllocNoCache(int nSizeIdx, EDefrag defrag) {
    int blockSize = nSizeIdxToSize[nSizeIdx];
    void* res = LFAllocFromCurrentChunk(nSizeIdx, blockSize, 1);
    if (res)
        return res;

    return SlowLFAlloc(nSizeIdx, blockSize, defrag);
}

// allocate multiple blocks, returns number of blocks allocated (max FL_GROUP_SIZE)
// buf should have space for at least FL_GROUP_SIZE elems
static Y_FORCE_INLINE int LFAllocNoCacheMultiple(int nSizeIdx, char** buf) {
    int blockSize = nSizeIdxToSize[nSizeIdx];
    void* res = LFAllocFromCurrentChunk(nSizeIdx, blockSize, FL_GROUP_SIZE);
    if (res) {
        char* resPtr = (char*)res;
        for (int k = 0; k < FL_GROUP_SIZE; ++k) {
            buf[k] = resPtr;
            resPtr += blockSize;
        }
        return FL_GROUP_SIZE;
    }
    buf[0] = (char*)SlowLFAlloc(nSizeIdx, blockSize, MEM_DEFRAG);
    return 1;
}

// take several blocks from global free list (max FL_GROUP_SIZE blocks), returns number of blocks taken
// buf should have space for at least FL_GROUP_SIZE elems
static Y_FORCE_INLINE int TakeBlocksFromGlobalFreeList(int nSizeIdx, char** buf) {
    TLFAllocFreeList& fl = globalFreeLists[nSizeIdx];
    TFreeListGroup* g = (TFreeListGroup*)fl.Alloc();
    if (g) {
        int resCount = 0;
        for (auto& ptr : g->Ptrs) {
            if (ptr)
                buf[resCount++] = ptr;
            else
                break;
        }
        blockFreeList.Free(g);
        return resCount;
    }
    return 0;
}

// add several blocks to global free list
static Y_FORCE_INLINE void PutBlocksToGlobalFreeList(ptrdiff_t nSizeIdx, char** buf, int count) {
    for (int startIdx = 0; startIdx < count;) {
        TFreeListGroup* g = (TFreeListGroup*)blockFreeList.Alloc();
        Y_ASSERT_NOBT(sizeof(TFreeListGroup) == nSizeIdxToSize[FREE_LIST_GROUP_SIZEIDX]);
        if (!g) {
            g = (TFreeListGroup*)LFAllocNoCache(FREE_LIST_GROUP_SIZEIDX, NO_MEM_DEFRAG);
        }

        int groupSize = count - startIdx;
        if (groupSize > FL_GROUP_SIZE)
            groupSize = FL_GROUP_SIZE;
        for (int i = 0; i < groupSize; ++i)
            g->Ptrs[i] = buf[startIdx + i];
        for (int i = groupSize; i < FL_GROUP_SIZE; ++i)
            g->Ptrs[i] = nullptr;

        // add free group to the global list
        TLFAllocFreeList& fl = globalFreeLists[nSizeIdx];
        fl.Free(g);

        startIdx += groupSize;
    }
}

//////////////////////////////////////////////////////////////////////////
static TAtomic GlobalCounters[CT_MAX];
const int MAX_LOCAL_UPDATES = 100;

struct TLocalCounter {
    intptr_t Value;
    int Updates;
    TAtomic* Parent;

    Y_FORCE_INLINE void Init(TAtomic* parent) {
        Parent = parent;
        Value = 0;
        Updates = 0;
    }

    Y_FORCE_INLINE void Increment(size_t value) {
        Value += value;
        if (++Updates > MAX_LOCAL_UPDATES) {
            Flush();
        }
    }

    Y_FORCE_INLINE void Flush() {
        AtomicAdd(*Parent, Value);
        Value = 0;
        Updates = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////
// DBG stuff
////////////////////////////////////////////////////////////////////////////////

#if defined(LFALLOC_DBG)

struct TPerTagAllocCounter {
    TAtomic Size;
    TAtomic Count;

    Y_FORCE_INLINE void Alloc(size_t size) {
        AtomicAdd(Size, size);
        AtomicAdd(Count, 1);
    }

    Y_FORCE_INLINE void Free(size_t size) {
        AtomicSub(Size, size);
        AtomicSub(Count, 1);
    }
};

struct TLocalPerTagAllocCounter {
    intptr_t Size;
    int Count;
    int Updates;

    Y_FORCE_INLINE void Init() {
        Size = 0;
        Count = 0;
        Updates = 0;
    }

    Y_FORCE_INLINE void Alloc(TPerTagAllocCounter& parent, size_t size) {
        Size += size;
        ++Count;
        if (++Updates > MAX_LOCAL_UPDATES) {
            Flush(parent);
        }
    }

    Y_FORCE_INLINE void Free(TPerTagAllocCounter& parent, size_t size) {
        Size -= size;
        --Count;
        if (++Updates > MAX_LOCAL_UPDATES) {
            Flush(parent);
        }
    }

    Y_FORCE_INLINE void Flush(TPerTagAllocCounter& parent) {
        AtomicAdd(parent.Size, Size);
        Size = 0;
        AtomicAdd(parent.Count, Count);
        Count = 0;
        Updates = 0;
    }
};

static const int DBG_ALLOC_MAX_TAG = 1000;
static const int DBG_ALLOC_NUM_SIZES = 30;
static TPerTagAllocCounter GlobalPerTagAllocCounters[DBG_ALLOC_MAX_TAG][DBG_ALLOC_NUM_SIZES];

#endif // LFALLOC_DBG

//////////////////////////////////////////////////////////////////////////
const int THREAD_BUF = 256;
static int borderSizes[N_SIZES];
const int MAX_MEM_PER_SIZE_PER_THREAD = 512 * 1024;
struct TThreadAllocInfo {
    // FreePtrs - pointers to first free blocks in per thread block list
    // LastFreePtrs - pointers to last blocks in lists, may be invalid if FreePtr is zero
    char* FreePtrs[N_SIZES][THREAD_BUF];
    int FreePtrIndex[N_SIZES];
    TThreadAllocInfo* pNextInfo;
    TLocalCounter LocalCounters[CT_MAX];

#if defined(LFALLOC_DBG)
    TLocalPerTagAllocCounter LocalPerTagAllocCounters[DBG_ALLOC_MAX_TAG][DBG_ALLOC_NUM_SIZES];
#endif
#ifdef _win_
    HANDLE hThread;
#endif

    void Init(TThreadAllocInfo** pHead) {
        memset(this, 0, sizeof(*this));
        for (auto& i : FreePtrIndex)
            i = THREAD_BUF;
#ifdef _win_
        BOOL b = DuplicateHandle(
            GetCurrentProcess(), GetCurrentThread(),
            GetCurrentProcess(), &hThread,
            0, FALSE, DUPLICATE_SAME_ACCESS);
        Y_ASSERT_NOBT(b);
#endif
        pNextInfo = *pHead;
        *pHead = this;
        for (int k = 0; k < N_SIZES; ++k) {
            int maxCount = MAX_MEM_PER_SIZE_PER_THREAD / nSizeIdxToSize[k];
            if (maxCount > THREAD_BUF)
                maxCount = THREAD_BUF;
            borderSizes[k] = THREAD_BUF - maxCount;
        }
        for (int i = 0; i < CT_MAX; ++i) {
            LocalCounters[i].Init(&GlobalCounters[i]);
        }
#if defined(LFALLOC_DBG)
        for (int tag = 0; tag < DBG_ALLOC_MAX_TAG; ++tag) {
            for (int sizeIdx = 0; sizeIdx < DBG_ALLOC_NUM_SIZES; ++sizeIdx) {
                auto& local = LocalPerTagAllocCounters[tag][sizeIdx];
                local.Init();
            }
        }
#endif
    }
    void Done() {
        for (auto sizeIdx : FreePtrIndex) {
            Y_ASSERT_NOBT(sizeIdx == THREAD_BUF);
        }
        for (auto& localCounter : LocalCounters) {
            localCounter.Flush();
        }
#if defined(LFALLOC_DBG)
        for (int tag = 0; tag < DBG_ALLOC_MAX_TAG; ++tag) {
            for (int sizeIdx = 0; sizeIdx < DBG_ALLOC_NUM_SIZES; ++sizeIdx) {
                auto& local = LocalPerTagAllocCounters[tag][sizeIdx];
                auto& global = GlobalPerTagAllocCounters[tag][sizeIdx];
                local.Flush(global);
            }
        }
#endif
#ifdef _win_
        if (hThread)
            CloseHandle(hThread);
#endif
    }
};
PERTHREAD TThreadAllocInfo* pThreadInfo;
static TThreadAllocInfo* pThreadInfoList;

static int* volatile nLockThreadInfo = nullptr;
class TLockThreadListMMgr {
public:
    TLockThreadListMMgr() {
        RealEnterCritical(&nLockThreadInfo);
    }
    ~TLockThreadListMMgr() {
        RealLeaveCritical(&nLockThreadInfo);
    }
};

static Y_FORCE_INLINE void IncrementCounter(ELFAllocCounter counter, size_t value) {
#ifdef LFALLOC_YT
    TThreadAllocInfo* thr = pThreadInfo;
    if (thr) {
        thr->LocalCounters[counter].Increment(value);
    } else {
        AtomicAdd(GlobalCounters[counter], value);
    }
#endif
}

extern "C" i64 GetLFAllocCounterFast(int counter) {
#ifdef LFALLOC_YT
    return GlobalCounters[counter];
#else
    return 0;
#endif
}

extern "C" i64 GetLFAllocCounterFull(int counter) {
#ifdef LFALLOC_YT
    i64 ret = GlobalCounters[counter];
    {
        TLockThreadListMMgr ll;
        for (TThreadAllocInfo** p = &pThreadInfoList; *p;) {
            TThreadAllocInfo* pInfo = *p;
            ret += pInfo->LocalCounters[counter].Value;
            p = &pInfo->pNextInfo;
        }
    }
    return ret;
#else
    return 0;
#endif
}

static void MoveSingleThreadFreeToGlobal(TThreadAllocInfo* pInfo) {
    for (int sizeIdx = 0; sizeIdx < N_SIZES; ++sizeIdx) {
        int& freePtrIdx = pInfo->FreePtrIndex[sizeIdx];
        char** freePtrs = pInfo->FreePtrs[sizeIdx];
        PutBlocksToGlobalFreeList(sizeIdx, freePtrs + freePtrIdx, THREAD_BUF - freePtrIdx);
        freePtrIdx = THREAD_BUF;
    }
}

#ifdef _win_
static bool IsDeadThread(TThreadAllocInfo* pInfo) {
    DWORD dwExit;
    bool isDead = !GetExitCodeThread(pInfo->hThread, &dwExit) || dwExit != STILL_ACTIVE;
    return isDead;
}

static void CleanupAfterDeadThreads() {
    TLockThreadListMMgr ls;
    for (TThreadAllocInfo** p = &pThreadInfoList; *p;) {
        TThreadAllocInfo* pInfo = *p;
        if (IsDeadThread(pInfo)) {
            MoveSingleThreadFreeToGlobal(pInfo);
            pInfo->Done();
            *p = pInfo->pNextInfo;
            SystemFree(pInfo);
        } else
            p = &pInfo->pNextInfo;
    }
}
#endif

#ifndef _win_
static pthread_key_t ThreadCacheCleaner;
static void* volatile ThreadCacheCleanerStarted; // 0 = not started, -1 = started, -2 = is starting
static PERTHREAD bool IsStoppingThread;

static void FreeThreadCache(void*) {
    TThreadAllocInfo* pToDelete = nullptr;
    {
        TLockThreadListMMgr ls;
        pToDelete = pThreadInfo;
        if (pToDelete == nullptr)
            return;

        // remove from the list
        for (TThreadAllocInfo** p = &pThreadInfoList; *p; p = &(*p)->pNextInfo) {
            if (*p == pToDelete) {
                *p = pToDelete->pNextInfo;
                break;
            }
        }
        IsStoppingThread = true;
        pThreadInfo = nullptr;
    }

    // free per thread buf
    MoveSingleThreadFreeToGlobal(pToDelete);
    pToDelete->Done();
    SystemFree(pToDelete);
}
#endif

static void AllocThreadInfo() {
#ifndef _win_
    if (DoCas(&ThreadCacheCleanerStarted, (void*)-2, (void*)nullptr) == (void*)nullptr) {
        pthread_key_create(&ThreadCacheCleaner, FreeThreadCache);
        ThreadCacheCleanerStarted = (void*)-1;
    }
    if (ThreadCacheCleanerStarted != (void*)-1)
        return; // do not use ThreadCacheCleaner until it is constructed

    {
        if (IsStoppingThread)
            return;
        TLockThreadListMMgr ls;
        if (IsStoppingThread) // better safe than sorry
            return;

        pThreadInfo = (TThreadAllocInfo*)SystemAlloc(sizeof(TThreadAllocInfo));
        pThreadInfo->Init(&pThreadInfoList);
    }
    pthread_setspecific(ThreadCacheCleaner, (void*)-1); // without value destructor will not be called
#else
    CleanupAfterDeadThreads();
    {
        TLockThreadListMMgr ls;
        pThreadInfo = (TThreadAllocInfo*)SystemAlloc(sizeof(TThreadAllocInfo));
        pThreadInfo->Init(&pThreadInfoList);
    }
#endif
}

    //////////////////////////////////////////////////////////////////////////
    // DBG stuff
    //////////////////////////////////////////////////////////////////////////

#if defined(LFALLOC_DBG)

struct TAllocHeader {
    size_t Size;
    int Tag;
    int Cookie;
};

static inline void* GetAllocPtr(TAllocHeader* p) {
    return p + 1;
}

static inline TAllocHeader* GetAllocHeader(void* p) {
    return ((TAllocHeader*)p) - 1;
}

PERTHREAD int AllocationTag;
extern "C" int SetThreadAllocTag(int tag) {
    int prevTag = AllocationTag;
    if (tag < DBG_ALLOC_MAX_TAG && tag >= 0) {
        AllocationTag = tag;
    }
    return prevTag;
}

PERTHREAD bool ProfileCurrentThread;
extern "C" bool SetProfileCurrentThread(bool newVal) {
    bool prevVal = ProfileCurrentThread;
    ProfileCurrentThread = newVal;
    return prevVal;
}

static volatile bool ProfileAllThreads;
extern "C" bool SetProfileAllThreads(bool newVal) {
    bool prevVal = ProfileAllThreads;
    ProfileAllThreads = newVal;
    return prevVal;
}

static volatile bool AllocationSamplingEnabled;
extern "C" bool SetAllocationSamplingEnabled(bool newVal) {
    bool prevVal = AllocationSamplingEnabled;
    AllocationSamplingEnabled = newVal;
    return prevVal;
}

static size_t AllocationSampleRate = 1000;
extern "C" size_t SetAllocationSampleRate(size_t newVal) {
    size_t prevVal = AllocationSampleRate;
    AllocationSampleRate = newVal;
    return prevVal;
}

static size_t AllocationSampleMaxSize = N_MAX_FAST_SIZE;
extern "C" size_t SetAllocationSampleMaxSize(size_t newVal) {
    size_t prevVal = AllocationSampleMaxSize;
    AllocationSampleMaxSize = newVal;
    return prevVal;
}

using TAllocationCallback = int(int tag, size_t size, int sizeIdx);
static TAllocationCallback* AllocationCallback;
extern "C" TAllocationCallback* SetAllocationCallback(TAllocationCallback* newVal) {
    TAllocationCallback* prevVal = AllocationCallback;
    AllocationCallback = newVal;
    return prevVal;
}

using TDeallocationCallback = void(int cookie, int tag, size_t size, int sizeIdx);
static TDeallocationCallback* DeallocationCallback;
extern "C" TDeallocationCallback* SetDeallocationCallback(TDeallocationCallback* newVal) {
    TDeallocationCallback* prevVal = DeallocationCallback;
    DeallocationCallback = newVal;
    return prevVal;
}

PERTHREAD TAtomic AllocationsCount;
PERTHREAD bool InAllocationCallback;

static const int DBG_ALLOC_INVALID_COOKIE = -1;
static inline int SampleAllocation(TAllocHeader* p, int sizeIdx) {
    int cookie = DBG_ALLOC_INVALID_COOKIE;
    if (AllocationSamplingEnabled && (ProfileCurrentThread || ProfileAllThreads) && !InAllocationCallback) {
        if (p->Size > AllocationSampleMaxSize || ++AllocationsCount % AllocationSampleRate == 0) {
            if (AllocationCallback) {
                InAllocationCallback = true;
                cookie = AllocationCallback(p->Tag, p->Size, sizeIdx);
                InAllocationCallback = false;
            }
        }
    }
    return cookie;
}

static inline void SampleDeallocation(TAllocHeader* p, int sizeIdx) {
    if (p->Cookie != DBG_ALLOC_INVALID_COOKIE && !InAllocationCallback) {
        if (DeallocationCallback) {
            InAllocationCallback = true;
            DeallocationCallback(p->Cookie, p->Tag, p->Size, sizeIdx);
            InAllocationCallback = false;
        }
    }
}

static inline void TrackPerTagAllocation(TAllocHeader* p, int sizeIdx) {
    if (p->Tag < DBG_ALLOC_MAX_TAG && p->Tag >= 0) {
        Y_ASSERT_NOBT(sizeIdx < DBG_ALLOC_NUM_SIZES);
        auto& global = GlobalPerTagAllocCounters[p->Tag][sizeIdx];

        TThreadAllocInfo* thr = pThreadInfo;
        if (thr) {
            auto& local = thr->LocalPerTagAllocCounters[p->Tag][sizeIdx];
            local.Alloc(global, p->Size);
        } else {
            global.Alloc(p->Size);
        }
    }
}

static inline void TrackPerTagDeallocation(TAllocHeader* p, int sizeIdx) {
    if (p->Tag < DBG_ALLOC_MAX_TAG && p->Tag >= 0) {
        Y_ASSERT_NOBT(sizeIdx < DBG_ALLOC_NUM_SIZES);
        auto& global = GlobalPerTagAllocCounters[p->Tag][sizeIdx];

        TThreadAllocInfo* thr = pThreadInfo;
        if (thr) {
            auto& local = thr->LocalPerTagAllocCounters[p->Tag][sizeIdx];
            local.Free(global, p->Size);
        } else {
            global.Free(p->Size);
        }
    }
}

static void* TrackAllocation(void* ptr, size_t size, int sizeIdx) {
    TAllocHeader* p = (TAllocHeader*)ptr;
    p->Size = size;
    p->Tag = AllocationTag;
    p->Cookie = SampleAllocation(p, sizeIdx);
    TrackPerTagAllocation(p, sizeIdx);
    return GetAllocPtr(p);
}

static void TrackDeallocation(void* ptr, int sizeIdx) {
    TAllocHeader* p = (TAllocHeader*)ptr;
    SampleDeallocation(p, sizeIdx);
    TrackPerTagDeallocation(p, sizeIdx);
}

struct TPerTagAllocInfo {
    ssize_t Count;
    ssize_t Size;
};

extern "C" void GetPerTagAllocInfo(
    bool flushPerThreadCounters,
    TPerTagAllocInfo* info,
    int& maxTag,
    int& numSizes) {
    maxTag = DBG_ALLOC_MAX_TAG;
    numSizes = DBG_ALLOC_NUM_SIZES;

    if (info) {
        if (flushPerThreadCounters) {
            TLockThreadListMMgr ll;
            for (TThreadAllocInfo** p = &pThreadInfoList; *p;) {
                TThreadAllocInfo* pInfo = *p;
                for (int tag = 0; tag < DBG_ALLOC_MAX_TAG; ++tag) {
                    for (int sizeIdx = 0; sizeIdx < DBG_ALLOC_NUM_SIZES; ++sizeIdx) {
                        auto& local = pInfo->LocalPerTagAllocCounters[tag][sizeIdx];
                        auto& global = GlobalPerTagAllocCounters[tag][sizeIdx];
                        local.Flush(global);
                    }
                }
                p = &pInfo->pNextInfo;
            }
        }

        for (int tag = 0; tag < DBG_ALLOC_MAX_TAG; ++tag) {
            for (int sizeIdx = 0; sizeIdx < DBG_ALLOC_NUM_SIZES; ++sizeIdx) {
                auto& global = GlobalPerTagAllocCounters[tag][sizeIdx];
                auto& res = info[tag * DBG_ALLOC_NUM_SIZES + sizeIdx];
                res.Count = global.Count;
                res.Size = global.Size;
            }
        }
    }
}

#endif // LFALLOC_DBG

//////////////////////////////////////////////////////////////////////////
static Y_FORCE_INLINE void* LFAllocImpl(size_t _nSize) {
#if defined(LFALLOC_DBG)
    size_t size = _nSize;
    _nSize += sizeof(TAllocHeader);
#endif

    IncrementCounter(CT_USER_ALLOC, _nSize);

    int nSizeIdx;
    if (_nSize > 512) {
        if (_nSize > N_MAX_FAST_SIZE) {
            void* ptr = LargeBlockAlloc(_nSize, CT_LARGE_ALLOC);
#if defined(LFALLOC_DBG)
            ptr = TrackAllocation(ptr, size, N_SIZES);
#endif
            return ptr;
        }
        nSizeIdx = size2idxArr2[(_nSize - 1) >> 8];
    } else
        nSizeIdx = size2idxArr1[1 + (((int)_nSize - 1) >> 3)];

    IncrementCounter(CT_SMALL_ALLOC, nSizeIdxToSize[nSizeIdx]);

    // check per thread buffer
    TThreadAllocInfo* thr = pThreadInfo;
    if (!thr) {
        AllocThreadInfo();
        thr = pThreadInfo;
        if (!thr) {
            void* ptr = LFAllocNoCache(nSizeIdx, MEM_DEFRAG);
#if defined(LFALLOC_DBG)
            ptr = TrackAllocation(ptr, size, nSizeIdx);
#endif
            return ptr;
        }
    }
    {
        int& freePtrIdx = thr->FreePtrIndex[nSizeIdx];
        if (freePtrIdx < THREAD_BUF) {
            void* ptr = thr->FreePtrs[nSizeIdx][freePtrIdx++];
#if defined(LFALLOC_DBG)
            ptr = TrackAllocation(ptr, size, nSizeIdx);
#endif
            return ptr;
        }

        // try to alloc from global free list
        char* buf[FL_GROUP_SIZE];
        int count = TakeBlocksFromGlobalFreeList(nSizeIdx, buf);
        if (count == 0) {
            count = LFAllocNoCacheMultiple(nSizeIdx, buf);
            if (count == 0) {
                NMalloc::AbortFromCorruptedAllocator(); // no way LFAllocNoCacheMultiple() can fail
            }
        }
        char** dstBuf = thr->FreePtrs[nSizeIdx] + freePtrIdx - 1;
        for (int i = 0; i < count - 1; ++i)
            dstBuf[-i] = buf[i];
        freePtrIdx -= count - 1;
        void* ptr = buf[count - 1];
#if defined(LFALLOC_DBG)
        ptr = TrackAllocation(ptr, size, nSizeIdx);
#endif
        return ptr;
    }
}

static Y_FORCE_INLINE void* LFAlloc(size_t _nSize) {
    void* res = LFAllocImpl(_nSize);
#ifdef DBG_FILL_MEMORY
    if (FillMemoryOnAllocation && res && (_nSize <= DBG_FILL_MAX_SIZE)) {
        memset(res, 0xcf, _nSize);
    }
#endif
    return res;
}

static Y_FORCE_INLINE void LFFree(void* p) {
#if defined(LFALLOC_DBG)
    if (p == nullptr)
        return;
    p = GetAllocHeader(p);
#endif

    uintptr_t chkOffset = ((char*)p - ALLOC_START) - 1ll;
    if (chkOffset >= N_MAX_WORKSET_SIZE) {
        if (p == nullptr)
            return;
#if defined(LFALLOC_DBG)
        TrackDeallocation(p, N_SIZES);
#endif
        LargeBlockFree(p, CT_LARGE_FREE);
        return;
    }

    uintptr_t chunk = ((char*)p - ALLOC_START) / N_CHUNK_SIZE;
    ptrdiff_t nSizeIdx = chunkSizeIdx[chunk];
    if (nSizeIdx <= 0) {
#if defined(LFALLOC_DBG)
        TrackDeallocation(p, N_SIZES);
#endif
        LargeBlockFree(p, CT_LARGE_FREE);
        return;
    }

#if defined(LFALLOC_DBG)
    TrackDeallocation(p, nSizeIdx);
#endif

#ifdef DBG_FILL_MEMORY
    memset(p, 0xfe, nSizeIdxToSize[nSizeIdx]);
#endif

    IncrementCounter(CT_SMALL_FREE, nSizeIdxToSize[nSizeIdx]);

    // try to store info to per thread buf
    TThreadAllocInfo* thr = pThreadInfo;
    if (thr) {
        int& freePtrIdx = thr->FreePtrIndex[nSizeIdx];
        if (freePtrIdx > borderSizes[nSizeIdx]) {
            thr->FreePtrs[nSizeIdx][--freePtrIdx] = (char*)p;
            return;
        }

        // move several pointers to global free list
        int freeCount = FL_GROUP_SIZE;
        if (freeCount > THREAD_BUF - freePtrIdx)
            freeCount = THREAD_BUF - freePtrIdx;
        char** freePtrs = thr->FreePtrs[nSizeIdx];
        PutBlocksToGlobalFreeList(nSizeIdx, freePtrs + freePtrIdx, freeCount);
        freePtrIdx += freeCount;

        freePtrs[--freePtrIdx] = (char*)p;

    } else {
        AllocThreadInfo();
        PutBlocksToGlobalFreeList(nSizeIdx, (char**)&p, 1);
    }
}

static size_t LFGetSize(const void* p) {
#if defined(LFALLOC_DBG)
    if (p == nullptr)
        return 0;
    return GetAllocHeader(const_cast<void*>(p))->Size;
#endif

    uintptr_t chkOffset = ((const char*)p - ALLOC_START);
    if (chkOffset >= N_MAX_WORKSET_SIZE) {
        if (p == nullptr)
            return 0;
        return TLargeBlk::As(p)->Pages * 4096ll;
    }
    uintptr_t chunk = ((const char*)p - ALLOC_START) / N_CHUNK_SIZE;
    ptrdiff_t nSizeIdx = chunkSizeIdx[chunk];
    if (nSizeIdx <= 0)
        return TLargeBlk::As(p)->Pages * 4096ll;
    return nSizeIdxToSize[nSizeIdx];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Output mem alloc stats
const int N_PAGE_SIZE = 4096;
static void DebugTraceMMgr(const char* pszFormat, ...) // __cdecl
{
    static char buff[20000];
    va_list va;
    //
    va_start(va, pszFormat);
    vsprintf(buff, pszFormat, va);
    va_end(va);
//
#ifdef _win_
    OutputDebugStringA(buff);
#else
    fprintf(stderr, buff);
#endif
}

struct TChunkStats {
    char *Start, *Finish;
    i64 Size;
    char* Entries;
    i64 FreeCount;

    TChunkStats(size_t chunk, i64 size, char* entries)
        : Size(size)
        , Entries(entries)
        , FreeCount(0)
    {
        Start = ALLOC_START + chunk * N_CHUNK_SIZE;
        Finish = Start + N_CHUNK_SIZE;
    }
    void CheckBlock(char* pBlock) {
        if (pBlock && pBlock >= Start && pBlock < Finish) {
            ++FreeCount;
            i64 nShift = pBlock - Start;
            i64 nOffsetInStep = nShift & (N_CHUNK_SIZE - 1);
            Entries[nOffsetInStep / Size] = 1;
        }
    }
    void SetGlobalFree(char* ptr) {
        i64 nShift = ptr - Start;
        i64 nOffsetInStep = nShift & (N_CHUNK_SIZE - 1);
        while (nOffsetInStep + Size <= N_CHUNK_SIZE) {
            ++FreeCount;
            Entries[nOffsetInStep / Size] = 1;
            nOffsetInStep += Size;
        }
    }
};

static void DumpMemoryBlockUtilizationLocked() {
    TFreeListGroup* wholeLists[N_SIZES];
    for (int nSizeIdx = 0; nSizeIdx < N_SIZES; ++nSizeIdx) {
        wholeLists[nSizeIdx] = (TFreeListGroup*)globalFreeLists[nSizeIdx].GetWholeList();
    }
    char* bfList = (char*)blockFreeList.GetWholeList();

    DebugTraceMMgr("memory blocks utilisation stats:\n");
    i64 nTotalAllocated = 0, nTotalFree = 0, nTotalBadPages = 0, nTotalPages = 0, nTotalUsed = 0, nTotalLocked = 0;
    i64 nTotalGroupBlocks = 0;
    char* entries;
    entries = (char*)SystemAlloc((N_CHUNK_SIZE / 4));
    for (size_t k = 0; k < N_CHUNKS; ++k) {
        if (chunkSizeIdx[k] <= 0) {
            if (chunkSizeIdx[k] == -1)
                nTotalLocked += N_CHUNK_SIZE;
            continue;
        }
        i64 nSizeIdx = chunkSizeIdx[k];
        i64 nSize = nSizeIdxToSize[nSizeIdx];
        TChunkStats cs(k, nSize, entries);
        int nEntriesTotal = N_CHUNK_SIZE / nSize;
        memset(entries, 0, nEntriesTotal);
        for (TFreeListGroup* g = wholeLists[nSizeIdx]; g; g = g->Next) {
            for (auto& ptr : g->Ptrs)
                cs.CheckBlock(ptr);
        }
        TChunkStats csGB(k, nSize, entries);
        if (nSizeIdx == FREE_LIST_GROUP_SIZEIDX) {
            for (auto g : wholeLists) {
                for (; g; g = g->Next)
                    csGB.CheckBlock((char*)g);
            }
            for (char* blk = bfList; blk; blk = *(char**)blk)
                csGB.CheckBlock(blk);
            nTotalGroupBlocks += csGB.FreeCount * nSize;
        }
        if (((globalCurrentPtr[nSizeIdx] - ALLOC_START) / N_CHUNK_SIZE) == k)
            cs.SetGlobalFree(globalCurrentPtr[nSizeIdx]);
        nTotalUsed += (nEntriesTotal - cs.FreeCount - csGB.FreeCount) * nSize;

        char pages[N_CHUNK_SIZE / N_PAGE_SIZE];
        memset(pages, 0, sizeof(pages));
        for (int i = 0, nShift = 0; i < nEntriesTotal; ++i, nShift += nSize) {
            int nBit = 0;
            if (entries[i])
                nBit = 1; // free entry
            else
                nBit = 2; // used entry
            for (i64 nDelta = nSize - 1; nDelta >= 0; nDelta -= N_PAGE_SIZE)
                pages[(nShift + nDelta) / N_PAGE_SIZE] |= nBit;
        }
        i64 nBadPages = 0;
        for (auto page : pages) {
            nBadPages += page == 3;
            nTotalPages += page != 1;
        }
        DebugTraceMMgr("entry = %lld; size = %lld; free = %lld; system %lld; utilisation = %g%%, fragmentation = %g%%\n",
                       k, nSize, cs.FreeCount * nSize, csGB.FreeCount * nSize,
                       (N_CHUNK_SIZE - cs.FreeCount * nSize) * 100.0f / N_CHUNK_SIZE, 100.0f * nBadPages / Y_ARRAY_SIZE(pages));
        nTotalAllocated += N_CHUNK_SIZE;
        nTotalFree += cs.FreeCount * nSize;
        nTotalBadPages += nBadPages;
    }
    SystemFree(entries);
    DebugTraceMMgr("Total allocated = %llu, free = %lld, system = %lld, locked for future use %lld, utilisation = %g, fragmentation = %g\n",
                   nTotalAllocated, nTotalFree, nTotalGroupBlocks, nTotalLocked,
                   100.0f * (nTotalAllocated - nTotalFree) / nTotalAllocated, 100.0f * nTotalBadPages / nTotalPages);
    DebugTraceMMgr("Total %lld bytes used, %lld bytes in used pages\n", nTotalUsed, nTotalPages * N_PAGE_SIZE);

    for (int nSizeIdx = 0; nSizeIdx < N_SIZES; ++nSizeIdx)
        globalFreeLists[nSizeIdx].ReturnWholeList(wholeLists[nSizeIdx]);
    blockFreeList.ReturnWholeList(bfList);
}

void FlushThreadFreeList() {
    if (pThreadInfo)
        MoveSingleThreadFreeToGlobal(pThreadInfo);
}

void DumpMemoryBlockUtilization() {
    // move current thread free to global lists to get better statistics
    FlushThreadFreeList();
    {
        CCriticalSectionLockMMgr ls;
        DumpMemoryBlockUtilizationLocked();
    }
}

//////////////////////////////////////////////////////////////////////////
// malloc api

static bool LFAlloc_SetParam(const char* param, const char* value) {
    if (!strcmp(param, "LB_LIMIT_TOTAL_SIZE")) {
        LB_LIMIT_TOTAL_SIZE = atoi(value);
        return true;
    }
    if (!strcmp(param, "LB_LIMIT_TOTAL_SIZE_BYTES")) {
        LB_LIMIT_TOTAL_SIZE = (atoi(value) + N_PAGE_SIZE - 1) / N_PAGE_SIZE;
        return true;
    }
#ifdef DBG_FILL_MEMORY
    if (!strcmp(param, "FillMemoryOnAllocation")) {
        FillMemoryOnAllocation = !strcmp(value, "true");
        return true;
    }
#endif
    if (!strcmp(param, "BeforeLFAllocGlobalLockAcquired")) {
        BeforeLFAllocGlobalLockAcquired = (decltype(BeforeLFAllocGlobalLockAcquired))(value);
        return true;
    }
    if (!strcmp(param, "AfterLFAllocGlobalLockReleased")) {
        AfterLFAllocGlobalLockReleased = (decltype(AfterLFAllocGlobalLockReleased))(value);
        return true;
    }
    if (!strcmp(param, "EnterCritical")) {
        assert(value);
        RealEnterCritical = (decltype(RealEnterCritical))(value);
        return true;
    }
    if (!strcmp(param, "LeaveCritical")) {
        assert(value);
        RealLeaveCritical = (decltype(RealLeaveCritical))(value);
        return true;
    }
    if (!strcmp(param, "TransparentHugePages")) {
        TransparentHugePages = !strcmp(value, "true");
        return true;
    }
    if (!strcmp(param, "MapHugeTLB")) {
        MapHugeTLB = !strcmp(value, "true");
        return true;
    }
    if (!strcmp(param, "EnableDefrag")) {
        EnableDefrag = !strcmp(value, "true");
        return true;
    }
    return false;
};

static const char* LFAlloc_GetParam(const char* param) {
    struct TParam {
        const char* Name;
        const char* Value;
    };

    static const TParam Params[] = {
        {"GetLFAllocCounterFast", (const char*)&GetLFAllocCounterFast},
        {"GetLFAllocCounterFull", (const char*)&GetLFAllocCounterFull},
#if defined(LFALLOC_DBG)
        {"SetThreadAllocTag", (const char*)&SetThreadAllocTag},
        {"SetProfileCurrentThread", (const char*)&SetProfileCurrentThread},
        {"SetProfileAllThreads", (const char*)&SetProfileAllThreads},
        {"SetAllocationSamplingEnabled", (const char*)&SetAllocationSamplingEnabled},
        {"SetAllocationSampleRate", (const char*)&SetAllocationSampleRate},
        {"SetAllocationSampleMaxSize", (const char*)&SetAllocationSampleMaxSize},
        {"SetAllocationCallback", (const char*)&SetAllocationCallback},
        {"SetDeallocationCallback", (const char*)&SetDeallocationCallback},
        {"GetPerTagAllocInfo", (const char*)&GetPerTagAllocInfo},
#endif // LFALLOC_DBG
    };

    for (int i = 0; i < Y_ARRAY_SIZE(Params); ++i) {
        if (strcmp(param, Params[i].Name) == 0) {
            return Params[i].Value;
        }
    }
    return nullptr;
}

static Y_FORCE_INLINE void* LFVAlloc(size_t size) {
    const size_t pg = N_PAGE_SIZE;
    size_t bigsize = (size + pg - 1) & (~(pg - 1));
    void* p = LFAlloc(bigsize);

    Y_ASSERT_NOBT((intptr_t)p % N_PAGE_SIZE == 0);
    return p;
}

static Y_FORCE_INLINE int LFPosixMemalign(void** memptr, size_t alignment, size_t size) {
    if (Y_UNLIKELY(alignment > 4096)) {
#ifdef _win_
        OutputDebugStringA("Larger alignment are not guaranteed with this implementation\n");
#else
        fprintf(stderr, "Larger alignment are not guaranteed with this implementation\n");
#endif
        NMalloc::AbortFromCorruptedAllocator();
    }
    size_t bigsize = size;
    if (bigsize <= alignment) {
        bigsize = alignment;
    } else if (bigsize < 2 * alignment) {
        bigsize = 2 * alignment;
    }
    *memptr = LFAlloc(bigsize);
    return 0;
}
#endif
