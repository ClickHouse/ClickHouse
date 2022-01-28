#pragma once

//
// Mostly wait free, huge page optimized memory allocator
//
// Allocations are divided into 3 types
//  * Huge, > 512mb
//  * Large, > = 256k
//  * Others
//
// ** Huge allocations **
// Huge allocations are allocated and freed in GA * () functions using mmap (), the sizes are 
// stored in a flat, fixed-size lockfree hash. There are no thread local caches, all the largest
// allocations are forwarded to the system. The size is rounded up to huge pages.
//
// ** Large allocations **
// Large allocations are handled by the LargeAlloc () / LargeFree () functions. At startup, we 
// pre-allocate 640gb of virtual memory (LargeMemoryPtr) and cut large allocations from it. All
// memory is divided into large blocks of 8mb each. Blocks are combined into large groups of 64
// large blocks. Large allocations are divided into two types - more than 4mb and less. For 
// allocations larger than 4mb, the size is rounded up to an integer number of blocks and we
// look for a group in LargeGroupInfo[] that has room for allocation. Allocations less than 4mb
// are allocated entirely within one large block. The size is rounded up to an integer number of
// chunks (large chunk, 128kb). Each thread keeps the large block from which the previous
// allocation was made (PrevThreadAllocLargeBlockId). First we try to allocate from this block.
// If there is no room, then we look for a new suitable block in the global array.
// 
// ** Other allocations **
// All other allocations come from "segments". A segment is 2MB of memory, ideally, located in
// one huge page. This is achieved with aligned mmap () and MADV_HUGEPAGE. At startup, 640gb of
// virtual memory is reserved for the array of segments. Pointer to this array is AllMemoryPtr.
// 
// Each segment is divided into 64 blocks. Each block can be further divided into allocations
// for small blocks (small) or into several (up to 64) chunks with 64 allocations each for tiny
// ones. Thus, segment allocations are divided into three types:
//  * medium (>=32k)
//  * small (>=512 bytes)
//  * tiny (<512 bytes)
// 
// For each segment, TSegmentInfo contains a bitmask of free blocks. The first block is always
// occupied by the segment header (TSegmentHeader). Since the first block is always occupied,
// the first bit of the bitmask is used for the segment "owning" flag (SEGMENT_ACTIVE flag).
// 
// Each thread has one current segment. Each segment can only be current for one thread.
// SwitchSegment() function finds a new segment for the current thread. The new segment should
// have room for len blocks in a row and also MIN_FREE_BLOCKS blocks. ReleaseSegmentOwnership()
// makes the segment belong to no thread, release the "ownership" of the segment.
// 
// All allocations smaller than 32kb are rounded to the power of two. Block can contain
// allocations of the same size or block can also be the beginning of a medium-sized allocation.
// The allocation size for a block is in the CurAllocLevel [] array in the segment header
// (TSegmentHeader). Besides the power of two of size, there can be special AL_ * values in
// CurAllocLevel[].
// 
// For medium allocations, atomic operations on TSegmentInfo are sufficient. Implemented in
// SegmentMedium*() functions. BitMaskFind() finds a place in a bitmask where several bits are
// set in a row.
// 
// Smaller allocations divide one block into several allocations. There are 2 cases. Simple
// (64 or fewer allocations in one block) and complex (>64 allocations). Both options use the
// functions of allocating and freeing whole blocks - Block*().
// 
// In the simple case (<= 64 allocations in one block), atomic operations are enough on a 64-bit
// bitmask of free space in the block. These bitmasks are called BlockFreeBits[] and are stored
// at the beginning of the segment in the second 512 bytes of the first block. Implemented in
// SegmentSmall*().
// 
// In a more complex case, a 2-level system of dividing the block into allocations is used. The
// block is divided into chunks of 64 allocations each. In this case, BlockFreeBits[] stores
// the number of completely free chunks, and the allocation bitmasks themselves are in
// MemFreeBits[]. MemFreeBits occupy the first block of the segment. Since the second 512 bytes
// of the segment are already occupied by BlockFreeBits[], tiny allocations are possible only
// starting from the third block of the segment (ALL_2LAYER_BLOCKS contains all blocks
// available for such allocations).
// 
// Since allocations and releases are possible from different threads, all bitmask operations
// are implemented using atomic operations. To make everything work faster for allocations
// (and frees of recently allocated), thread-local bitmasks are used. Since these bitmasks are
// used from one thread, operations with them are not atomic. These local bitmasks are stored
// in the segment header in TSegmentSizeContext structures. When we release ownership of the
// segment, local allocation caches are transferred to global bitmasks in the
// DumpLocalAllocMasksLocked () function.
// 
// In the case when we have freed all allocations inside the block, we need to mark the block
// as free. This can be done without races only by "owning" the segment. If we do not have
// ownership, we set the required bit in the RetestBlockMask. Blocks from this mask will be
// processed when the thread owning segment is looking for a free block or when other thread
// is releasing the segment ownership, or we ourselves will seize the segment's ownership and
// release the blocks using ProcessRetestMaskLocked().
// 
// In order to free unused memory into the system, one can run a special thread (RunHuReclaim())
// which will watch once a second how much "extra" memory is used and if there is a lot of
// committed but not used memory (more than ReclaimKeepSize), then this thread will release to
// the system at most ReclaimMaxReclaim bytes per second. Memory is returned to the system
// separately from the pool for large allocations and from the pool of segments.
// 

#include <atomic>
#include <cassert>
#include <cstdint>
#include <algorithm>
#include <string.h>

#ifdef NDEBUG
#define Y_ASSERT(expr) sizeof(expr)
#else
#define Y_ASSERT assert
#endif

typedef uint8_t ui8;
typedef uint16_t ui16;
typedef uint64_t ui64;
typedef int64_t yint;

using std::max;
using std::min;

#ifdef _MSC_VER
#define _win_
#define WIN32_LEAN_AND_MEAN
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <intrin.h>
#include <Windows.h>

#define PERTHREAD __declspec(thread)
#define NOINLINE __declspec(noinline)
#define THREAD_FUNC_RETURN DWORD
#define SchedYield SwitchToThread

#else
#include <immintrin.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sched.h>
#include <unistd.h>
#include <sys/utsname.h>

#define PERTHREAD __thread
#define NOINLINE __attribute__((noinline))
#define THREAD_FUNC_RETURN void*
#define SchedYield sched_yield

static __inline__ unsigned char __attribute__((__always_inline__, __nodebug__))
_BitScanForward64(unsigned long *_Index, unsigned long long _Mask)
{
    if (!_Mask)
        return 0;
    *_Index = __builtin_ctzll(_Mask);
    return 1;
}
static __inline__ unsigned char __attribute__((__always_inline__, __nodebug__))
_BitScanReverse64(unsigned long *_Index, unsigned long long _Mask)
{
    if (!_Mask)
        return 0;
    *_Index = 63 - __builtin_clzll(_Mask);
    return 1;
}

inline void Sleep(yint x)
{
    usleep(x * 1000);
}
#endif

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))
#endif


#undef PAGE_SIZE
// platform specific actually
#define PAGE_SIZE 4096
#define HUGE_PAGE_SIZE (1ull << 21)


static yint RoundSizeUp(yint sz, ui64 page)
{
    return (sz + page - 1) & ~(page - 1);
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
#ifdef _win_
// no huge pages for windows so far
static void* OsReserve(yint sz)
{
    return VirtualAlloc(0, sz, MEM_RESERVE, PAGE_NOACCESS);
}
static void OsCommit(void *p, yint sz)
{
    VirtualAlloc(p, sz, MEM_COMMIT, PAGE_READWRITE);
}
static void OsReclaim(void *p, yint sz)
{
    VirtualFree(p, sz, MEM_DECOMMIT);
}
static void* OsAlloc(yint *pSz)
{
    yint &sz = *pSz;
    return VirtualAlloc(0, sz, MEM_COMMIT, PAGE_READWRITE);
}
static void OsFree(void *p, yint sz)
{
    (void)sz;
    VirtualFree(p, 0, MEM_RELEASE); // size not needed
}
static void OsInit()
{
}

#else
static std::atomic<ui64> ReserveAttempt;
static yint HugePageAdvise;
static void* OsReserve(yint sz)
{
    char *res = 0;
    for(;;) {
        ui64 x = ReserveAttempt.fetch_add(1);
        ui64 rndVal = x * 0x83e385b21a346273ull + x * x * 0x554721eb2148deadull;
        ui64 alignment = HUGE_PAGE_SIZE - 1;
        char *ptr = (char*)((rndVal >> 17) & ~alignment); // high address bits are zero (48 bits for x64), low bits are zero for alignment
        res = (char*)mmap(ptr, sz, PROT_NONE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
        if (res == ptr) {
            break;
        }
        if (res != MAP_FAILED) {
            ui64 addr = (ui64)ptr;
            if ((addr & alignment) == 0) {
                // not required address, but result is aligned
                break;
            }
            munmap(res, sz);
        }
    }
    Y_ASSERT(res);
    if (HugePageAdvise != 0) {
        madvise(res, sz, HugePageAdvise);
    }
    return res;
}
static void OsCommit(void *p, yint sz)
{
    mprotect(p, sz, PROT_READ | PROT_WRITE);
}
static void OsReclaim(void *p, yint sz)
{
    //madvise(segment, SEGMENT_SIZE, MADV_DONTNEED); // super slow for no apparent reason, use MAP_FIXED to map new memory over non needed
    void *res = mmap(p, sz, PROT_NONE, MAP_FIXED | MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
    if (res != p) {
        abort(); // MAP_FIXED failed
    }
    if (HugePageAdvise != 0) {
        madvise(res, sz, HugePageAdvise);
    }
}
static void* OsAlloc(yint *pSz)
{
    yint &sz = *pSz;
    bool isHuge = false;
    if (sz > (yint)(2 * HUGE_PAGE_SIZE)) {
        // huge alloc
        sz = RoundSizeUp(sz, HUGE_PAGE_SIZE);
        isHuge = true;
    } else {
        // regular alloc
        sz = RoundSizeUp(sz, PAGE_SIZE);
    }
    void *res = mmap(0, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (isHuge && HugePageAdvise != 0) {
        madvise(res, sz, HugePageAdvise);
    }
    return res;
}
static void OsFree(void *p, yint sz)
{
    munmap(p, sz);
}
static void OsInit()
{
    HugePageAdvise = 0;
#ifdef MADV_HUGEPAGE
    HugePageAdvise = MADV_HUGEPAGE;
    utsname uu;
    uname(&uu);
    if (strncmp(uu.sysname, "Linux", 6) == 0 && atoi(uu.release) <= 4) {
        HugePageAdvise = 0; // on linux 4 and earlier THP may significantly slow down application
    }
#endif
}
#endif

static void* OsAlloc(yint szArg)
{
    yint sz = szArg;
    return OsAlloc(&sz);
}

// smallest power of 2 greater or equal to val
static yint CalcLog2(yint val)
{
    if (val <= 1) {
        return 0;
    }
    unsigned long xx;
    _BitScanReverse64(&xx, val - 1);
    return xx + 1ll;
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
static std::atomic<ui64> AllocatorIsInitialized;
static void hu_init();
static void hu_check_init()
{
    if (AllocatorIsInitialized != 1) {
        hu_init();
    }
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////
const yint GLOBAL_HASH_SIZE_LN = 20; // 4x+ number of blocks and more to avoid excessive hash lookups
const yint GLOBAL_HASH_SIZE = 1ll << GLOBAL_HASH_SIZE_LN;

struct TGAEntry
{
    std::atomic<ui64> Ptr;
    yint Size;
};
static TGAEntry *GAHash;


static void GAInit()
{
    GAHash = (TGAEntry*)OsAlloc(GLOBAL_HASH_SIZE * sizeof(TGAEntry));
}


static yint GAHashPtr(const void *ptr)
{
    ui64 x = (const char*)(ptr) - (const char*)nullptr;
    ui64 res = (0x4847badfea31337ull * x) >> (64 - GLOBAL_HASH_SIZE_LN);
    Y_ASSERT(res < GLOBAL_HASH_SIZE);
    return res;
}


NOINLINE static void* GAAlloc(yint szArg)
{
    if (szArg == 0) {
        return 0;
    }
    hu_check_init();
    yint sz = szArg;
    void *ptr = OsAlloc(&sz);
    if (ptr == 0) {
        abort(); // out of memory
    }
    yint k = GAHashPtr(ptr);
    for (yint kStart = k;;) {
        TGAEntry &ga = GAHash[k];
        ui64 oldPtr = ga.Ptr.load();
        if (oldPtr <= 1) {
            ui64 newPtr = (ui64)ptr;
            if (ga.Ptr.compare_exchange_strong(oldPtr, newPtr)) {
                ga.Size = sz;
                return ptr;
            }
            continue;
        }
        k = (k + 1) & (GLOBAL_HASH_SIZE - 1);
        if (k == kStart) {
            break;
        }
    }
    abort(); // out of hash
    return 0;
}


static void GAFree(void *ptr)
{
    if (ptr == 0) {
        return;
    }
    yint k = GAHashPtr(ptr);
    yint sz = 0;
    for (;;) {
        TGAEntry &ga = GAHash[k];
        void *gaPtr = (void*)ga.Ptr.load();
        if (gaPtr == ptr) {
            sz = ga.Size;
            ga.Ptr = 1;
            break;
        }
        if (gaPtr == 0) {
            abort(); // not found
        }
        k = (k + 1) & (GLOBAL_HASH_SIZE - 1);
    }
    OsFree(ptr, sz);
}


static yint GAGetSize(const void *ptr)
{
    if (ptr == 0) {
        return 0;
    }
    yint k = GAHashPtr(ptr);
    for (;;) {
        const TGAEntry &ga = GAHash[k];
        void *gaPtr = (void*)ga.Ptr.load();
        if (gaPtr == ptr) {
            return ga.Size;
        }
        if (gaPtr == 0) {
            break;
        }
        k = (k + 1) & (GLOBAL_HASH_SIZE - 1);
    }
    abort(); // not found
    return 0;
}


//////////////////////////////////////////////////////////////////////////////
const yint BIT_MASK_MAX = 64;
const yint BIT_SHIFT_ARR_SIZE = 8;
static ui8 BitMaskShift[BIT_MASK_MAX + 1][BIT_SHIFT_ARR_SIZE];
static void BitMaskInit()
{
    for (yint sz = 0; sz <= BIT_MASK_MAX; ++sz) {
        ui8 *dst = BitMaskShift[sz];
        yint cur = sz;
        for (yint i = 0; i < BIT_SHIFT_ARR_SIZE; ++i) {
            yint next = (cur + 1) / 2;
            dst[i] = cur - next;
            cur = next;
        }
    }
}

static bool BitMaskFind(ui64 mask, yint sz, unsigned long *index)
{
    const ui8 *ss = BitMaskShift[sz];
    ui64 ms = mask;
    ms = ms & (ms >> ss[0]);
    ms = ms & (ms >> ss[1]);
    ms = ms & (ms >> ss[2]);
    if (ss[3]) { // shortcut for small sizes, faster on average
        ms = ms & (ms >> ss[3]);
        ms = ms & (ms >> ss[4]);
        ms = ms & (ms >> ss[5]);
    }
    Y_ASSERT(ss[6] == 0);
    return _BitScanForward64(index, ms);
}


//////////////////////////////////////////////////////////////////////////////////////////////
// Large allocs, 2 layer, group consists of 64 blocks, block consists of 64 chunks
const yint LARGE_CHUNK_SIZE_LN = 17;
const yint LARGE_CHUNK_SIZE = 1ll << LARGE_CHUNK_SIZE_LN;
const yint LARGE_BLOCK_SIZE_LN = LARGE_CHUNK_SIZE_LN + 6;
const yint LARGE_BLOCK_SIZE = 1ll << LARGE_BLOCK_SIZE_LN;
const yint LARGE_GROUP_SIZE_LN = LARGE_BLOCK_SIZE_LN + 6;
const yint LARGE_GROUP_SIZE = 1ll << LARGE_GROUP_SIZE_LN;
const yint LARGE_GROUP_COUNT = (1ull << (30 - LARGE_GROUP_SIZE_LN)) * 640; // 640 gb ought to be enough for anybody
const yint LARGE_BLOCK_COUNT = LARGE_GROUP_COUNT * 64;
const yint LARGE_CHUNK_COUNT = LARGE_BLOCK_COUNT * 64;
const yint HUGE_SIZE_LN = LARGE_GROUP_SIZE_LN;

const yint LARGE_MAX_CHUNK_ALLOC_LEN = 32;
const yint LARGE_MIN_FREE_CHUNKS = 32;

struct TLargeGroupInfo
{
    std::atomic<ui64> FreeBlockMask;
    std::atomic<ui64> CommitedMask;
    std::atomic<ui64> FreeBlockCounters;
    ui64 Padding[5];
};

struct TLargeBlockInfo
{
    std::atomic<ui64> FreeChunkMask;
};


static char *LargeMemoryPtr;
static TLargeGroupInfo *LargeGroupInfo;
static TLargeBlockInfo *LargeBlockInfo;
static ui16 *LargeAllocChunkCount;
static PERTHREAD yint PrevThreadAllocLargeBlockId;


static void LargeInit()
{
    Y_ASSERT(LargeMemoryPtr == 0); // call init once
    LargeMemoryPtr = (char*)OsReserve(LARGE_GROUP_COUNT * LARGE_GROUP_SIZE);
    LargeGroupInfo = (TLargeGroupInfo*)OsAlloc(sizeof(TLargeGroupInfo) * LARGE_GROUP_COUNT);
    LargeBlockInfo = (TLargeBlockInfo*)OsAlloc(sizeof(TLargeBlockInfo) * LARGE_BLOCK_COUNT);
    LargeAllocChunkCount = (ui16*)OsAlloc(sizeof(LargeAllocChunkCount[0]) * LARGE_CHUNK_COUNT);
    for (yint i = 0; i < LARGE_GROUP_COUNT; ++i) {
        LargeGroupInfo[i].FreeBlockMask = -1ll;
        LargeGroupInfo[i].FreeBlockCounters = 0x8080808080808080ull;
    }
}


static ui64 MakeAllocMask(ui8 len)
{
    if (len == 64) {
        return -1ll;
    }
    return  (1ull << len) - 1ull;
}


static void LargeMemoryCommit(yint g, yint block, yint len)
{
    TLargeGroupInfo &gg = LargeGroupInfo[g];
    ui64 allocMask = MakeAllocMask(len) << block;
    if ((gg.CommitedMask & allocMask) == 0) {
        char *ptr = LargeMemoryPtr + (g * 64 + block) * LARGE_BLOCK_SIZE;
        OsCommit(ptr, len * LARGE_BLOCK_SIZE);
        gg.CommitedMask.fetch_add(allocMask);
    } else if ((gg.CommitedMask & allocMask) != allocMask) {
        for (yint z = block; z < block + len; ++z) {
            ui64 bit = 1ull << z;
            if ((gg.CommitedMask & bit) == 0) {
                char *ptr = LargeMemoryPtr + (g * 64 + z) * LARGE_BLOCK_SIZE;
                OsCommit(ptr, LARGE_BLOCK_SIZE);
                gg.CommitedMask.fetch_add(bit);
            }
        }
    }
}


// callback on changes in free chunk mask
static void LargeFreeChunkMaskHasChanged(yint largeBlockId, ui64 oldMask, ui64 newMask)
{
    ui64 posOldMask = oldMask;
    ui64 posNewMask = newMask;
    yint oldFree = _mm_popcnt_u64(oldMask);
    yint newFree = _mm_popcnt_u64(newMask);
    ui64 fbDelta = 0;
    for (yint lenLn = 0; lenLn < 6; ++lenLn) {
        yint len = 1ull << lenLn;
        yint oldOk = (posOldMask != 0) & (oldFree >= LARGE_MIN_FREE_CHUNKS + len);
        yint newOk = (posNewMask != 0) & (newFree >= LARGE_MIN_FREE_CHUNKS + len);
        fbDelta += (newOk - oldOk) << (lenLn * 8);
        posOldMask = posOldMask & (posOldMask >> len);
        posNewMask = posNewMask & (posNewMask >> len);
    }
    if (fbDelta != 0) {
        TLargeGroupInfo &gg = LargeGroupInfo[largeBlockId / 64];
        gg.FreeBlockCounters.fetch_add(fbDelta);
    }
}


static void LargeAddFreeChunkMask(yint largeBlockId, ui64 freeMask)
{
    yint largeGroupId = largeBlockId >> 6;
    TLargeBlockInfo &bmask = LargeBlockInfo[largeBlockId];
    Y_ASSERT((bmask.FreeChunkMask & freeMask) == 0);
    ui64 oldMask = bmask.FreeChunkMask.fetch_add(freeMask);
    LargeFreeChunkMaskHasChanged(largeBlockId, oldMask, oldMask + freeMask);
    ui64 fullMask = -1ll;
    if (oldMask + freeMask == fullMask) {
        if (bmask.FreeChunkMask.compare_exchange_strong(fullMask, 0)) {
            ui64 blockMask = 1ull << (largeBlockId & 63);
            TLargeGroupInfo &gg = LargeGroupInfo[largeGroupId];
            Y_ASSERT((gg.FreeBlockMask & blockMask) == 0);
            gg.FreeBlockMask.fetch_add(blockMask);
            gg.FreeBlockCounters.fetch_add(-0x0101010101010101ll);
        }
    }
}


static bool LargeTryAllocChunks(yint largeBlockId, yint chunkCount, yint minAdditionalFreeChunks, char **res)
{
    yint len = chunkCount;
    TLargeBlockInfo& bmask = LargeBlockInfo[largeBlockId];
    ui64 freeMask = bmask.FreeChunkMask;
    if (_mm_popcnt_u64(freeMask) < (minAdditionalFreeChunks + len)) {
        return false;
    }
    unsigned long freeChunkPtr;
    if (!BitMaskFind(freeMask, len, &freeChunkPtr)) {
        return false;
    }
    ui64 allocMask = MakeAllocMask(len) << freeChunkPtr;
    Y_ASSERT((freeMask & allocMask) == allocMask);
    ui64 newMask = freeMask ^ allocMask;
    ui64 expected = freeMask;
    if (bmask.FreeChunkMask.compare_exchange_weak(expected, newMask)) {
        yint largeChunkId = largeBlockId * 64 + freeChunkPtr;
        LargeFreeChunkMaskHasChanged(largeBlockId, freeMask, newMask);
        LargeAllocChunkCount[largeChunkId] = chunkCount;
        *res = LargeMemoryPtr + largeChunkId * LARGE_CHUNK_SIZE;
        return true;
    }
    return false;
}


static void* LargeAllocChunks(yint chunkCount)
{
    char* res;
    if (LargeTryAllocChunks(PrevThreadAllocLargeBlockId, chunkCount, 0, &res)) {
        return res;
    }
    // select new block
    yint len = chunkCount;
    yint lenLn = CalcLog2(len);
    ui64 countersMask = 0x7full << (lenLn * 8);
    for (yint g = 0; g < LARGE_GROUP_COUNT; ++g) {
        TLargeGroupInfo& gg = LargeGroupInfo[g];
        if (gg.FreeBlockCounters & countersMask) {
            // there is space for len blocks
            for (yint b = 0; b < 64; ++b) {
                yint largeBlockId = g * 64 + b;
                if (LargeTryAllocChunks(largeBlockId, chunkCount, LARGE_MIN_FREE_CHUNKS, &res)) {
                    PrevThreadAllocLargeBlockId = largeBlockId;
                    return res;
                }
            }
        }
        if (gg.FreeBlockMask != 0) {
            ui64 toSplit = gg.FreeBlockMask.exchange(0);
            if (toSplit != 0) {
                yint count = 0;
                unsigned long freeBlockPtr;
                while (_BitScanForward64(&freeBlockPtr, toSplit)) {
                    LargeMemoryCommit(g, freeBlockPtr, 1);
                    TLargeBlockInfo& bmask = LargeBlockInfo[g * 64 + freeBlockPtr];
                    Y_ASSERT(bmask.FreeChunkMask == 0);
                    bmask.FreeChunkMask = -1ll;
                    ++count;
                    toSplit ^= 1ull << freeBlockPtr;
                }
                gg.FreeBlockCounters.fetch_add(count * 0x0101010101010101ll);
                --g;
            }
        }
    }
    abort(); // out of large memory
}


static void* LargeAllocBlocks(yint chunkCount)
{
    yint len = (chunkCount + 63) / 64;
    for (yint g = 0; g < LARGE_GROUP_COUNT; ++g) {
        TLargeGroupInfo& gg = LargeGroupInfo[g];
        ui64 freeMask = gg.FreeBlockMask;
        if (_mm_popcnt_u64(freeMask) < len) {
            continue;
        }
        unsigned long freeBlockPtr;
        if (!BitMaskFind(freeMask, len, &freeBlockPtr)) {
            continue;
        }
        ui64 expectedMask = freeMask;
        ui64 allocMask = MakeAllocMask(len) << freeBlockPtr;
        Y_ASSERT((freeMask & allocMask) == allocMask);
        if (gg.FreeBlockMask.compare_exchange_weak(expectedMask, freeMask ^ allocMask)) {
            yint largeBlockId = g * 64 + freeBlockPtr;
            yint largeChunkId = largeBlockId * 64;
            LargeAllocChunkCount[largeChunkId] = chunkCount;
            LargeMemoryCommit(g, freeBlockPtr, len);
            char* res = LargeMemoryPtr + largeBlockId * LARGE_BLOCK_SIZE;
            return res;
        }
    }
    abort(); // out of large memory
}


NOINLINE static void* LargeAlloc(yint sz)
{
    if (sz == 0) {
        return 0;
    }
    hu_check_init();
    yint chunkCount = (sz + LARGE_CHUNK_SIZE - 1) / LARGE_CHUNK_SIZE;
    if (chunkCount > LARGE_MAX_CHUNK_ALLOC_LEN) {
        return LargeAllocBlocks(chunkCount);
    } else {
        return LargeAllocChunks(chunkCount);
    }
}


static void LargeFree(void *p)
{
    yint largeChunkId = ((char*)p - LargeMemoryPtr) / LARGE_CHUNK_SIZE;
    yint largeBlockId = largeChunkId >> 6;
    yint largeGroupId = largeBlockId >> 6;
    yint chunkCount = LargeAllocChunkCount[largeChunkId];
    if (chunkCount > LARGE_MAX_CHUNK_ALLOC_LEN) {
        yint len = (chunkCount + 63) / 64;
        ui64 allocMask = MakeAllocMask(len) << (largeBlockId & 63);
        TLargeGroupInfo &gg = LargeGroupInfo[largeGroupId];
        Y_ASSERT((gg.FreeBlockMask & allocMask) == 0);
        gg.FreeBlockMask.fetch_add(allocMask);
    } else {
        yint len = chunkCount;
        ui64 allocMask = MakeAllocMask(len) << (largeChunkId & 63);
        LargeAddFreeChunkMask(largeBlockId, allocMask);
    }
}


static yint LargeGetSize(const void *p)
{
    yint largeChunkId = ((char*)p - LargeMemoryPtr) / LARGE_CHUNK_SIZE;
    yint sz = LargeAllocChunkCount[largeChunkId] * LARGE_CHUNK_SIZE;
    return sz;
}


static void LargeReclaim(yint keepSize, yint maxReclaim)
{
    if (maxReclaim == 0) {
        return;
    }
    yint goodForReclaim = 0;
    for (yint g = 0; g < LARGE_GROUP_COUNT; ++g) {
        TLargeGroupInfo &gg = LargeGroupInfo[g];
        goodForReclaim += _mm_popcnt_u64(gg.FreeBlockMask & gg.CommitedMask);
    }
    yint reclaimBlocks = min(goodForReclaim - keepSize / LARGE_BLOCK_SIZE, maxReclaim / LARGE_BLOCK_SIZE);
    for (yint g = LARGE_GROUP_COUNT - 1; reclaimBlocks > 0 && g >= 0; --g) {
        TLargeGroupInfo &gg = LargeGroupInfo[g];
        if (gg.FreeBlockMask & gg.CommitedMask) {
            ui64 freeMask = gg.FreeBlockMask.exchange(0); // alloc to prevent races
            ui64 mask = freeMask & gg.CommitedMask;
            reclaimBlocks -= _mm_popcnt_u64(mask);
            gg.CommitedMask &= ~mask;
            unsigned long z;
            while (_BitScanForward64(&z, mask)) {
                char *ptr = LargeMemoryPtr + (g * 64 + z) * LARGE_BLOCK_SIZE;
                OsReclaim(ptr, LARGE_BLOCK_SIZE);
                mask ^= 1ull << z;
            }
            Y_ASSERT((gg.FreeBlockMask & freeMask) == 0);
            gg.FreeBlockMask.fetch_add(freeMask);
        }
    }
}


//////////////////////////////////////////////////////////////////////////////////////////////
// Allocations from segments
//
// basic principles:
//   whenever possible alloc from local masks to avoid atomic operations
//   some operations require segment ownership, if we have no ownership we set RetestMask which is checked on segment release, acquire and other cases
//   Locked suffix indicates that function is called on the locked segment, segment can be locked by single thread only
//   bit SEGMENT_ACTIVE indicates that segment is locked
//

const yint SEGMENT_SIZE_LN = 21;
const yint SEGMENT_SIZE = 1ll << SEGMENT_SIZE_LN;
const yint SEGMENT_COUNT = (1ull << (30 - SEGMENT_SIZE_LN)) * 640; // 640 gb ought to be enough for anybody
const yint LARGE_SIZE_LN = SEGMENT_SIZE_LN - 3;
const yint BLOCK_SIZE_LN = SEGMENT_SIZE_LN - 6;
const yint BLOCK_SIZE = 1 << BLOCK_SIZE_LN;
const yint NEED_2LAYER = BLOCK_SIZE_LN - 6;
const yint MIN_POW2 = SEGMENT_SIZE_LN - 18;
const yint MIN_ALLOC = 1ull << MIN_POW2;
const yint BITS_OFFSET_MULT = BLOCK_SIZE / 64 / sizeof(ui64); // freebits per block size

const yint BLOCK_COUNT = 64;
const ui64 ALL_BLOCKS = ~1ull; // first is header and always occupied
const ui64 ALL_2LAYER_BLOCKS = ~3ull; // first is header, second is forbidden, since it's MemFreeBits are occupied by BlockFreeBits
const ui64 ALL_BITS = (ui64)-1ll;
const ui64 SEGMENT_ACTIVE = 1; // segment is used by one of the threads, using first bit since first block can not be free

const yint SEGMENT_GROUP_COUNT = SEGMENT_COUNT / 64;

// split all segments in groups of SEGMENT_HIER_SIZE size, for each group
// count number of segments with sufficient number of free segments and contigous free space for medium allocations of different sizes
// allows skipping SEGMENT_HIER_SIZE at once when searching for suitable segment
const yint SEGMENT_HIER_SIZE = 32; // must be power of 2
const yint SEGMENT_HIER_SIZE2 = 512; // more layers for even larger skips
const yint SEGMENT_HIER_SIZE3 = 8192;

// trade locality for volume
//const yint MIN_FREE_BLOCKS = 16;
const yint MIN_FREE_BLOCKS = 8;
//const yint MIN_FREE_BLOCKS = 1;


struct TSegmentInfo
{
    std::atomic<ui64> GlobalFreeBlockMask;
    std::atomic<ui64> HierFreeCount;
    std::atomic<ui64> HierFreeCount2;
    std::atomic<ui64> HierFreeCount3;
    ui64 Padding[4];
};


struct TSegmentGroupInfo
{
    std::atomic<ui64> CommitedMask;
    std::atomic<ui64> GoodForReclaimMask;
};


struct TSegmentSizeContext
{
    ui64 CanAllocBlockMask; // set bit means allocation from this block is or was possible
    ui64 LocalFreeAllocMask; // avoid atomics for most allocs
    int ChunkOffset; // pointer to the current chunk within segment
    int ChunkOffsetMask;

    void Init()
    {
        CanAllocBlockMask = 0;
        LocalFreeAllocMask = 0;
        ChunkOffset = -1;
        ChunkOffsetMask = 0;
    }
};


const ui8 AL_FREE = 0;
const ui8 AL_SYSTEM = 0x7f;
const ui8 AL_MEDIUM_ALLOC = 0x77;

struct TSegmentHeader
{
    ui8 CurAllocLevel[BLOCK_COUNT]; // secondary structure, just to determine allocation size, primary info if block is free is stored at GlobalFreeBlockMask
    std::atomic<ui64> RetestBlockMask;
    ui64 Padding[7];
    TSegmentSizeContext SizeCtx[BLOCK_SIZE_LN];
    ui64 LocalFreeBlockMask;

    void Init()
    {
        CurAllocLevel[0] = AL_SYSTEM; // first block is occupied by header and bit masks
        for (yint i = 1; i < (yint)ARRAY_SIZE(CurAllocLevel); ++i) {
            CurAllocLevel[i] = AL_FREE;
        }
        for (yint i = 0; i < (yint)ARRAY_SIZE(SizeCtx); ++i) {
            SizeCtx[i].Init();
        }
        LocalFreeBlockMask = 0;
        RetestBlockMask = 0;
    }
};


struct TGlobalSizeContext
{
    yint ChunkCount = 0;
    ui64 AllFree = 0;
    yint MinFreeCount = 0;
};


// pointer to segmented memory
static char *SegmentMemoryPtr;
static TSegmentInfo *SegmentInfo;
static TSegmentGroupInfo *SegmentGroupInfo;
static TGlobalSizeContext GlobalSizeCtx[BLOCK_SIZE_LN + 1];
static PERTHREAD char* pThreadSegment;
static PERTHREAD bool IsStoppingThread;
static yint MaxSegmentId = 0;
#ifdef _win_
static DWORD ThreadDestructionTracker;
#else
static pthread_key_t ThreadDestructionTracker;
#endif


static void OnThreadDestruction(void *lpFlsData);

static void SegmentInit()
{
    Y_ASSERT(sizeof(std::atomic<ui64>) == sizeof(ui64));
    Y_ASSERT(SegmentMemoryPtr == 0); // call init once

    yint segInfoSize = sizeof(TSegmentInfo) * SEGMENT_COUNT;
    segInfoSize = RoundSizeUp(segInfoSize, HUGE_PAGE_SIZE);
    SegmentInfo = (TSegmentInfo*)OsReserve(segInfoSize); // guaranteed huge page alignment
    OsCommit(SegmentInfo, segInfoSize);
    SegmentGroupInfo = (TSegmentGroupInfo*)OsAlloc(sizeof(TSegmentGroupInfo) * SEGMENT_GROUP_COUNT);
    SegmentMemoryPtr = (char*)OsReserve(SEGMENT_COUNT * SEGMENT_SIZE);

    Y_ASSERT(BITS_OFFSET_MULT * sizeof(ui64) * 8 >= BLOCK_SIZE / MIN_ALLOC); // MemFreeBits has space for one bit per MIN_ALLOC
    Y_ASSERT(BITS_OFFSET_MULT * BLOCK_COUNT * sizeof(ui64) <= BLOCK_SIZE); // MemFreeBits fits into first block
    Y_ASSERT(BITS_OFFSET_MULT * sizeof(ui64) >= sizeof(ui64) * BLOCK_COUNT); // one block MemFreeBits is sufficient for BlockFreeBits storage
    Y_ASSERT(sizeof(TSegmentHeader) <= BITS_OFFSET_MULT * sizeof(ui64));

    for (yint i = 0; i < BLOCK_SIZE_LN; ++i) {
        TGlobalSizeContext &ctx = GlobalSizeCtx[i];
        if (i < NEED_2LAYER) {
            ctx.ChunkCount = 1ll << (BLOCK_SIZE_LN - i - 6);
            ctx.AllFree = 0;
            for (yint z = 0; z < ctx.ChunkCount; ++z) {
                ctx.AllFree |= 1ll << z;
            }
        } else {
            int count = 1ll << (BLOCK_SIZE_LN - i);
            ctx.AllFree = 0;
            for (yint z = 0; z < count; ++z) {
                ctx.AllFree |= 1ll << z;
            }
            ctx.ChunkCount = 1;
            //ctx.MinFreeCount = 1024 / (1ll << i);
            ctx.MinFreeCount = 2048 / (1ll << i);
            //ctx.MinFreeCount = 4096 / (1ll << i);
        }
    }

    for (yint i = 0; i < SEGMENT_COUNT; ++i) {
        SegmentInfo[i].GlobalFreeBlockMask = ALL_BLOCKS;
        SegmentInfo[i].HierFreeCount = SEGMENT_HIER_SIZE;
        SegmentInfo[i].HierFreeCount2 = SEGMENT_HIER_SIZE2 / SEGMENT_HIER_SIZE;
        SegmentInfo[i].HierFreeCount3 = SEGMENT_HIER_SIZE3 / SEGMENT_HIER_SIZE2;
    }
#ifdef _win_
    ThreadDestructionTracker = FlsAlloc(OnThreadDestruction);
#else
    pthread_key_create(&ThreadDestructionTracker, OnThreadDestruction);
#endif
}


inline yint GetSegmentId(char *segment)
{
    return (segment - SegmentMemoryPtr) >> SEGMENT_SIZE_LN;
}

inline char* GetSegmentData(yint segmentId)
{
    return SegmentMemoryPtr + (segmentId << SEGMENT_SIZE_LN);
}

// returns -1 for -1 chunkOffset
inline yint GetBlockId(yint chunkOffset)
{
    return chunkOffset >> BLOCK_SIZE_LN;
}

inline yint GetChunkId(yint chunkOffset, ui8 pow2)
{
    return (chunkOffset & (BLOCK_SIZE - 1)) >> (pow2 + 6);
}


/////////////////////////////////////////////////////////////////////////////////
// track segment memory commitment status

// returns true if new memory allocated
static bool SegmentCommitLocked(char *segment)
{
    yint segmentId = GetSegmentId(segment);
    TSegmentGroupInfo &gg = SegmentGroupInfo[segmentId / 64];
    ui64 bit = 1ull << (segmentId & 63);
    gg.GoodForReclaimMask &= ~bit;
    if (gg.CommitedMask & bit) {
        return false;
    }
    OsCommit(segment, SEGMENT_SIZE);
    gg.CommitedMask.fetch_add(bit);
    return true;
}


static void SegmentMarkForReclaimLocked(yint segmentId)
{
    TSegmentGroupInfo &gg = SegmentGroupInfo[segmentId / 64];
    ui64 bit = 1ull << (segmentId & 63);
    gg.GoodForReclaimMask |= bit;
}


static void SegmentReclaim(yint keepSize, yint maxReclaim)
{
    if (maxReclaim == 0) {
        return;
    }
    yint goodForReclaim = 0;
    for (yint g = 0; g < SEGMENT_GROUP_COUNT; ++g) {
        TSegmentGroupInfo &gg = SegmentGroupInfo[g];
        goodForReclaim += _mm_popcnt_u64(gg.GoodForReclaimMask);
    }
    yint reclaimBlocks = min(goodForReclaim - keepSize / SEGMENT_SIZE, maxReclaim / SEGMENT_SIZE);
    for (yint g = SEGMENT_GROUP_COUNT - 1; reclaimBlocks > 0 && g >= 0; --g) {
        TSegmentGroupInfo &gg = SegmentGroupInfo[g];
        if (gg.GoodForReclaimMask) {
            ui64 mask = gg.GoodForReclaimMask;
            unsigned long z;
            while (_BitScanForward64(&z, mask)) {
                ui64 bit = 1ull << z;
                mask ^= bit;
                yint segmentId = g * 64 + z;
                TSegmentInfo *segInfo = &SegmentInfo[segmentId];
                // lock presumably free segment
                ui64 expectedMask = ALL_BLOCKS;
                if (segInfo->GlobalFreeBlockMask.compare_exchange_strong(expectedMask, SEGMENT_ACTIVE)) {
                    if (gg.GoodForReclaimMask & bit) {
                        Y_ASSERT(gg.CommitedMask & bit);
                        gg.CommitedMask &= ~bit;
                        gg.GoodForReclaimMask &= ~bit;
                        --reclaimBlocks;
                        char *segment = GetSegmentData(segmentId);
                        OsReclaim(segment, SEGMENT_SIZE);
                    }
                    segInfo->GlobalFreeBlockMask = ALL_BLOCKS;
                }
            }
        }
    }
}


/////////////////////////////////////////////////////////////////////////////////
// callback on changes in free block bits (besides SEGMENT_ACTIVE)
static void FreeBlockMaskHasChanged(yint segmentId, ui64 oldMask, ui64 newMask)
{
    yint hierStart = (segmentId & ~(SEGMENT_HIER_SIZE - 1));
    yint hierStart2 = (segmentId & ~(SEGMENT_HIER_SIZE2 - 1));
    yint hierStart3 = (segmentId & ~(SEGMENT_HIER_SIZE3 - 1));
    yint oldFree = _mm_popcnt_u64(oldMask & ALL_2LAYER_BLOCKS);
    yint newFree = _mm_popcnt_u64(newMask & ALL_2LAYER_BLOCKS);
    ui64 posOldMask = oldMask & ALL_BLOCKS;
    ui64 posNewMask = newMask & ALL_BLOCKS;
    for (yint lenLn = 0; lenLn < 6; ++lenLn) {
        yint len = 1ull << lenLn;
        yint oldOk = (posOldMask != 0) & (oldFree >= MIN_FREE_BLOCKS + len);
        yint newOk = (posNewMask != 0) & (newFree >= MIN_FREE_BLOCKS + len);
        yint delta = newOk - oldOk;
        if (delta != 0) {
            TSegmentInfo *hseg = &SegmentInfo[hierStart + lenLn];
            yint oldHCount = hseg->HierFreeCount.fetch_add(delta);
            yint delta2 = 0;
            if (oldHCount == 0) {
                delta2 = 1;
            } else if (oldHCount + delta == 0) {
                delta2 = -1;
            }
            if (delta2 != 0) {
                TSegmentInfo *hseg2 = &SegmentInfo[hierStart2 + lenLn];
                TSegmentInfo *hseg3 = &SegmentInfo[hierStart3 + lenLn];
                yint oldHCount2 = hseg2->HierFreeCount2.fetch_add(delta2);
                if (oldHCount2 == 0) {
                    hseg3->HierFreeCount2.fetch_add(1);
                } else if (oldHCount2 + delta2 == 0) {
                    hseg3->HierFreeCount2.fetch_add(-1);
                }
            }
        }
        posOldMask = posOldMask & (posOldMask >> len);
        posNewMask = posNewMask & (posNewMask >> len);
    }
}


// modify global free block mask
static bool AddFreeBlockMask(yint segmentId, char *segment, ui64 blockMask)
{
    Y_ASSERT(segmentId == GetSegmentId(segment));
    TSegmentInfo *segInfo = &SegmentInfo[segmentId];
    ui64 oldMask = segInfo->GlobalFreeBlockMask.fetch_add(blockMask);
    ui64 newMask = oldMask + blockMask;
    FreeBlockMaskHasChanged(segmentId, oldMask, newMask);
    bool rv = false;
    if (newMask == ALL_BLOCKS) {
        // can free the segment now!
        ui64 expectedMask = ALL_BLOCKS;
        if (segInfo->GlobalFreeBlockMask.compare_exchange_strong(expectedMask, SEGMENT_ACTIVE)) {
            SegmentMarkForReclaimLocked(segmentId);
            segInfo->GlobalFreeBlockMask = ALL_BLOCKS;
        }
    } else {
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        rv = (segHdr->RetestBlockMask != 0);
    }
    return rv;
}


/////////////////////////////////////////////////////////////////////////////////
// thread segment attach / release

static bool ProcessRetestMaskLocked(char *segment);
static void DumpLocalAllocMasksLocked(char *segment);


static void ReleaseSegmentOwnership(char *segment)
{
    for (;;) {
        ProcessRetestMaskLocked(segment);

        yint segmentId = GetSegmentId(segment);
        TSegmentInfo *segInfo = &SegmentInfo[segmentId];
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        ui64 localFreeMask = segHdr->LocalFreeBlockMask;
        segHdr->LocalFreeBlockMask = 0;
        localFreeMask &= ~SEGMENT_ACTIVE;
        Y_ASSERT(segInfo->GlobalFreeBlockMask & SEGMENT_ACTIVE);
        bool hasRetest = AddFreeBlockMask(segmentId, segment, localFreeMask - SEGMENT_ACTIVE);
        if (!hasRetest) {
            break;
        }
        // bad luck, some operations happened while we were releasing lock
        for (;;) {
            ui64 freeMask = segInfo->GlobalFreeBlockMask;
            if (freeMask & SEGMENT_ACTIVE) {
                // not our problem, RetestBlockMask will be processed at least on segment release, quit
                return;
            } else {
                // acquire & release, RetestBlockMask is processed at the outer loop start
                if (segInfo->GlobalFreeBlockMask.compare_exchange_weak(freeMask, freeMask | SEGMENT_ACTIVE)) {
                    // retry ReleaseSegmentOwnership
                    break;
                }
            }
        }
    }
}


static void OnThreadDestruction(void*)
{
    Y_ASSERT(pThreadSegment != 0);
    DumpLocalAllocMasksLocked(pThreadSegment);
    ReleaseSegmentOwnership(pThreadSegment);
    pThreadSegment = 0;
    IsStoppingThread = true;
}


// try to switch to segment s
static bool TryToSwitchSegment(ui64 allowMask, yint len, yint s)
{
    TSegmentInfo *segInfo = &SegmentInfo[s];
    ui64 freeMask = segInfo->GlobalFreeBlockMask;
    if (freeMask & SEGMENT_ACTIVE) {
        return false;
    }
    yint freeBlockCount = _mm_popcnt_u64(freeMask & allowMask);
    if (freeBlockCount < MIN_FREE_BLOCKS + len) {
        return false;
    }
    unsigned long freePtr = 0;
    if (len > 1 && !BitMaskFind(freeMask & allowMask, len, &freePtr)) {
        return false;
    }
    ui64 expectedMask = freeMask;
    if (SegmentInfo[s].GlobalFreeBlockMask.compare_exchange_weak(expectedMask, SEGMENT_ACTIVE)) {
        FreeBlockMaskHasChanged(s, expectedMask, SEGMENT_ACTIVE);
        char *segment = GetSegmentData(s);
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        if (freeMask == ALL_BLOCKS) {
            // new segment is allocated
            MaxSegmentId = max(MaxSegmentId, s); // non atomic, true number of segments used might be higher then stored in MaxSegmentId
            if (SegmentCommitLocked(segment)) {
                segHdr->Init();
            }
        }
        Y_ASSERT(segHdr->LocalFreeBlockMask == 0); // local free mask should be empty for nonactive segment
        segHdr->LocalFreeBlockMask = freeMask | SEGMENT_ACTIVE;
        // process postponed segment operations on switching to the new segment
        ProcessRetestMaskLocked(segment);
        if (pThreadSegment == 0) {
            // call thread destruction callback for this thread
#ifdef _win_
            FlsSetValue(ThreadDestructionTracker, segment);
#else
            pthread_setspecific(ThreadDestructionTracker, (void*)-1);
#endif
        }
        pThreadSegment = segment;
        return true;
    }
    // failed to acquire, avoid this segment for awhile? or retry?
    return false; // avoid
}


// no len consequitive blocks in the current segment, switch to the suitable segment
static void SwitchSegment(ui64 allowMask, yint len)
{
    hu_check_init();
    if (pThreadSegment != 0) {
        DumpLocalAllocMasksLocked(pThreadSegment);
        ReleaseSegmentOwnership(pThreadSegment);
    }
    yint lenLn = CalcLog2(len);
    for (yint hierStart3 = 0; hierStart3 < SEGMENT_COUNT; hierStart3 += SEGMENT_HIER_SIZE3) {
        if (SegmentInfo[hierStart3 + lenLn].HierFreeCount3 == 0) {
            continue;
        }
        for (yint hierStart2 = hierStart3; hierStart2 < hierStart3 + SEGMENT_HIER_SIZE3; hierStart2 += SEGMENT_HIER_SIZE2) {
            if (SegmentInfo[hierStart2 + lenLn].HierFreeCount2 == 0) {
                continue;
            }
            for (yint hierStart = hierStart2; hierStart < hierStart2 + SEGMENT_HIER_SIZE2; hierStart += SEGMENT_HIER_SIZE) {
                if (SegmentInfo[hierStart + lenLn].HierFreeCount == 0) {
                    continue;
                }
                for (yint s = hierStart; s < hierStart + SEGMENT_HIER_SIZE; ++s) {
                    if (TryToSwitchSegment(allowMask, len, s)) {
                        return;
                    }
                }
            }
        }
    }
    abort(); // out of segments
}


// returns true if found non trivial operations performed by other threads
static bool CheckOtherThreadsOpsLocked(char *segment)
{
    bool rv = false;
    yint segmentId = GetSegmentId(segment);
    TSegmentInfo *segInfo = &SegmentInfo[segmentId];
    if (segInfo->GlobalFreeBlockMask != SEGMENT_ACTIVE) {
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        // other threads has freed some blocks
        ui64 newFreeBlocks = segInfo->GlobalFreeBlockMask.exchange(SEGMENT_ACTIVE);
        FreeBlockMaskHasChanged(segmentId, newFreeBlocks, SEGMENT_ACTIVE);
        segHdr->LocalFreeBlockMask |= newFreeBlocks;
        rv = true;
    }
    rv |= ProcessRetestMaskLocked(segment);
    return rv;
}


/////////////////////////////////////////////////////////////////////////////////
// medium size allocs
NOINLINE static void* SegmentMediumAlloc(yint sz)
{
    yint len = (sz + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for (;;) {
        if (pThreadSegment) {
            char *segment = pThreadSegment;
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            unsigned long freePtr;
            if (BitMaskFind(segHdr->LocalFreeBlockMask & ALL_BLOCKS, len, &freePtr)) {
                ui64 *blockFreeBits = ((ui64*)segment) + BITS_OFFSET_MULT;

                // mark type of the allocation
                Y_ASSERT(segHdr->CurAllocLevel[freePtr] == 0);
                segHdr->CurAllocLevel[freePtr] = AL_MEDIUM_ALLOC;
                // keep the size of allocation
                blockFreeBits[freePtr] = sz;
                // mark blocks as used
                ui64 blockMask = ((1ull << len) - 1) << freePtr;
                Y_ASSERT(SegmentInfo[GetSegmentId(segment)].GlobalFreeBlockMask & SEGMENT_ACTIVE);
                Y_ASSERT(blockMask != 0);
                Y_ASSERT((segHdr->LocalFreeBlockMask & blockMask) == blockMask);
                segHdr->LocalFreeBlockMask -= blockMask;
                Y_ASSERT((segHdr->LocalFreeBlockMask & blockMask) == 0);
                return segment + freePtr * BLOCK_SIZE;
            }
            if (CheckOtherThreadsOpsLocked(segment)) {
                continue;
            }
        } else {
            if (IsStoppingThread) {
                return GAAlloc(sz);
            }
        }
        SwitchSegment(ALL_BLOCKS, len);
    }
}


NOINLINE static void SegmentMediumFree(char *segment, yint block)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    ui64 *blockFreeBits = ((ui64*)segment) + BITS_OFFSET_MULT;

    yint sz = blockFreeBits[block];
    yint len = (sz + BLOCK_SIZE - 1) / BLOCK_SIZE;
    ui64 blockMask = ((1ull << len) - 1) << block;
    // have to update CurAllocLevel first, free block bits last to avoid races
    segHdr->CurAllocLevel[block] = AL_FREE;
    if (pThreadSegment == segment) {
        // shortcut is possible, free from the current block
        Y_ASSERT((segHdr->LocalFreeBlockMask & blockMask) == 0);
        segHdr->LocalFreeBlockMask |= blockMask;
    } else {
        AddFreeBlockMask(GetSegmentId(segment), segment, blockMask);
    }
}


static yint SegmentMediumGetSize(char *segment, yint block)
{
    ui64 *blockFreeBits = ((ui64*)segment) + BITS_OFFSET_MULT;
    return blockFreeBits[block];
}


/////////////////////////////////////////////////////////////////////////////////////////////////
// single block alloc/free
static bool BlockAlloc(yint pow2, ui64 allowMask, yint *res)
{
    char *segment = pThreadSegment;
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    for (;;) {
        unsigned long freePtr;
        if (_BitScanForward64(&freePtr, segHdr->LocalFreeBlockMask & allowMask)) {
            Y_ASSERT(segHdr->CurAllocLevel[freePtr] == AL_FREE);
            segHdr->CurAllocLevel[freePtr] = pow2;
            ui64 blockMask = 1ull << freePtr;
            Y_ASSERT(segHdr->LocalFreeBlockMask & blockMask);
            segHdr->LocalFreeBlockMask -= blockMask;
            *res = freePtr;
            return true;
        }
        if (CheckOtherThreadsOpsLocked(segment)) {
            continue;
        }
        *res = -1;
        return false;
    }
}


static void BlockFreeLocked(TSegmentHeader *segHdr, TSegmentSizeContext *sCtx, yint block)
{
    ui64 blockMask = (1ull << block);
    segHdr->CurAllocLevel[block] = AL_FREE;
    if (GetBlockId(sCtx->ChunkOffset) == block) {
        sCtx->ChunkOffset = -1;
        sCtx->LocalFreeAllocMask = 0;
    }
    sCtx->CanAllocBlockMask &= ~blockMask;
    segHdr->LocalFreeBlockMask |= blockMask;
}


static void BlockFree(char *segment, yint block, TSegmentSizeContext *sCtx)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    if (pThreadSegment == segment) {
        BlockFreeLocked(segHdr, sCtx, block);
    } else {
        // locking segment allows us to process RetestBlockMask ourselves
        yint segmentId = GetSegmentId(segment);
        TSegmentInfo *segInfo = &SegmentInfo[segmentId];
        segHdr->RetestBlockMask |= 1ull << block;
        for (;;) {
            ui64 freeMask = segInfo->GlobalFreeBlockMask;
            if (freeMask & SEGMENT_ACTIVE) {
                // RetestBlockMask will be processed by other thread
                break;
            } else {
                if (segInfo->GlobalFreeBlockMask.compare_exchange_weak(freeMask, freeMask | SEGMENT_ACTIVE)) {
                    ReleaseSegmentOwnership(segment);
                    break;
                }
            }
        }
    }
}


///////////////////////////////////////////////////////////////////////////////////////////////////
// Small size allocations
static void ProcessRetestSmallSizeLocked(char *segment, ui8 pow2, yint block)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
    TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
    TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
    if (blockFreeBits[block] == gCtx.AllFree) {
        BlockFreeLocked(segHdr, &sCtx, block);
    } else {
        sCtx.CanAllocBlockMask |= (1ull << block);
    }
}


static void DumpSmallLocalAllocMasksLocked(char *segment)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
    for (ui8 pow2 = NEED_2LAYER; pow2 < BLOCK_SIZE_LN; ++pow2) {
        TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
        ui64 allocMask = sCtx.LocalFreeAllocMask;
        if (allocMask != 0 && sCtx.ChunkOffset >= 0) {
            yint block = GetBlockId(sCtx.ChunkOffset);
            Y_ASSERT(segHdr->CurAllocLevel[block] == pow2);
            sCtx.LocalFreeAllocMask = 0;
            TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
            std::atomic<ui64> &mask = blockFreeBits[block];
            Y_ASSERT((mask & allocMask) == 0);
            ui64 newMask = mask.fetch_add(allocMask) + allocMask;
            sCtx.CanAllocBlockMask |= (1ull << block);
            if (newMask == gCtx.AllFree) {
                BlockFreeLocked(segHdr, &sCtx, block);
            }
        }
        sCtx.ChunkOffset = -1;
    }
}


NOINLINE static void* SegmentSmallAlloc(yint pow2)
{
    for (;; SwitchSegment(ALL_BLOCKS, 1)) {
        char *segment = pThreadSegment;
        if (segment) {
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
            unsigned long idx;
            if (!_BitScanForward64(&idx, sCtx.LocalFreeAllocMask)) {
                yint block = GetBlockId(sCtx.ChunkOffset);
                Y_ASSERT(sCtx.LocalFreeAllocMask == 0);
                if (block >= 0) {
                    // reload allocMask from other threads
                    Y_ASSERT(segHdr->CurAllocLevel[block] == pow2);
                    sCtx.LocalFreeAllocMask = blockFreeBits[block].exchange(0);
                }
                if (sCtx.LocalFreeAllocMask == 0) {
                    // select different block with the same size allocs
                    block = -1;
                    unsigned long freePtr;
                    while (_BitScanForward64(&freePtr, sCtx.CanAllocBlockMask)) {
                        if (segHdr->CurAllocLevel[freePtr] == pow2) {
                            ui64 freeAllocMask = blockFreeBits[freePtr].exchange(0);
                            if (freeAllocMask != 0) {
                                sCtx.LocalFreeAllocMask = freeAllocMask;
                                block = freePtr;
                                break;
                            }
                        } else {
                            abort(); // is this possible? never happened so far
                        }
                        sCtx.CanAllocBlockMask &= ~(1ull << freePtr);
                    }
                }
                if (sCtx.LocalFreeAllocMask != 0) {
                    Y_ASSERT(block >= 0);
                    bool ok = _BitScanForward64(&idx, sCtx.LocalFreeAllocMask);
                    Y_ASSERT(ok);
                } else {
                    // failed to find, use new block
                    Y_ASSERT(block < 0);
                    if (BlockAlloc(pow2, ALL_BLOCKS, &block)) {
                        sCtx.CanAllocBlockMask |= (1ull << block);
                        idx = 0;
                        sCtx.LocalFreeAllocMask = gCtx.AllFree;
                        blockFreeBits[block] = 0;
                    } else {
                        sCtx.ChunkOffset = -1;
                        continue;
                    }
                }
                sCtx.ChunkOffset = block << BLOCK_SIZE_LN;
            }
            Y_ASSERT(sCtx.ChunkOffset >= 0);
            Y_ASSERT(sCtx.LocalFreeAllocMask & (1ull << idx));
            sCtx.LocalFreeAllocMask ^= 1ull << idx; // expect btc opcode
            yint offset = ((yint)idx << pow2);
            Y_ASSERT(offset < BLOCK_SIZE);
            return segment + sCtx.ChunkOffset + offset;
        } else {
            if (IsStoppingThread) {
                return GAAlloc(1ll << pow2);
            }
        }
    }
}


NOINLINE static void SegmentSmallFree(char *segment, yint block, yint memPtr, ui8 pow2)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    yint idx = (memPtr & (BLOCK_SIZE - 1)) >> pow2;
    TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
    std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
    std::atomic<ui64> &mask = blockFreeBits[block];
    ui64 allocMask = 1ull << idx;
    Y_ASSERT((mask & allocMask) == 0);
    ui64 newMask = mask.fetch_add(allocMask) + allocMask;
    if (_mm_popcnt_u64(newMask) == gCtx.MinFreeCount) { // if has space for MinFreeCount allocs then set flag for reuse
        // can modify CanAllocBlockMask for the current segment only
        if (segment == pThreadSegment) {
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            sCtx.CanAllocBlockMask |= (1ull << block);
        } else {
            segHdr->RetestBlockMask |= (1ull << block); // no need to acquire lock at this point, the flag will be used later
        }
    }
    if (newMask == gCtx.AllFree) {
        TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
        BlockFree(segment, block, &sCtx);
    }
}


///////////////////////////////////////////////////////////////////////////////////////////////////
// Tiny size allocations
static void ProcessRetestTinySizeLocked(char *segment, ui8 pow2, yint block)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
    TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
    TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
    if (blockFreeBits[block] == (ui64)gCtx.ChunkCount) {
        BlockFreeLocked(segHdr, &sCtx, block);
    } else {
        sCtx.CanAllocBlockMask |= (1ull << block);
    }
}


static void DumpTinyLocalAllocMasksLocked(char *segment)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;
    for (ui8 pow2 = MIN_POW2; pow2 < NEED_2LAYER; ++pow2) {
        TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
        ui64 allocMask = sCtx.LocalFreeAllocMask;
        if (allocMask != 0 && sCtx.ChunkOffset >= 0) {
            yint block = GetBlockId(sCtx.ChunkOffset);
            yint chunk = GetChunkId(sCtx.ChunkOffset, pow2);
            Y_ASSERT(chunk >= 0);
            Y_ASSERT(segHdr->CurAllocLevel[block] == pow2);
            sCtx.LocalFreeAllocMask = 0;
            std::atomic<ui64> *memFreeBits = (std::atomic<ui64>*)segment;
            std::atomic<ui64> &mask = memFreeBits[block * BITS_OFFSET_MULT + chunk];
            Y_ASSERT((mask & allocMask) == 0);
            ui64 newMask = mask.fetch_add(allocMask) + allocMask;
            sCtx.CanAllocBlockMask |= (1ull << block);
            if (newMask == ALL_BITS) {
                TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
                ui64 newFreeChunkCount = blockFreeBits[block].fetch_add(1) + 1;
                if (newFreeChunkCount == (ui64)gCtx.ChunkCount) {
                    TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
                    BlockFreeLocked(segHdr, &sCtx, block);
                }
            }
        }
        sCtx.ChunkOffset = -1;
    }
}


static bool SearchChunkLocked(
    TSegmentSizeContext *sCtx, TGlobalSizeContext &gCtx,
    std::atomic<ui64> *blockFreeBits, std::atomic<ui64> *memFreeBits,
    yint block, yint startChunk, yint *resChunk)
{
    std::atomic<ui64> *bitsPtr = &memFreeBits[block * BITS_OFFSET_MULT];
    yint chunkMask = gCtx.ChunkCount - 1;
    for (yint k = startChunk, kFin = startChunk + gCtx.ChunkCount; k < kFin; ++k) {
        yint chunk = k & chunkMask;
        if (bitsPtr[chunk]) { // load and test for zero is much cheaper then xchg
            // only additional free can happen between these steps
            ui64 mask = bitsPtr[chunk].exchange(0);
            Y_ASSERT(mask != 0);  // something is seriously broken if this happens, alloc from different thread?
            sCtx->LocalFreeAllocMask = mask;
            if (mask == ALL_BITS) {
                blockFreeBits[block].fetch_add(-1);
            }
            *resChunk = chunk;
            return true;
        }
    }
    *resChunk = -1;
    return false;
}


NOINLINE static void* SegmentTinyAlloc(yint pow2)
{
    for (;; SwitchSegment(ALL_2LAYER_BLOCKS, 1)) {
        char *segment = pThreadSegment;
        if (segment) {
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            unsigned long idx;
            if (!_BitScanForward64(&idx, sCtx.LocalFreeAllocMask)) {
                TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
                std::atomic<ui64> *memFreeBits = (std::atomic<ui64>*)segment;
                std::atomic<ui64> *blockFreeBits = ((std::atomic<ui64>*)segment) + BITS_OFFSET_MULT;

                yint block = GetBlockId(sCtx.ChunkOffset);
                yint chunk = GetChunkId(sCtx.ChunkOffset, pow2);
                // local alloc mask is empty
                Y_ASSERT(sCtx.LocalFreeAllocMask == 0);
                if (block >= 0) {
                    // select different chunk from the current block, (or the same chunk if it was changed by other threads)
                    Y_ASSERT(chunk >= 0);
                    Y_ASSERT(segHdr->CurAllocLevel[block] == pow2);
                    if (!SearchChunkLocked(&sCtx, gCtx, blockFreeBits, memFreeBits, block, chunk, &chunk)) {
                        Y_ASSERT(sCtx.LocalFreeAllocMask == 0);
                        sCtx.CanAllocBlockMask &= ~(1ull << block);
                    }
                }
                if (sCtx.LocalFreeAllocMask == 0) {
                    // select different block with the same size allocs
                    block = -1;
                    unsigned long freePtr;
                    while (_BitScanForward64(&freePtr, sCtx.CanAllocBlockMask)) {
                        if (segHdr->CurAllocLevel[freePtr] == pow2) {
                            if (SearchChunkLocked(&sCtx, gCtx, blockFreeBits, memFreeBits, freePtr, 0, &chunk)) {
                                block = freePtr;
                                break;
                            }
                        } else {
                            abort(); // is this possible? never happened so far
                        }
                        sCtx.CanAllocBlockMask &= ~(1ull << freePtr);
                    }
                }
                if (sCtx.LocalFreeAllocMask != 0) {
                    Y_ASSERT(block >= 0);
                    Y_ASSERT(chunk >= 0);
                    bool ok = _BitScanForward64(&idx, sCtx.LocalFreeAllocMask);
                    Y_ASSERT(ok);
                } else {
                    // failed to find suitable block, use new block
                    Y_ASSERT(block < 0);
                    if (BlockAlloc(pow2, ALL_2LAYER_BLOCKS, &block)) {
                        chunk = 0;
                        sCtx.CanAllocBlockMask |= (1ull << block);
                        blockFreeBits[block] = gCtx.ChunkCount - 1;
                        sCtx.LocalFreeAllocMask = ALL_BITS;
                        for (yint k = 0; k < gCtx.ChunkCount; ++k) {
                            memFreeBits[block * BITS_OFFSET_MULT + k] = ALL_BITS;
                        }
                        memFreeBits[block * BITS_OFFSET_MULT + chunk] = 0;
                        idx = 0;
                    } else {
                        sCtx.ChunkOffset = -1;
                        continue;

                    }
                }
                sCtx.ChunkOffset = block * BLOCK_SIZE + (chunk << (6 + pow2));
                sCtx.ChunkOffsetMask = ~((1ull << (6 + pow2)) - 1);
            }
            Y_ASSERT(sCtx.ChunkOffset >= 0);
            Y_ASSERT(sCtx.LocalFreeAllocMask & (1ull << idx));
            sCtx.LocalFreeAllocMask ^= 1ull << idx; // expect btc opcode
            yint offset = ((yint)idx << pow2);
            Y_ASSERT(sCtx.ChunkOffset + offset < SEGMENT_SIZE);
            return segment + sCtx.ChunkOffset + offset;
        } else {
            if (IsStoppingThread) {
                return GAAlloc(1ll << pow2);
            }
        }
    }
}


NOINLINE static void SegmentTinyFree(char *segment, yint block, yint memPtr, ui8 pow2)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    yint blockOffset = (memPtr & (BLOCK_SIZE - 1)) >> pow2;
    yint chunk = blockOffset >> 6;
    yint idx = (blockOffset & 63);
    ui64 allocMask = 1ull << idx;
    std::atomic<ui64> *memFreeBits = (std::atomic<ui64>*)segment;
    std::atomic<ui64> &mask = memFreeBits[block * BITS_OFFSET_MULT + chunk];
    Y_ASSERT((mask & allocMask) == 0);
    ui64 newMask = mask.fetch_add(allocMask) + allocMask;
    if (_mm_popcnt_u64(newMask) == 16) { // if there is non trivial number of free allocations reuse the block
        // can modify CanAllocBlockMask for the current segment only
        if (segment == pThreadSegment) {
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            sCtx.CanAllocBlockMask |= (1ull << block);
        } else {
            segHdr->RetestBlockMask |= (1ull << block); // no need to acquire lock at this point, the flag will be used later
        }
    }
    if (newMask == ALL_BITS) {
        TGlobalSizeContext &gCtx = GlobalSizeCtx[pow2];
        std::atomic<ui64> *blockFreeBits = memFreeBits + BITS_OFFSET_MULT;
        ui64 newFreeChunkCount = blockFreeBits[block].fetch_add(1) + 1;
        if (newFreeChunkCount == (ui64)gCtx.ChunkCount) {
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            BlockFree(segment, block, &sCtx);
        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////////
// common functions for all segment* allocations
static bool ProcessRetestMaskLocked(char *segment)
{
    TSegmentHeader *segHdr = (TSegmentHeader*)segment;
    ui64 retestMask = segHdr->RetestBlockMask.exchange(0);
    if (retestMask == 0) {
        return false;
    }
    unsigned long block;
    while (_BitScanForward64(&block, retestMask)) {
        retestMask -= 1ull << block;
        ui8 pow2 = segHdr->CurAllocLevel[block];
        if (pow2 < MIN_POW2) {
            ; // this can happen, retest mask does not guarantee there is something to be done
        } else if (pow2 < NEED_2LAYER) {
            ProcessRetestTinySizeLocked(segment, pow2, block);
        } else if (pow2 < BLOCK_SIZE_LN) {
            ProcessRetestSmallSizeLocked(segment, pow2, block);
        } else {
            ; // this can happen, retest mask does not guarantee there is something to be done
        }
    }
    return true;
}


static void DumpLocalAllocMasksLocked(char *segment)
{
    DumpTinyLocalAllocMasksLocked(segment);
    DumpSmallLocalAllocMasksLocked(segment);
}



////////////////////////////////////////////////////////////////////////////////////////////////////////
static yint ReclaimKeepSize = 1024 * (1ull << 20);
static yint ReclaimMaxReclaim = 512 * (1ull << 20);
static THREAD_FUNC_RETURN ReclaimThread(void*)
{
    // keep & max can be separate for large & segment spaces
    for (;;) {
        Sleep(1000);
        LargeReclaim(ReclaimKeepSize, ReclaimMaxReclaim);
        SegmentReclaim(ReclaimKeepSize, ReclaimMaxReclaim);
    }
}

void RunHuReclaim()
{
    hu_check_init();
#ifdef _win_
    CreateThread(0, 0, ReclaimThread, 0, 0, 0);
#else
    pthread_t tid;
    pthread_create(&tid, 0, ReclaimThread, 0);
#endif
}

static void hu_init()
{
    for (;;) {
        ui64 val = AllocatorIsInitialized;
        if (val == 1) {
            break;
        }
        if (val == 0 && AllocatorIsInitialized.compare_exchange_weak(val, 2)) {
            OsInit();
            BitMaskInit();
            GAInit();
            LargeInit();
            SegmentInit();
            AllocatorIsInitialized = 1;
            break;
        } else {
            SchedYield();
        }
    }
}


static void* hu_alloc(yint sz)
{
    unsigned long pow2 = MIN_POW2;
    if (sz > (1ll << MIN_POW2)) {
        unsigned long xx;
        _BitScanReverse64(&xx, sz - 1);
        pow2 = xx + 1;
    }

    if (pow2 < BLOCK_SIZE_LN) {
        Y_ASSERT(pow2 >= MIN_POW2);
        char *segment = pThreadSegment;
        if (segment) {
            // local shortcut is same for tiny & small allocs
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
            unsigned long idx;
            if (_BitScanForward64(&idx, sCtx.LocalFreeAllocMask)) {
                Y_ASSERT(sCtx.ChunkOffset >= 0);
                Y_ASSERT(sCtx.LocalFreeAllocMask & (1ull << idx));
                sCtx.LocalFreeAllocMask ^= 1ull << idx; // expect btc opcode
                yint offset = ((yint)idx << pow2);
                Y_ASSERT(sCtx.ChunkOffset + offset < SEGMENT_SIZE);
                return segment + sCtx.ChunkOffset + offset;
            }
        }
        if (pow2 < NEED_2LAYER) {
            // tiny alloc
            return SegmentTinyAlloc(pow2);
        } else {
            // small alloc
            return SegmentSmallAlloc(pow2);
        }
    } else if (pow2 <= LARGE_SIZE_LN) {
        Y_ASSERT(sz > 0);
        return SegmentMediumAlloc(sz);
    } else if (pow2 <= HUGE_SIZE_LN) {
        return LargeAlloc(sz);
    } else {
        return GAAlloc(sz);
    }
}


NOINLINE static void hu_free_large(void *p)
{
    if (p == 0) {
        return;
    }
    ui64 largeOffset = ((const char*)p) - LargeMemoryPtr;
    if (largeOffset < LARGE_BLOCK_COUNT * LARGE_BLOCK_SIZE) {
        LargeFree(p);
        return;
    }
    GAFree(p);
}

static void hu_free(void *p)
{
    Y_ASSERT(SegmentMemoryPtr != 0);
    ui64 offset = ((char*)p) - SegmentMemoryPtr;
    if (offset < SEGMENT_COUNT * SEGMENT_SIZE) {
        char *segment = SegmentMemoryPtr + (offset & ~(SEGMENT_SIZE - 1));
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;

        yint memPtr = ((char*)p) - segment;
        yint block = memPtr >> BLOCK_SIZE_LN;
        ui8 pow2 = segHdr->CurAllocLevel[block];
        Y_ASSERT(pow2 >= MIN_POW2);

        if (pow2 < (ui8)NEED_2LAYER) {
            // tiny free
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            if (segment == pThreadSegment) {
                TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
                if ((memPtr & sCtx.ChunkOffsetMask) == sCtx.ChunkOffset) {
                    Y_ASSERT(GetBlockId(sCtx.ChunkOffset) == block);
                    ui8 idx = (memPtr & ~sCtx.ChunkOffsetMask) >> pow2;
                    Y_ASSERT(idx < 64);
                    sCtx.LocalFreeAllocMask |= 1ull << idx; // expect bts opcode
                    return;
                }
            }
            SegmentTinyFree(segment, block, memPtr, pow2);

        } else if (pow2 < (ui8)BLOCK_SIZE_LN) {
            // small alloc
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            if (segment == pThreadSegment) {
                // avoid atomic when possible
                TSegmentSizeContext &sCtx = segHdr->SizeCtx[pow2];
                yint blockOffset = memPtr & ~(BLOCK_SIZE - 1);
                if (blockOffset == sCtx.ChunkOffset) {
                    yint idx = (memPtr & (BLOCK_SIZE - 1)) >> pow2;
                    sCtx.LocalFreeAllocMask |= 1ull << idx;  // expect bts opcode
                    return;
                }
            }
            SegmentSmallFree(segment, block, memPtr, pow2);

        } else if (pow2 == AL_MEDIUM_ALLOC) {
            SegmentMediumFree(segment, block);
        } else {
            abort(); // memory corruption
        }
    } else {
        hu_free_large(p);
    }
}


static yint hu_getsize(const void *p)
{
    Y_ASSERT(SegmentMemoryPtr != 0);
    Y_ASSERT(LargeMemoryPtr != 0);
    if (p == 0) {
        return 0;
    }
    ui64 offset = ((const char*)p) - SegmentMemoryPtr;
    if (offset < SEGMENT_COUNT * SEGMENT_SIZE) {
        yint segmentId = offset >> SEGMENT_SIZE_LN;
        char *segment = SegmentMemoryPtr + (segmentId << SEGMENT_SIZE_LN);
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;

        yint memPtr = ((char*)p) - segment;
        yint block = memPtr >> BLOCK_SIZE_LN;
        ui8 pow2 = segHdr->CurAllocLevel[block];
        Y_ASSERT(pow2 != 0);

        if (pow2 < BLOCK_SIZE_LN) {
            return 1ll << pow2;
        } else {
            return SegmentMediumGetSize(segment, block);
        }
    } else {
        ui64 largeOffset = ((const char*)p) - LargeMemoryPtr;
        if (largeOffset < LARGE_BLOCK_COUNT * LARGE_BLOCK_SIZE) {
            return LargeGetSize(p);
        }
        return GAGetSize(p);
    }
}
