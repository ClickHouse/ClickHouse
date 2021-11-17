#pragma once

/// Author: Andrey Gulin, Yandex.
/// This code is published to be tested for performance with ClickHouse.

//
// -profile mimalloc winning workload
// -speed up SwitchSegment with some hierarchical structure
// -remove Padding from GlobalFreeBlockMask structure?
//  -avoid atomics overlap somehow?
// -huge pages under win?
//

#include <atomic>

#include <cassert>

#define Y_ASSERT assert
#define Y_VERIFY assert

#include <cstdint>

#define ui8 uint8_t
#define ui64 uint64_t

#include <sched.h>

#define SchedYield sched_yield

#include <algorithm>

#define Max std::max

#ifdef _MSC_VER
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <intrin.h>
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

#define PERTHREAD __declspec(thread)
#define NOINLINE __declspec(noinline)
#define _win_

#else
#include <immintrin.h>
#include <pthread.h>
#include <sys/mman.h>

#define PERTHREAD __thread
#define NOINLINE __attribute__((noinline))

static __inline__ unsigned char __attribute__((__always_inline__, __nodebug__))
_BitScanForward64(unsigned long *_Index, unsigned long long _Mask) {
  if (!_Mask)
    return 0;
  *_Index = __builtin_ctzll(_Mask);
  return 1;
}
static __inline__ unsigned char __attribute__((__always_inline__, __nodebug__))
_BitScanReverse64(unsigned long *_Index, unsigned long long _Mask) {
  if (!_Mask)
    return 0;
  *_Index = 63 - __builtin_clzll(_Mask);
  return 1;
}
#endif

typedef long long yint;
#ifndef ARRAY_SIZE
    #define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))
#endif

#undef PAGE_SIZE
/////////////////////////////////////////////////////////////////////////////////////////////////////////////
const yint GLOBAL_HASH_SIZE_LN = 20; // 4x+ number of blocks and more to avoid excessive hash lookups
const yint GLOBAL_HASH_SIZE = 1ll << GLOBAL_HASH_SIZE_LN;
const yint PAGE_SIZE_LN = 12;
const yint PAGE_SIZE = 1ll << PAGE_SIZE_LN;
const yint ADDRESS_BITS = 48; // only 48 bits are used in x64
const yint HUGEPAGE_SIZE = 1ll << 21;

static std::atomic<ui64> *GAHash;


static void GAInit()
{
    yint sz = GLOBAL_HASH_SIZE * sizeof(GAHash[0]);
#ifdef _win_
    GAHash = (std::atomic<ui64>*)VirtualAlloc(0, sz, MEM_COMMIT, PAGE_READWRITE);
#else
    GAHash = (std::atomic<ui64>*)mmap(0, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
#endif
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
    yint sz;
    if (szArg >= 1ll << 20) {
        // huge alloc
        const yint hugePageSize = 1ll << 21;
        sz = (szArg + hugePageSize - 1) & ~(hugePageSize - 1);
    } else {
        // regular alloc
        sz = (szArg + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
    }
#ifdef _win_
    void *ptr = VirtualAlloc(0, sz, MEM_COMMIT, PAGE_READWRITE);
#else
    void *ptr = mmap(0, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (sz >= HUGEPAGE_SIZE) {
        madvise(ptr, sz, MADV_HUGEPAGE);
    }
#endif
    if (ptr == 0) {
        abort(); // out of memory
    }
    yint k = GAHashPtr(ptr);
    for (yint kStart = k;;) {
        std::atomic<ui64> &ga = GAHash[k];
        ui64 gaVal = ga;
        if (gaVal <= 1) {
            ui64 expVal = gaVal;
            ui64 newVal = (((char*)ptr - (char*)nullptr) >> PAGE_SIZE_LN) + ((sz >> PAGE_SIZE_LN) << (ADDRESS_BITS - PAGE_SIZE_LN));
            if (ga.compare_exchange_weak(expVal, newVal)) {
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


NOINLINE static void GAFree(void *ptr)
{
    if (ptr == 0) {
        return;
    }
    yint k = GAHashPtr(ptr);
    yint sz = 0;
    for (;;) {
        std::atomic<ui64> &ga = GAHash[k];
        ui64 gaVal = ga;
        const yint ptrMask = (1ll << (ADDRESS_BITS - PAGE_SIZE_LN)) - 1;
        char *gaPtr = (char*)((gaVal & ptrMask) << PAGE_SIZE_LN);
        if (gaPtr == ptr) {
            sz = (gaVal >> (ADDRESS_BITS - PAGE_SIZE_LN)) << PAGE_SIZE_LN;
            ga = 1;
            break;
        }
        if (gaVal == 0) {
            abort(); // not found
        }
        k = (k + 1) & (GLOBAL_HASH_SIZE - 1);
    }
#ifdef _win_
    VirtualFree(ptr, 0, MEM_RELEASE); // no need to remember size for VirtualAlloc case!
#else
    munmap(ptr, sz);
#endif
}


static yint GAGetSize(const void *ptr)
{
    if (ptr == 0) {
        return 0;
    }
    yint k = GAHashPtr(ptr);
    for (;;) {
        std::atomic<ui64> &ga = GAHash[k];
        ui64 gaVal = ga;
        const yint ptrMask = (1ll << (ADDRESS_BITS - PAGE_SIZE_LN)) - 1;
        char *gaPtr = (char*)((gaVal & ptrMask) << PAGE_SIZE_LN);
        if (gaPtr == ptr) {
            return (gaVal >> (ADDRESS_BITS - PAGE_SIZE_LN)) << PAGE_SIZE_LN;
        }
        if (gaVal == 0) {
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
    if (ss[3]) { // shortcutfor small sizes, faster on average
        ms = ms & (ms >> ss[3]);
        ms = ms & (ms >> ss[4]);
        ms = ms & (ms >> ss[5]);
    }
    Y_ASSERT(ss[6] == 0);
    return _BitScanForward64(index, ms);
}


//////////////////////////////////////////////////////////////////////////////////////////////
//
// Segment structure
// segment (2M) / block (32k) / chunk (in 2 layer) / element
// segment 1st block has special usage : header(512 bytes), BlockFreeBits (512 bytes), MemFreeBits (62 * 512 bytes)
//  BlockFreeBits usage:
//   size for medium allocations
//   free bit mask for 1layer structure
//   number of completely free chunks for 2layer structure
//  MemFreeBits are lower level free bitmasks for2 layer allocation structure
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
const yint HUGE_SIZE_LN = SEGMENT_SIZE_LN - 1;
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

// trade locality for volume
//const yint MIN_FREE_BLOCKS = 16;
const yint MIN_FREE_BLOCKS = 8;
//const yint MIN_FREE_BLOCKS = 1;



struct TSegmentInfo
{
    std::atomic<ui64> GlobalFreeBlockMask;
    ui64 Padding[7];
};

// speed up SwitchSegment when low segments are occupied by long lived allocations
struct TFirstFreeId
{
    enum {
        ALIGNMENT = 64 // avoid frequent first free segment id updates
    };
    yint SegmentId = 0;
    ui64 Padding[7];

    void Set(yint s) { SegmentId = s & ~(ALIGNMENT - 1ll); }
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
static char *AllMemoryPtr;
static TSegmentInfo *SegmentInfo;
static TFirstFreeId FirstFree;
static TGlobalSizeContext GlobalSizeCtx[BLOCK_SIZE_LN + 1];
static PERTHREAD char* pThreadSegment;
static PERTHREAD bool IsStoppingThread;
static std::atomic<ui64> AllocatorIsInitialized;
static yint MaxSegmentId = 0;
#ifdef _win_
static DWORD ThreadDestructionTracker;
#else
static pthread_key_t ThreadDestructionTracker;
#endif


static void OnThreadDestruction(void *lpFlsData);

static void hu_init()
{
    Y_ASSERT(AllocatorIsInitialized == 2); // call hu_init() in locked state
    Y_ASSERT(sizeof(std::atomic<ui64>) == sizeof(ui64));
    Y_ASSERT(AllMemoryPtr == 0); // call init once
    GAInit();
    BitMaskInit();

    yint sz = SEGMENT_COUNT * SEGMENT_SIZE;
#ifdef _win_
    SegmentInfo = (TSegmentInfo*)VirtualAlloc(0, sizeof(TSegmentInfo) * SEGMENT_COUNT, MEM_COMMIT, PAGE_READWRITE);
    // large pages under windows require reserve + commit simultaneously, invent way to live with that
    //AllMemoryPtr = (char*)VirtualAlloc(0, sz, MEM_RESERVE | MEM_COMMIT | MEM_LARGE_PAGES, PAGE_READWRITE);
    AllMemoryPtr = (char*)VirtualAlloc(0, sz, MEM_RESERVE, PAGE_NOACCESS);
#else
    SegmentInfo = (TSegmentInfo*)mmap(0, sizeof(TSegmentInfo) * SEGMENT_COUNT, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
    for (char *ptr = (char*)0x1300000000ull;; ptr += 0x100000000ull) {
        AllMemoryPtr = (char*)mmap(ptr, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0);
        if (AllMemoryPtr == ptr) {
            break;
        }
    }
    madvise(AllMemoryPtr, sz, MADV_HUGEPAGE);
#endif

    Y_ASSERT(SegmentInfo);
    Y_ASSERT(AllMemoryPtr);
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
    }
#ifdef _win_
    ThreadDestructionTracker = FlsAlloc(OnThreadDestruction);
#else
    pthread_key_create(&ThreadDestructionTracker, OnThreadDestruction);
#endif
    AllocatorIsInitialized = 1;
}


inline yint GetSegmentId(char *segment)
{
    return (segment - AllMemoryPtr) >> SEGMENT_SIZE_LN;
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


// modify global free block mask
static bool AddFreeBlockMask(yint segmentId, char *segment, ui64 blockMask)
{
    Y_ASSERT(segmentId == GetSegmentId(segment));
    TSegmentInfo *segInfo = &SegmentInfo[segmentId];
    ui64 newMask = segInfo->GlobalFreeBlockMask.fetch_add(blockMask) + blockMask;
    if (_mm_popcnt_u64(newMask) > MIN_FREE_BLOCKS && segmentId < FirstFree.SegmentId) {
        FirstFree.Set(segmentId); // non atomic, not sync, but who cares, works most of the time
    }
    bool rv = false;
    if (newMask == ALL_BLOCKS) {
        // can free the segment now!
        ui64 expectedMask = ALL_BLOCKS;
        if (segInfo->GlobalFreeBlockMask.compare_exchange_strong(expectedMask, SEGMENT_ACTIVE)) {
            // got ownership on completely free segment, release memory and set bits to fresh segment state
//#ifdef _win_
//            VirtualFree(segment, SEGMENT_SIZE, MEM_DECOMMIT);
//#elif defined(_freebsd_)
//            madvise(segment, SEGMENT_SIZE, MADV_FREE);
//#else
//            madvise(segment, SEGMENT_SIZE, MADV_DONTNEED); // super slow for no apparent reason
//#endif
            segInfo->GlobalFreeBlockMask = ALL_BLOCKS;
        }
    } else {
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        rv = (segHdr->RetestBlockMask != 0);
    }
    return rv;
}


// has later defined logic
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


// no len consequitive blocks in the current segment, switch to the suitable segment
static void SwitchSegment(ui64 allowMask, yint len)
{
    for (;;) {
        ui64 val = AllocatorIsInitialized;
        if (val == 1) {
            break;
        }
        if (val == 0 && AllocatorIsInitialized.compare_exchange_weak(val, 2)) {
            hu_init();
            break;
        } else {
            SchedYield();
        }
    }
    if (pThreadSegment != 0) {
        DumpLocalAllocMasksLocked(pThreadSegment);
        ReleaseSegmentOwnership(pThreadSegment);
    }
    // need dramatic speed up for large memory footprint cases
    for (yint s = FirstFree.SegmentId; s < SEGMENT_COUNT; ++s) {
        TSegmentInfo *segInfo = &SegmentInfo[s];
        ui64 freeMask = segInfo->GlobalFreeBlockMask;
        if (freeMask & SEGMENT_ACTIVE) {
            continue;
        }
        yint freeBlockCount = _mm_popcnt_u64(freeMask & allowMask);
        if (freeBlockCount < MIN_FREE_BLOCKS + len) {
            continue;
        }
        unsigned long freePtr;
        if (len > 0 && !BitMaskFind(freeMask & allowMask, len, &freePtr)) {
            continue;
        }
        // weak or strong is unclear
        ui64 expectedMask = freeMask;
        if (SegmentInfo[s].GlobalFreeBlockMask.compare_exchange_weak(expectedMask, SEGMENT_ACTIVE)) {
            // update first free segment id if found segment is far from FirstFree
            if (s >= FirstFree.SegmentId + TFirstFreeId::ALIGNMENT * 3 && len == 1) {
                FirstFree.Set(s);
            }
            char *segment = AllMemoryPtr + s * SEGMENT_SIZE;
            TSegmentHeader *segHdr = (TSegmentHeader*)segment;
            if (freeMask == ALL_BLOCKS) {
                // new segment is allocated
                MaxSegmentId = Max(MaxSegmentId, s); // non atomic, true number of segments used might be higher then stored in MaxSegmentId
#ifdef _win_
                VirtualAlloc(segment, SEGMENT_SIZE, MEM_COMMIT, PAGE_READWRITE);
#endif
                segHdr->Init();
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
            return;
        }
        // failed to acquire, avoid this segment for awhile? or retry?
    }
    abort(); // out of segments
}


// returns true if found non trivial operations performed by other threads
static bool CheckOtherThreadsOpsLocked(char *segment)
{
    bool rv = false;
    TSegmentInfo *segInfo = &SegmentInfo[GetSegmentId(segment)];
    if (segInfo->GlobalFreeBlockMask != SEGMENT_ACTIVE) {
        TSegmentHeader *segHdr = (TSegmentHeader*)segment;
        // other threads has freed some blocks
        ui64 newFreeBlocks = segInfo->GlobalFreeBlockMask.exchange(SEGMENT_ACTIVE);
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
            yint offset = (idx << pow2);
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
            yint offset = (idx << pow2);
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


static void *hu_alloc(yint sz)
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
                yint offset = (idx << pow2);
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
    } else if (pow2 <= HUGE_SIZE_LN) {
        Y_ASSERT(sz > 0);
        return SegmentMediumAlloc(sz);
    } else {
        return GAAlloc(sz);
    }
}


static void hu_free(void *p)
{
    Y_ASSERT(AllMemoryPtr != 0);
    ui64 offset = ((char*)p) - AllMemoryPtr;
    if (offset < SEGMENT_COUNT * SEGMENT_SIZE) {
        char *segment = AllMemoryPtr + (offset & ~(SEGMENT_SIZE - 1));
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
        if (p == 0) {
            return;
        }
        GAFree(p);
    }
}


static yint hu_getsize(const void *p)
{
    Y_ASSERT(AllMemoryPtr != 0);
    if (p == 0) {
        return 0;
    }
    ui64 offset = ((const char*)p) - AllMemoryPtr;
    if (offset < SEGMENT_COUNT * SEGMENT_SIZE) {
        yint segmentId = offset >> SEGMENT_SIZE_LN;
        char *segment = AllMemoryPtr + (segmentId << SEGMENT_SIZE_LN);
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
        return GAGetSize(p);
    }
}
