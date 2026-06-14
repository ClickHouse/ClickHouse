#pragma once

#include <base/MemorySanitizer.h>
#include <base/defines.h>
#include <base/types.h>

#include <absl/container/flat_hash_map.h>
#include <roaring/roaring.hh>

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

namespace DB
{

class Arena;
class ReadBuffer;
class WriteBuffer;
class MergedPartOffsets;
struct MergeTreeIndexTextParams;
struct PostingListParams;
struct LargePostingListWriterStream;

enum class PostingListKind : Int32
{
    Stream = 0,
    Writer,
};

struct PostingListChunk
{
    PostingListChunk * next = nullptr;
    /// Semantics depend on chunk role in the interleaved [doc][freq]...[pos] chain:
    ///   - doc chunk: last_doc_id of the block (used for binary search at write-to-disk time)
    ///   - freq chunk: sum(freq) for the block — lets PostingListWriter::finish skip decoding
    ///     the freq TurboPFor block just to compute pos_cum_deltas
    ///   - pos page: unused (set to 0)
    union
    {
        UInt32 last_doc_id;
        UInt32 freq_sum;
    };
    UInt32 first_doc_id = 0;
    UInt32 len;

    PostingListChunk(UInt32 aux, UInt32 len_, UInt32 first_doc_ = 0) noexcept
        : last_doc_id(aux)
        , first_doc_id(first_doc_)
        , len(len_)
    {
    }

    /// Returns a pointer to the memory buffer immediately following this chunk structure.
    ///
    /// This uses the "trailing array" / "struct hack" pattern: the actual data buffer is allocated together with the
    /// struct, so that memory is contiguous. For example:
    ///
    ///   // allocate a chunk with len bytes of data
    ///   PostingListChunk* chunk =
    ///       (PostingListChunk*)malloc(sizeof(PostingListChunk) + len);
    ///   chunk->len = len;
    ///   uint8_t* buffer = chunk->data();
    ///
    /// The expression (this + 1) computes the address immediately after this struct, effectively pointing to the start
    /// of the data buffer.
    uint8_t * data() { return reinterpret_cast<uint8_t *>(this + 1); }
    const uint8_t * data() const { return reinterpret_cast<const uint8_t *>(this + 1); }
};

/// TurboPFor encodes/decodes deltas in fixed-size blocks.
/// 256 gives better compression ratio and smaller Index Sections than 128,
/// with equivalent query performance.
inline static constexpr size_t TURBOPFOR_BLOCK_SIZE = 256;

/// Maximum encoded size of a single TurboPFor block (256 elements).
/// Worst case: b=32, bx=0 → 1 header byte + 256×4 payload bytes = 1025.
inline static constexpr size_t TURBOPFOR_MAX_ENCODED_SIZE = TURBOPFOR_BLOCK_SIZE * 4 + 2;

/// Maximum encoded size for 64-bit TurboPFor block (256 elements).
/// Worst case: b=64, bx=0 → 1 header byte + 256×8 payload bytes = 2049.
inline static constexpr size_t TURBOPFOR_MAX_ENCODED_SIZE_64 = TURBOPFOR_BLOCK_SIZE * 8 + 2;

struct LargePostingBlockMeta;
struct LargePostingListReaderStream;

/// Per-large-block metadata loaded lazily via `ProjectionPostingListCursor::ensureLargeBlock`.
struct LargeBlockData
{
    /// Interleaved [first_0, last_0, first_1, last_1, ...], 2 * num_packed_blocks elements.
    /// Strictly monotone: first_i <= last_i < first_{i+1}.
    std::vector<UInt32> packed_block_ranges;
    std::vector<UInt32> packed_block_cum_bytes; /// cumulative end byte offsets, N values (doc+freq combined)
    size_t block_count = 0;
    size_t tail_size = 0;
    UInt32 doc_count = 0;
    UInt64 data_section_start = 0;

    /// Position skip data (phrase mode only, empty when phrase=false)
    std::vector<UInt64> pos_cum_deltas; /// cumulative freq sum per doc block [0..n], n+1 values
    std::vector<UInt64> pos_cum_bytes; /// cumulative bytes per pos TurboPFor block, N values
    UInt32 num_pos_blocks = 0;
    UInt64 pos_start_offset = 0; /// Start offset of this large block's pos data in .pos
    bool has_positions = false;

    /// Number of packed blocks in this large block.
    size_t numPackedBlocks() const { return packed_block_ranges.size() / 2; }

    /// Accessor helpers for the interleaved ranges array.
    UInt32 firstDocIdOf(size_t block) const { return packed_block_ranges[block * 2]; }
    UInt32 lastDocIdOf(size_t block) const { return packed_block_ranges[block * 2 + 1]; }

    /// Decode a LargeBlockData from the .pidx stream for the given large block.
    /// Reads num_packed_blocks, packed_block_ranges, packed_block_cum_bytes,
    /// and optionally position skip data (pos_cum_deltas, pos_cum_bytes).
    /// Returns nullptr if num_packed_blocks == 0.
    static std::shared_ptr<LargeBlockData> decodeFromIndex(
        LargePostingListReaderStream & idx_stream,
        const LargePostingBlockMeta & meta,
        bool decode_positions);
};

/// Query-scoped cache of decoded TurboPFor packed blocks.
///
/// During mark filtering, `hasDocInRangePrecise` may decode a packed block from
/// `.pst` to determine whether a mark range contains any doc_ids. The decoded doc_ids
/// (up to 256 uint32_t) are cached here so that `ProjectionPostingListCursor` can reuse
/// them during the subsequent data-read phase without re-reading `.pst`.
///
/// Lifetime: owned by MergeTreeProjectionIndexGranuleText (query-scoped).
/// Thread safety: single-writer (mark filtering is single-threaded per granule).
class DecodedBlockCache
{
public:
    struct Entry // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        alignas(16) uint32_t doc_ids[TURBOPFOR_BLOCK_SIZE];
        uint32_t count = 0;
    };

    const Entry * get(const LargeBlockData * lb, size_t packed_block_idx) const
    {
        auto it = cache.find(makeKey(lb, packed_block_idx));
        return it != cache.end() ? it->second.get() : nullptr;
    }

    const Entry * put(const LargeBlockData * lb, size_t packed_block_idx, std::unique_ptr<Entry> entry)
    {
        auto * ptr = entry.get();
        cache.emplace(makeKey(lb, packed_block_idx), std::move(entry));
        return ptr;
    }

private:
    static uint64_t makeKey(const LargeBlockData * lb, size_t idx)
    {
        /// Pointer identity is stable: LargeBlockData lives in shared_ptr
        /// inside TokenPostingsInfo, which outlives the cache.
        return folly_hash(reinterpret_cast<uintptr_t>(lb), idx);
    }

    static uint64_t folly_hash(uint64_t a, uint64_t b)
    {
        a ^= b + 0x9e3779b97f4a7c15ULL + (a << 12) + (a >> 4);
        return a;
    }

    absl::flat_hash_map<uint64_t, std::unique_ptr<Entry>> cache;
};

/// Small-page pool for accumulating doc deltas, freqs, and position deltas.
///
/// Pages are fixed-size arrays of PAGE_ELEMS UInt32s (64 bytes). The pool allocates
/// pages in fixed-size segments (SEGMENT_PAGES per segment) and maintains a free list.
/// Free pages embed the next-free index in data[0] (classic slab trick), so there is
/// zero per-page header overhead.
///
/// Segmented layout avoids realloc-copy: growing the pool only allocates a new segment;
/// existing segments stay put. Page lookup is a div+mod on the page index (compiled to
/// shift+and since SEGMENT_PAGES is a power of two).
///
/// Linkage between pages in a chain uses a side table `next_segments[]` parallel to the
/// data segments (UInt32 per page = 4B / 64B = 6% overhead). This avoids storing a
/// pointer in each page and keeps pages SIMD-friendly.
///
/// NOTE: PagePool indices are constrained to 27 bits (<= 134M pages) so that PageChain
/// can pack tail index + tail_used into a single UInt32. 134M pages × 16 elems × 4B =
/// 8.6TB of raw UInt32 per tokenize() call — far beyond any realistic single-part size.
/// Any overflow would be caught by the chassert in `PageChain::setTail()` under debug
/// builds and produce silent corruption in release builds. Guard against this by
/// ensuring that tokenize() is scoped to a single part and pages are recycled aggressively.
struct PagePool
{
    static constexpr UInt32 PAGE_ELEMS = 16;
    static constexpr UInt32 SEGMENT_PAGES = 1024; /// 64KB data + 4KB next-table per segment
    static constexpr UInt32 NIL = (1u << 27) - 1; /// sentinel within 27-bit index space

    std::vector<UInt32 *> segments; /// data: seg[page_in_seg * PAGE_ELEMS + elem]
    std::vector<UInt32 *> next_segments; /// next-index side table
    UInt32 free_head = NIL;
    Arena * arena = nullptr;

    explicit PagePool(Arena * arena_)
        : arena(arena_)
    {
    }

    UInt32 * page(UInt32 idx) { return segments[idx / SEGMENT_PAGES] + (idx % SEGMENT_PAGES) * PAGE_ELEMS; }

    UInt32 & nextOf(UInt32 idx) { return next_segments[idx / SEGMENT_PAGES][idx % SEGMENT_PAGES]; }

    UInt32 alloc()
    {
        if (free_head == NIL)
            grow();
        UInt32 idx = free_head;
        free_head = page(idx)[0];
        nextOf(idx) = NIL;
        return idx;
    }

    void free(UInt32 idx)
    {
        page(idx)[0] = free_head;
        free_head = idx;
    }

    void freeChain(UInt32 head)
    {
        while (head != NIL)
        {
            UInt32 n = nextOf(head);
            free(head);
            head = n;
        }
    }

private:
    void grow();
};

/// Lightweight chain of pages in a PagePool. Zero-overhead per token.
/// Accumulates UInt32 values; when full (TURBOPFOR_BLOCK_SIZE elements),
/// the caller gathers into a scratch buffer for TurboPFor encoding.
///
/// Compact 8-byte layout: head (32 bits) + tail+used packed (27+5 bits).
/// NOTE: tail index uses 27 bits (max 134M pages). See PagePool for the
/// rationale and overflow guarantees.
struct PageChain
{
    static constexpr UInt32 USED_BITS = 5; /// 5 bits: 0..16 inclusive
    static constexpr UInt32 USED_MASK = (1u << USED_BITS) - 1;
    static constexpr UInt32 NIL_TAIL_AND_USED = PagePool::NIL << USED_BITS;

    UInt32 head = PagePool::NIL;
    UInt32 tail_and_used = NIL_TAIL_AND_USED;

    UInt32 tail() const { return tail_and_used >> USED_BITS; }
    UInt32 tailUsed() const { return tail_and_used & USED_MASK; }

    void setTail(UInt32 t, UInt32 u)
    {
        /// Catch index overflow in debug builds; silent truncation in release.
        chassert(t == PagePool::NIL || t < PagePool::NIL);
        chassert(u <= PagePool::PAGE_ELEMS);
        tail_and_used = (t << USED_BITS) | u;
    }

    void append(UInt32 value, PagePool & pool)
    {
        UInt32 t = tail();
        UInt32 u = tailUsed();
        if (head == PagePool::NIL || u == PagePool::PAGE_ELEMS)
        {
            UInt32 p = pool.alloc();
            if (head == PagePool::NIL)
                head = p;
            else
                pool.nextOf(t) = p;
            t = p;
            u = 0;
        }
        pool.page(t)[u++] = value;
        setTail(t, u);
    }

    /// Gather all elements into a contiguous buffer. Returns element count.
    UInt32 gather(PagePool & pool, UInt32 * out) const
    {
        UInt32 copied = 0;
        UInt32 p = head;
        UInt32 t = tail();
        UInt32 used_at_tail = tailUsed();
        while (p != PagePool::NIL)
        {
            UInt32 n = (p == t) ? used_at_tail : PagePool::PAGE_ELEMS;
            memcpy(out + copied, pool.page(p), n * sizeof(UInt32));
            copied += n;
            p = pool.nextOf(p);
        }
        return copied;
    }

    /// Total number of elements across all pages.
    UInt32 size(PagePool & pool) const
    {
        if (head == PagePool::NIL)
            return 0;
        UInt32 count = 0;
        UInt32 p = head;
        UInt32 t = tail();
        UInt32 used_at_tail = tailUsed();
        while (p != PagePool::NIL)
        {
            count += (p == t) ? used_at_tail : PagePool::PAGE_ELEMS;
            p = pool.nextOf(p);
        }
        return count;
    }

    bool empty() const { return head == PagePool::NIL; }

    void freeAll(PagePool & pool)
    {
        pool.freeChain(head);
        head = PagePool::NIL;
        tail_and_used = NIL_TAIL_AND_USED;
    }
};

struct alignas(8) PostingListWriter
{
    PostingListKind type = PostingListKind::Writer;
    UInt32 doc_count = 0;
    UInt32 first_doc_id = 0;
    UInt32 first_doc_freq = 0;
    PostingListChunk * blocks_head = nullptr;

    void finish(
        WriteBuffer & wb,
        LargePostingListWriterStream & stream,
        const MergeTreeIndexTextParams & index_params,
        WriteBuffer * index_stream = nullptr,
        LargePostingListWriterStream * pos_stream = nullptr) const;
};

static_assert(sizeof(PostingListWriter) <= 24);
static_assert(std::is_standard_layout_v<PostingListWriter>);

/// Per-token write context for non-phrase mode.
/// Placed immediately after PostingListWriter in the Arena, shares doc_count/first_doc_id/blocks_head
/// with the writer via writer() accessor.
struct TokenWriteContext
{
    UInt32 last_doc_id = 0;
    UInt32 block_first_doc_id = 0;
    PageChain doc_deltas;
    PostingListChunk ** blocks_tail = nullptr;

    PostingListWriter & writer() { return reinterpret_cast<PostingListWriter *>(this)[-1]; }

    void add(UInt32 doc_id, PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer);
    void finalize(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer);
};

/// Per-token write context for phrase mode.
/// Uses PageChain for doc deltas, freqs, and position deltas.
/// Pages are allocated from a shared PagePool (per tokenize() call).
struct TokenWriteContextPhrase
{
    UInt32 last_doc_id = 0;
    UInt32 current_doc_id = 0;
    UInt32 current_doc_freq = 0;
    UInt32 last_position = 0;
    UInt32 block_first_doc_id = 0;

    PageChain doc_deltas;
    PageChain freqs;
    PostingListChunk ** blocks_tail = nullptr;

    PageChain positions;
    PostingListChunk * pos_pages_head = nullptr;
    PostingListChunk ** pos_pages_tail = nullptr;

    PostingListWriter & writer() { return reinterpret_cast<PostingListWriter *>(this)[-1]; }

    void add(UInt32 doc_id, UInt32 position, PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer);
    void finalize(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer);

private:
    void flushBlock(PagePool & pool, Arena & chunk_arena, UInt32 * scratch, uint8_t * packed_buffer);
    void sealPosPage(PagePool & pool, Arena & chunk_arena, UInt32 * scratch);
};

struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;

struct LargePostingBlockMeta
{
    UInt32 first_doc_id;
    UInt32 last_doc_id;
    UInt32 block_doc_count;
    UInt64 offset; /// Data Section start offset in .pst
    UInt64 index_offset; /// Index Section start offset in .pidx (0 when block index feature is absent)
    UInt64 pos_start_offset; /// Position data start offset in .pos (0 when phrase is disabled)

    /// Default constructor (required by std::vector, resize, etc.)
    LargePostingBlockMeta() noexcept
        : first_doc_id(0)
        , last_doc_id(0)
        , block_doc_count(0)
        , offset(0)
        , index_offset(0)
        , pos_start_offset(0)
    {
    }

    /// Construct with offset only
    LargePostingBlockMeta(UInt64 offset_) noexcept // NOLINT
        : first_doc_id(0)
        , last_doc_id(0)
        , block_doc_count(0)
        , offset(offset_)
        , index_offset(0)
        , pos_start_offset(0)
    {
    }

    /// Fully initialized constructor (without index_offset)
    LargePostingBlockMeta(UInt32 first_doc_id_, UInt32 last_doc_id_, UInt32 doc_count_, UInt64 offset_) noexcept
        : first_doc_id(first_doc_id_)
        , last_doc_id(last_doc_id_)
        , block_doc_count(doc_count_)
        , offset(offset_)
        , index_offset(0)
        , pos_start_offset(0)
    {
    }

    /// Fully initialized constructor (with index_offset)
    LargePostingBlockMeta(
        UInt32 first_doc_id_,
        UInt32 last_doc_id_,
        UInt32 doc_count_,
        UInt64 offset_,
        UInt64 index_offset_,
        UInt64 pos_start_offset_ = 0) noexcept
        : first_doc_id(first_doc_id_)
        , last_doc_id(last_doc_id_)
        , block_doc_count(doc_count_)
        , offset(offset_)
        , index_offset(index_offset_)
        , pos_start_offset(pos_start_offset_)
    {
    }

    /// Implicit conversion to offset (INTENTIONALLY implicit)
    operator UInt64() const noexcept { return offset; } // NOLINT

    String toString() const;
};

using LargePostingBlockMetas = std::vector<LargePostingBlockMeta>;

/// Returns true if the block metadata indicates that Index Sections are present.
struct ReaderStreamEntry
{
    LargePostingListReaderStreamPtr stream;
    UInt32 first_doc_id;
    UInt32 doc_count;
    UInt32 first_doc_freq = 0;
    UInt64 first_doc_pos_offset = 0;

    LargePostingBlockMetas large_posting_blocks;

    LargePostingListReaderStreamPtr pos_stream;

    /// Materialized doc_ids from deserialized state (used during merge).
    /// When non-null, collect/iterate reads from this bitmap instead of the stream.
    PostingListPtr embedded_postings;

    ReaderStreamEntry(
        LargePostingListReaderStreamPtr stream_, UInt32 first_doc_id_, UInt32 doc_count_, LargePostingBlockMetas large_posting_blocks_);

    /// Identity is the underlying stream pointer when one is attached. For embedded entries
    /// (deserialized from `AggregateFunctionPostingList::deserialize`, `stream == nullptr`),
    /// fall back to `(first_doc_id, doc_count, embedded_postings)` so two distinct embedded
    /// states do not falsely compare equal and trip `ReaderStreamVector::merge`'s
    /// duplicate-stream check.
    bool operator==(const ReaderStreamEntry & other) const
    {
        if (stream || other.stream)
            return stream.get() == other.stream.get();
        return first_doc_id == other.first_doc_id
            && doc_count == other.doc_count
            && embedded_postings.get() == other.embedded_postings.get();
    }

    static PostingListPtr materializeLargeBlockIntoBitmap(
        LargePostingListReaderStream & stream, UInt32 last_doc_id, UInt32 doc_count, UInt64 offset, bool include_first_doc, bool has_freq = false);

    String toString() const;
};

struct ReaderStreamVector
{
    std::vector<ReaderStreamEntry> entries;

    ReaderStreamVector() = default;

    ReaderStreamVector(
        LargePostingListReaderStreamPtr stream, UInt32 first_doc_id, UInt32 doc_count, LargePostingBlockMetas large_posting_blocks);

    void merge(ReaderStreamVector & other);

    size_t size() const { return entries.size(); }

    bool empty() const { return entries.empty(); }

    void clear() { entries.clear(); }

    // --- support range-based for ---
    auto begin() { return entries.begin(); }
    auto end() { return entries.end(); }
    auto begin() const { return entries.begin(); }
    auto end() const { return entries.end(); }

    String toString() const;
};

struct LargePostingListWriterStream;

/// Initialized once during deserialization and reused across columns.
struct LazyPostingStream
{
    ReaderStreamVector streams;

    LazyPostingStream() = default;
    explicit LazyPostingStream(ReaderStreamVector streams_);
    ~LazyPostingStream();
};

struct alignas(8) PostingListStream
{
    PostingListKind type = PostingListKind::Stream;
    UInt32 doc_count = 0;
    LazyPostingStream * lazy = nullptr;
    UInt32 first_doc_freq = 0;
    UInt32 first_doc_id = 0;

    PostingListStream() = default;
    PostingListStream(const PostingListStream &) = delete;
    PostingListStream & operator=(const PostingListStream &) = delete;

    PostingListStream(PostingListStream && other) noexcept
        : type(other.type)
        , doc_count(other.doc_count)
        , lazy(other.lazy)
        , first_doc_freq(other.first_doc_freq)
        , first_doc_id(other.first_doc_id)
    {
        other.lazy = nullptr;
        other.doc_count = 0;
    }

    PostingListStream & operator=(PostingListStream && other) noexcept
    {
        if (this != &other)
        {
            delete lazy;
            type = other.type;
            doc_count = other.doc_count;
            lazy = other.lazy;
            first_doc_freq = other.first_doc_freq;
            first_doc_id = other.first_doc_id;
            other.lazy = nullptr;
            other.doc_count = 0;
        }
        return *this;
    }

    ~PostingListStream() { delete lazy; }

    void read(
        ReadBuffer & in,
        const LargePostingListReaderStreamPtr & stream,
        const MergeTreeIndexTextParams & index_params,
        const LargePostingListReaderStreamPtr & pos_stream = nullptr);

    static void skip(ReadBuffer & in, const MergeTreeIndexTextParams & index_params);

    void write(
        WriteBuffer & wb,
        LargePostingListWriterStream & stream,
        const MergeTreeIndexTextParams & index_params,
        LargePostingListWriterStream * index_stream = nullptr,
        WriteBuffer * pos_writer = nullptr) const;
    void collect(UInt32 * buf) const;

    void merge(PostingListStream & other);
};

static_assert(sizeof(PostingListStream) == 24, "PostingListStream must be 24 bytes");
static_assert(std::is_standard_layout_v<PostingListStream>);

/// PostingListData is a manually managed tagged union optimized for minimal memory footprint.
///
/// Design rationale:
///
/// - All union alternatives are standard-layout types and share a Common Initial Sequence (CIS) consisting of the first
///   member:
///
///       PostingListKind type;
///
/// - Accessing `*.type` to discriminate the active alternative is well-defined in C++11 and later for standard-layout
///   unions.
///
/// - Object lifetimes of union members are explicitly managed via placement new and manual destruction; no aliasing or
///   speculative access is used.
///
/// This design intentionally avoids std::variant to minimize size. With alignas(8), the layout is tightly packed and
/// currently occupies exactly 40 bytes.
///
/// Any change to union members MUST preserve:
///   - standard-layout
///   - identical first member (type) across all alternatives
struct PostingListData
{
    union
    {
        PostingListStream stream;
        PostingListWriter writer;
    };

    PostingListData() { new (&stream) PostingListStream; }

    explicit PostingListData(PostingListKind kind)
    {
        switch (kind)
        {
            case PostingListKind::Stream:
                new (&stream) PostingListStream;
                break;

            case PostingListKind::Writer:
                new (&writer) PostingListWriter;
                break;
        }
    }
    PostingListData(const PostingListData &) = delete;
    PostingListData & operator=(const PostingListData &) = delete;

    ~PostingListData() { destroy(); }

    void destroy()
    {
        switch (stream.type)
        {
            case PostingListKind::Stream:
                stream.~PostingListStream();
                break;

            case PostingListKind::Writer:
                writer.~PostingListWriter();
                break;
        }
    }

    bool isStream() const { return stream.type == PostingListKind::Stream; }
    bool isWriter() const { return stream.type == PostingListKind::Writer; }

    PostingListData(PostingListData && other) noexcept
    {
        switch (other.stream.type)
        {
            case PostingListKind::Stream:
                new (&stream) PostingListStream(std::move(other.stream));
                break;

            case PostingListKind::Writer:
                new (&writer) PostingListWriter(std::move(other.writer));
                break;
        }
    }

    PostingListData & operator=(PostingListData && other) noexcept
    {
        if (this != &other)
        {
            destroy();
            switch (other.stream.type)
            {
                case PostingListKind::Stream:
                    new (&stream) PostingListStream(std::move(other.stream));
                    break;

                case PostingListKind::Writer:
                    new (&writer) PostingListWriter(std::move(other.writer));
                    break;
            }
        }
        return *this;
    }
};

static_assert(std::is_standard_layout_v<PostingListData>);
static_assert(sizeof(PostingListData) == 24, "PostingListData must be 24 bytes");

}
