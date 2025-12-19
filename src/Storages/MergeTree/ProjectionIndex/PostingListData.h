#pragma once

#include <base/MemorySanitizer.h>
#include <base/defines.h>
#include <base/types.h>

#include <memory>
#include <vector>
#include <roaring/roaring.hh>

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;
using PostingListUniquePtr = std::unique_ptr<PostingList>;

namespace DB
{

class Arena;
class ReadBuffer;
class WriteBuffer;
class MergedPartOffsets;
struct MergeTreeIndexTextParams;

enum class PostingListKind : Int32
{
    Stream = 0,
    Writer,
    Bitmap,
    /// TODO(amos):
    /// We should introduce another variant which holds a single stream that can do seek but cannot merge
};

struct alignas(16) PostingListChunk
{
    PostingListChunk * next = nullptr;
    UInt32 last_doc_id;
    UInt32 len;

    explicit PostingListChunk(UInt32 last_doc_id_, UInt32 len_) noexcept
        : last_doc_id(last_doc_id_)
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
    void write(WriteBuffer & wb) const;
};

struct alignas(8) PostingListWriter
{
    PostingListKind type = PostingListKind::Writer;
    UInt32 doc_count = 0;

    PostingListChunk * blocks_head = nullptr;
    PostingListChunk ** blocks_tail = nullptr;
    UInt32 * doc_delta_buffer = nullptr;

    UInt32 first_doc_id = 0;
    UInt32 last_doc_id = 0;

    void add(UInt32 doc_id, Arena * arena, uint8_t * packed_buffer);
    void
    finish(WriteBuffer & wb, WriteBuffer & large_posting, uint8_t * packed_buffer, const MergeTreeIndexTextParams & index_params) const;
};

static_assert(sizeof(PostingListWriter) <= 40, "PostingListWriter must be less than 40 bytes");
static_assert(std::is_standard_layout_v<PostingListWriter>);

struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;

struct LargePostingBlockMeta
{
    UInt32 last_doc_id;
    UInt32 block_doc_count;
    UInt64 offset;

    /// Default constructor (required by std::vector, resize, etc.)
    LargePostingBlockMeta() noexcept
        : last_doc_id(0)
        , block_doc_count(0)
        , offset(0)
    {
    }

    /// Construct with offset only
    LargePostingBlockMeta(UInt64 offset_) noexcept // NOLINT
        : last_doc_id(0)
        , block_doc_count(0)
        , offset(offset_)
    {
    }

    /// Fully initialized constructor
    LargePostingBlockMeta(UInt32 last_doc_id_, UInt32 doc_count_, UInt64 offset_) noexcept
        : last_doc_id(last_doc_id_)
        , block_doc_count(doc_count_)
        , offset(offset_)
    {
    }

    /// Implicit conversion to offset (INTENTIONALLY implicit)
    operator UInt64() const noexcept { return offset; } // NOLINT

    String toString() const;
};

using LargePostingBlockMetas = std::vector<LargePostingBlockMeta>;

struct ReaderStreamEntry
{
    LargePostingListReaderStreamPtr stream;
    UInt32 first_doc_id;
    UInt32 doc_count;

    /// TODO(amos): Redundant for merge path: blocks are consumed sequentially
    LargePostingBlockMetas large_posting_blocks;

    ReaderStreamEntry(
        LargePostingListReaderStreamPtr stream_, UInt32 first_doc_id_, UInt32 doc_count_, LargePostingBlockMetas large_posting_blocks_);

    bool operator==(const ReaderStreamEntry & other) const { return stream.get() == other.stream.get(); }

    static PostingListPtr materializeLargeBlockIntoBitmap(
        LargePostingListReaderStream & stream, UInt32 last_doc_id, UInt32 doc_count, UInt64 offset, bool include_first_doc);

    String toString() const;
};

struct ReaderStreamVector
{
    std::vector<ReaderStreamEntry> entries;

    ReaderStreamVector() = default;

    ReaderStreamVector(
        LargePostingListReaderStreamPtr stream, UInt32 first_doc_id, UInt32 doc_count, LargePostingBlockMetas large_posting_blocks);

    void merge(const ReaderStreamVector & other);

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
    std::vector<UInt32> merged_embedded_postings;
    ReaderStreamVector streams;

    LazyPostingStream() = default;
    LazyPostingStream(const UInt32 * embedded_postings, UInt32 num_embedded_docs, ReaderStreamVector streams_ = {});
    ~LazyPostingStream();
};

using LazyPostingStreamPtr = std::unique_ptr<LazyPostingStream>;

inline static constexpr UInt64 MAX_SIZE_OF_EMBEDDED_POSTINGS = 6;

struct alignas(8) PostingListStream
{
    PostingListKind type = PostingListKind::Stream;
    UInt32 doc_count = 0;
    UInt32 embedded_postings[MAX_SIZE_OF_EMBEDDED_POSTINGS];
    LazyPostingStreamPtr lazy_posting_stream;

    PostingListStream() = default;
    PostingListStream(const PostingListStream &) = delete;
    PostingListStream & operator=(const PostingListStream &) = delete;

    PostingListStream(PostingListStream && other) noexcept
        : type(other.type)
        , doc_count(other.doc_count)
        , lazy_posting_stream(std::move(other.lazy_posting_stream))
    {
        if (doc_count > 0 && doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
        {
            chassert(!lazy_posting_stream);
            memcpy(embedded_postings, other.embedded_postings, doc_count * sizeof(UInt32));
        }
        other.doc_count = 0;
    }

    PostingListStream & operator=(PostingListStream && other) noexcept
    {
        if (this != &other)
        {
            type = other.type;
            doc_count = other.doc_count;
            lazy_posting_stream = std::move(other.lazy_posting_stream);

            if (doc_count > 0 && doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
            {
                chassert(!lazy_posting_stream);
                memcpy(embedded_postings, other.embedded_postings, doc_count * sizeof(UInt32));
            }

            other.doc_count = 0;
        }
        return *this;
    }

    void read(ReadBuffer & in, const LargePostingListReaderStreamPtr & stream, const MergeTreeIndexTextParams & index_params);

    void write(WriteBuffer & wb, LargePostingListWriterStream & stream, const MergeTreeIndexTextParams & index_params) const;

    /// Collects all doc IDs into `buf`. Caller must preallocate `buf` with more than `doc_count` elements.
    void collect(UInt32 * buf) const;

    void merge(const PostingListStream & other);
};

static_assert(sizeof(PostingListStream) <= 40, "PostingListStream must be less than 40 bytes");
static_assert(std::is_standard_layout_v<PostingListStream>);

struct alignas(8) PostingListBitmap
{
    PostingListKind type = PostingListKind::Bitmap;
    UInt32 doc_count = 0;

    union
    {
        UInt32 embedded_postings[MAX_SIZE_OF_EMBEDDED_POSTINGS];
        roaring::BulkContext context;
    };

    PostingListUniquePtr bitmap;

    PostingListBitmap()
    {
        /// Establish the lifetime of the embedded storage as the active union member.
        /// No initialization is performed here; elements are written before any read.
        new (&embedded_postings) UInt32[MAX_SIZE_OF_EMBEDDED_POSTINGS];
    }

    void add(UInt32 doc_id);

    void finish(
        WriteBuffer & wb,
        WriteBuffer & large_posting,
        UInt32 * doc_buffer,
        uint8_t * packed_buffer,
        const MergeTreeIndexTextParams & index_params) const;
};

static_assert(sizeof(PostingListBitmap) <= 40, "PostingListBitmap must be less than 40 bytes");
static_assert(std::is_standard_layout_v<PostingListBitmap>);

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
        PostingListBitmap bitmap;
        /// TODO(amos):
        /// We should introduce another variant which holds a single stream that can do seek but cannot merge
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

            case PostingListKind::Bitmap:
                new (&bitmap) PostingListBitmap;
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

            case PostingListKind::Bitmap:
                bitmap.~PostingListBitmap();
                break;
        }
    }

    /// ColumnAggregateFunction constructs elements only via insertDefault(), which does not invoke user-defined
    /// constructors for the stored state. The concrete posting list variant must therefore be constructed explicitly
    /// after insertion.
    ///
    /// These methods perform a manual lifetime transition from an empty (no-active-object) state into a concrete
    /// posting list variant using placement new.
    ///
    /// IMPORTANT:
    ///   - Must be called ONLY when kind == PostingListKind::Empty.
    ///   - The underlying storage must not contain any active object.
    ///   - The prior contents of `storage` are irrelevant and must not be
    ///     accessed.
    ///
    /// This is a low-level lifetime-unsafe operation by design.
    void toStreamUnsafe()
    {
        // chassert(empty.type == PostingListKind::Empty);
        destroy();
        new (&stream) PostingListStream();
    }

    template <typename T>
    void toStreamUnsafe(T && other)
    {
        destroy();
        new (&stream) PostingListStream(std::forward<T>(other));
    }

    void toWriterUnsafe()
    {
        destroy();
        new (&writer) PostingListWriter();
    }

    template <typename T>
    void toWriterUnsafe(T && other)
    {
        destroy();
        new (&writer) PostingListWriter(std::forward<T>(other));
    }

    void toBitmapUnsafe()
    {
        destroy();
        new (&bitmap) PostingListBitmap();
    }

    template <typename T>
    void toBitmapUnsafe(T && other)
    {
        destroy();
        new (&bitmap) PostingListBitmap(std::forward<T>(other));
    }

    bool isStream() const { return stream.type == PostingListKind::Stream; }
    bool isWriter() const { return stream.type == PostingListKind::Writer; }
    bool isBitmap() const { return stream.type == PostingListKind::Bitmap; }

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

            case PostingListKind::Bitmap:
                new (&bitmap) PostingListBitmap(std::move(other.bitmap));
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

                case PostingListKind::Bitmap:
                    new (&bitmap) PostingListBitmap(std::move(other.bitmap));
                    break;
            }
        }
        return *this;
    }
};

static_assert(std::is_standard_layout_v<PostingListData>);
static_assert(sizeof(PostingListData) == 40, "PostingListData must be 40 bytes");

}
