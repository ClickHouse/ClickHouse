#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/ProjectionIndex/LengthPrefixedInt.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <base/scope_guard.h>
#include <Common/Arena.h>
#include <Common/Exception.h>

#include <fmt/ranges.h>
#include <turbopfor.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

/// Alias: VarInt functions delegate to the shared LengthPrefixedInt header.
namespace VarInt = LengthPrefixedInt;

void PostingListChunk::write(WriteBuffer & wb) const
{
    wb.write(reinterpret_cast<const char *>(data()), len);
}

void PostingListWriter::add(UInt32 doc_id, Arena * arena, uint8_t * packed_buffer)
{
    if (doc_count == 0)
    {
        first_doc_id = doc_id;
        last_doc_id = doc_id;
        ++doc_count;
        return;
    }

    if (doc_id < last_doc_id)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Received out of order doc id. doc_id = {}, last_doc_id = {}", doc_id, last_doc_id);

    if (doc_id == last_doc_id)
        return;

    /// TODO(amos): Arena reallocation here causes memory waste, because the old buffer cannot be reclaimed or reused.
    /// We may replace it with a small-size bucket / buddy-style allocator to reuse buffers and reduce realloc + copy
    /// overhead.
    /// 1, 16, 128
    switch (doc_count)
    {
        case 1:
            doc_delta_buffer = reinterpret_cast<UInt32 *>(arena->alignedAlloc(16 * 4, 4));
            break;
        case 17:
            doc_delta_buffer
                = reinterpret_cast<UInt32 *>(arena->alignedRealloc(reinterpret_cast<char *>(doc_delta_buffer), 16 * 4, TURBOPFOR_BLOCK_SIZE * 4, 16));
            break;
        default:
            break;
    }

    UInt8 doc_buffer_up_to = (doc_count - 1) % TURBOPFOR_BLOCK_SIZE;
    UInt32 doc_delta = doc_id - last_doc_id - 1;
    doc_delta_buffer[doc_buffer_up_to] = doc_delta;

    last_doc_id = doc_id;
    ++doc_buffer_up_to;
    ++doc_count;

    if (doc_buffer_up_to == TURBOPFOR_BLOCK_SIZE)
    {
        uint8_t * packed_buffer_end = turbopfor::p4Enc128v32(doc_delta_buffer, TURBOPFOR_BLOCK_SIZE, packed_buffer);
        UInt32 len = static_cast<UInt32>(packed_buffer_end - packed_buffer);
        chassert(len <= 512);
        auto * place = arena->alignedAlloc(len + sizeof(PostingListChunk), alignof(PostingListChunk));
        PostingListChunk * cur_block = new (place) PostingListChunk(last_doc_id, len);
        memcpy(cur_block->data(), packed_buffer, len);
        if (!blocks_head)
            blocks_head = cur_block;
        else
            *blocks_tail = cur_block;
        blocks_tail = &cur_block->next;
    }
}

class LargePostingBlockWriter
{
public:
    LargePostingBlockWriter(WriteBuffer & meta_out_, WriteBuffer & data_out_, UInt32 docs_per_large_block_, bool write_block_index_)
        : meta_out(meta_out_)
        , data_out(data_out_)
        , docs_per_large_block(docs_per_large_block_)
        , write_block_index(write_block_index_)
    {
        if (write_block_index)
        {
            UInt32 packed_blocks_per_large_block = docs_per_large_block / TURBOPFOR_BLOCK_SIZE;
            packed_block_last_doc_ids.reserve(packed_blocks_per_large_block);
            packed_block_offsets.reserve(packed_blocks_per_large_block);
        }
    }

    void addBlock(UInt32 last_doc_id, const char * data, UInt32 bytes)
    {
        if (docs_in_current_block == 0)
            large_block_start_offset = data_out.count();

        if (write_block_index)
        {
            /// Record the absolute offset of this sub-block before writing.
            packed_block_last_doc_ids.push_back(last_doc_id);
            packed_block_offsets.push_back(data_out.count());
        }

        VarInt::writeUInt32(bytes, data_out);
        data_out.write(data, bytes);

        /// Always count a packed block as TURBOPFOR_BLOCK_SIZE docs.
        /// The tail block is the final one and will be flushed immediately,
        /// so treating it as full does not affect block layout.
        docs_in_current_block += TURBOPFOR_BLOCK_SIZE;
        current_block_last_doc_id = last_doc_id;

        if (docs_in_current_block >= docs_per_large_block)
            flushLargeBlock();
    }

    void finish(UInt32 num_large_blocks_expected)
    {
        if (docs_in_current_block > 0)
            flushLargeBlock();

        chassert(num_large_blocks_written == num_large_blocks_expected);
    }

private:
    void flushLargeBlock()
    {
        /// V1/V2 shared: offset_in_lpst always points to Data Section start
        VarInt::writeUInt32(current_block_last_doc_id, meta_out);
        writeVarUInt(large_block_start_offset, meta_out);

        if (write_block_index)
        {
            /// Data Section is already written to data_out. Now append the Index Section.
            UInt64 index_section_offset = data_out.count();

            UInt32 num_packed_blocks = static_cast<UInt32>(packed_block_last_doc_ids.size());

            /// Write Index Section:
            /// [PrefixVarInt: num_packed_blocks]
            VarInt::writeUInt32(num_packed_blocks, data_out);
            /// N × [PrefixVarInt: last_doc_id]
            for (const auto & id : packed_block_last_doc_ids)
                VarInt::writeUInt32(id, data_out);
            /// N × [VarUInt64: absolute_offset]
            for (const auto & off : packed_block_offsets)
                writeVarUInt(off, data_out);

            packed_block_last_doc_ids.clear();
            packed_block_offsets.clear();

            /// Write index_offset_in_lpst to dictionary stream (V2 only).
            writeVarUInt(index_section_offset, meta_out);
        }

        /// Reset for next large block.
        docs_in_current_block = 0;
        ++num_large_blocks_written;
    }

    WriteBuffer & meta_out;
    WriteBuffer & data_out;

    UInt32 docs_per_large_block;
    bool write_block_index;
    UInt32 docs_in_current_block = 0;
    UInt32 current_block_last_doc_id = 0;
    UInt32 num_large_blocks_written = 0;
    UInt64 large_block_start_offset = 0;

    std::vector<UInt32> packed_block_last_doc_ids;
    std::vector<UInt64> packed_block_offsets;
};

void PostingListWriter::finish(
    WriteBuffer & wb, WriteBuffer & large_posting, uint8_t * packed_buffer, const MergeTreeIndexTextParams & index_params) const
{
    VarInt::writeUInt32(doc_count, wb);
    if (doc_count == 0)
        return;

    VarInt::writeUInt32(first_doc_id, wb);

    /// Single doc: nothing more to write
    if (doc_count == 1)
        return;

    /// Very small posting list:
    /// inline encode all doc deltas directly into wb
    if (doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
    {
        uint8_t * packed_buffer_end = turbopfor::p4Enc32(doc_delta_buffer, doc_count - 1, packed_buffer);
        UInt32 len = static_cast<UInt32>(packed_buffer_end - packed_buffer);
        VarInt::writeUInt32(len, wb);
        wb.write(reinterpret_cast<const char *>(packed_buffer), len);
        return;
    }

    /// PostingList Format
    /// --------------------------------------------
    /// Large posting list mode
    ///
    /// wb:
    ///   doc_count
    ///   first_doc_id
    ///   num_large_blocks
    ///   [last_doc_id, offset] * N
    ///
    /// large_posting:
    ///   Packed128Block #0
    ///   Packed128Block #1
    ///   ...
    /// --------------------------------------------

    /// Align posting_list_block_size up to TURBOPFOR_BLOCK_SIZE docs, so that each large block
    /// consists of an integral number of packed blocks.
    const UInt32 docs_per_large_block = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);

    /// The first document is stored inline, so only (doc_count - 1) documents
    /// are written into the large_posting stream.
    const UInt32 large_doc_count = doc_count - 1;

    /// Total number of large blocks in large_posting, computed as ceil division.
    const UInt32 num_large_blocks = (large_doc_count + docs_per_large_block - 1) / docs_per_large_block;

    chassert(num_large_blocks >= 1);
    VarInt::writeUInt32(num_large_blocks, wb);

    LargePostingBlockWriter block_writer(wb, large_posting, docs_per_large_block,
        postingListFormatHasBlockIndex(resolvePostingListFormatVersion(index_params.posting_list_version)));

    /// Iterate packed TURBOPFOR_BLOCK_SIZE-doc chunks
    PostingListChunk * it = blocks_head;
    while (it != nullptr)
    {
        block_writer.addBlock(it->last_doc_id, reinterpret_cast<const char *>(it->data()), it->len);
        it = it->next;
    }

    /// Tail packed block (large_doc_count % TURBOPFOR_BLOCK_SIZE)
    UInt8 doc_buffer_up_to = large_doc_count % TURBOPFOR_BLOCK_SIZE;
    if (doc_buffer_up_to > 0)
    {
        uint8_t * packed_buffer_end = turbopfor::p4Enc32(doc_delta_buffer, doc_buffer_up_to, packed_buffer);
        UInt32 len = static_cast<UInt32>(packed_buffer_end - packed_buffer);
        block_writer.addBlock(last_doc_id, reinterpret_cast<const char *>(packed_buffer), len);
    }

    block_writer.finish(num_large_blocks);
}

ReaderStreamEntry::ReaderStreamEntry(
    LargePostingListReaderStreamPtr stream_, UInt32 first_doc_id_, UInt32 doc_count_, LargePostingBlockMetas large_posting_blocks_)
    : stream(std::move(stream_))
    , first_doc_id(first_doc_id_)
    , doc_count(doc_count_)
    , large_posting_blocks(std::move(large_posting_blocks_))
{
    chassert(stream);
}

ReaderStreamVector::ReaderStreamVector(
    LargePostingListReaderStreamPtr stream, UInt32 first_doc_id, UInt32 doc_count, LargePostingBlockMetas large_posting_blocks)
    : entries({{std::move(stream), first_doc_id, doc_count, std::move(large_posting_blocks)}})
{
}

void ReaderStreamVector::merge(const ReaderStreamVector & other)
{
    for (const auto & oe : other.entries)
    {
        for (const auto & e : entries)
        {
            if (e == oe)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate LargePostingListReaderStream detected in merge");
        }
        entries.emplace_back(oe);
    }
    /// Propagate format_version from the other side if this side has no entries yet.
    if (format_version == 0 && other.format_version != 0)
        format_version = other.format_version;
}

struct ReaderStreamCursor
{
    LargePostingListReaderStream * stream;

    UInt32 * doc_buffer;
    UInt32 last_doc_id;
    UInt32 remaining_count;
    UInt32 buf_size;
    UInt32 pos;
    UInt64 offset;
    bool do_seek;

    /// V2 large block boundary tracking: when Index Sections are interleaved
    /// between Data Sections, we need to skip over them during sequential reads.
    /// Each entry is (docs_remaining_in_this_block, next_block_data_offset).
    /// When `remaining_in_large_block` hits 0, seek to the next block's offset.
    struct LargeBlockSkip
    {
        UInt32 doc_count;
        UInt64 next_data_offset; /// 0 means last block, no skip needed
    };
    std::vector<LargeBlockSkip> large_block_skips;
    size_t current_large_block = 0;
    UInt32 remaining_in_large_block = 0;

    /// Disk-based c'tor
    ReaderStreamCursor(
        LargePostingListReaderStream * s,
        UInt32 first_doc_id,
        UInt32 remaining_count_,
        UInt64 offset_,
        bool do_seek_,
        bool include_first_doc)
        : stream(s)
        , doc_buffer(stream->doc_buffer)
        , last_doc_id(first_doc_id)
        , remaining_count(remaining_count_)
        , buf_size(1)
        , pos(0)
        , offset(offset_)
        , do_seek(do_seek_)
    {
        chassert(stream);
        chassert(doc_buffer);

        if (!do_seek)
            chassert(static_cast<UInt64>(stream->getPosition()) == offset);

        if (include_first_doc)
        {
            doc_buffer[0] = first_doc_id;
            if (stream->merged_part_offsets)
                stream->merged_part_offsets->mapOffsets(stream->part_index, doc_buffer, 1);
        }
        else
        {
            next();
        }
    }

    /// Embedded postings c'tor
    ReaderStreamCursor(UInt32 * doc_buffer_, UInt32 buf_size_)
        : stream(nullptr)
        , doc_buffer(doc_buffer_)
        , last_doc_id(0)
        , remaining_count(0)
        , buf_size(buf_size_)
        , pos(0)
        , offset(0)
        , do_seek(false)
    {
        chassert(doc_buffer);
    }

    /// Set up large block skip schedule for V2 format.
    /// `blocks` contains per-large-block metadata; when a large block's Data Section
    /// is fully consumed, the cursor seeks past its Index Section to the next block's
    /// Data Section offset.
    void setLargeBlockSkips(const LargePostingBlockMetas & blocks)
    {
        large_block_skips.resize(blocks.size());
        for (size_t i = 0; i < blocks.size(); ++i)
        {
            large_block_skips[i].doc_count = blocks[i].block_doc_count;
            large_block_skips[i].next_data_offset = (i + 1 < blocks.size()) ? blocks[i + 1].offset : 0;
        }
        current_large_block = 0;
        remaining_in_large_block = blocks.empty() ? 0 : blocks[0].block_doc_count;
    }

    UInt32 ALWAYS_INLINE current() const
    {
        chassert(pos < buf_size);
        return doc_buffer[pos];
    }

    void ALWAYS_INLINE next()
    {
        chassert(pos < buf_size);
        ++pos;
        if (pos >= buf_size)
            loadNextBlock();
    }

    bool ALWAYS_INLINE empty() const { return buf_size == pos && remaining_count == 0; }

    /// Batch emit all remaining documents in current buffer and beyond
    template <typename Emit>
    void emitAll(Emit && emit)
    {
        while (!empty())
        {
            size_t remaining = buf_size - pos;
            for (size_t i = 0; i < remaining; ++i)
                emit(doc_buffer[pos + i]);
            pos = buf_size;
            loadNextBlock();
        }
    }

    /// Emit up to next_min (exclusive), loading blocks as needed
    template <typename Emit>
    void emitUntil(UInt32 next_min, Emit && emit)
    {
        chassert(current() < next_min);
        while (!empty())
        {
            // Emit entire block if all elements < next_min
            if (doc_buffer[buf_size - 1] < next_min)
            {
                for (size_t i = pos; i < buf_size; ++i)
                    emit(doc_buffer[i]);
                pos = buf_size;
                loadNextBlock();
            }
            else
            {
                // Emit partial block up to next_min
                const UInt32 * it = std::lower_bound(doc_buffer + pos, doc_buffer + buf_size, next_min);
                for (const UInt32 * p = doc_buffer + pos; p != it; ++p)
                    emit(*p);
                pos = static_cast<UInt32>(it - doc_buffer);
                break;
            }
        }
    }

    PostingListPtr materializeIntoBitmap()
    {
        auto bitmap = std::make_shared<PostingList>();
        while (!empty())
        {
            chassert(pos == 0);
            bitmap->addMany(buf_size, doc_buffer);
            pos = buf_size;
            loadNextBlock();
        }
        return bitmap;
    }

    bool ALWAYS_INLINE operator<(const ReaderStreamCursor & rhs) const { return current() < rhs.current(); }

private:
    void loadNextBlock()
    {
        if (remaining_count == 0)
            return;

        /// V2 large block boundary: if we've consumed all docs in the current
        /// large block, skip over the trailing Index Section by seeking to the
        /// next large block's Data Section offset.
        if (!large_block_skips.empty() && remaining_in_large_block == 0)
        {
            ++current_large_block;
            chassert(current_large_block < large_block_skips.size());
            remaining_in_large_block = large_block_skips[current_large_block].doc_count;

            /// Seek past the Index Section to the next Data Section
            UInt64 next_offset = large_block_skips[current_large_block - 1].next_data_offset;
            if (next_offset != 0)
            {
                stream->seek(next_offset);
                do_seek = false;
            }
        }

        if (do_seek)
        {
            stream->seek(offset);
            do_seek = false;
        }

        auto & data_buf = *stream->getDataBuffer();
        UInt32 bytes;
        VarInt::readUInt32(bytes, data_buf);
        UInt32 count = std::min(remaining_count, static_cast<UInt32>(TURBOPFOR_BLOCK_SIZE));
        uint8_t * src_ptr;
        if (data_buf.available() >= bytes)
        {
            src_ptr = reinterpret_cast<uint8_t *>(data_buf.position());
            data_buf.position() += bytes;
        }
        else
        {
            chassert(bytes <= 512);
            data_buf.readStrict(reinterpret_cast<char *>(stream->packed_buffer), bytes);
            src_ptr = stream->packed_buffer;
        }

        if (count == TURBOPFOR_BLOCK_SIZE)
            turbopfor::p4D1Dec128v32(src_ptr, TURBOPFOR_BLOCK_SIZE, doc_buffer, last_doc_id);
        else
            turbopfor::p4D1Dec32(src_ptr, count, doc_buffer, last_doc_id);

        last_doc_id = doc_buffer[count - 1];
        remaining_count -= count;
        buf_size = count;
        pos = 0;

        if (!large_block_skips.empty())
            remaining_in_large_block -= count;

        if (stream->merged_part_offsets)
            stream->merged_part_offsets->mapOffsets(stream->part_index, doc_buffer, count);
    }
};

struct ReaderStreamCursorNode
{
    ReaderStreamCursor * cursor;

    /// Inverted so that the priority queue elements are removed in ascending order.
    bool ALWAYS_INLINE operator<(const ReaderStreamCursorNode & rhs) const { return cursor->current() > rhs.cursor->current(); }
};

using ReaderStreamQueue = std::priority_queue<ReaderStreamCursorNode, std::vector<ReaderStreamCursorNode>>;

template <typename EmitFirst, typename Emit>
void mergePostingCursors(std::vector<ReaderStreamCursor> & cursors, EmitFirst && emit_first, Emit && emit)
{
    cursors.erase(std::remove_if(cursors.begin(), cursors.end(), [](const auto & c) { return c.empty(); }), cursors.end());

    if (cursors.empty())
        return;

    if (cursors.size() == 1)
    {
        emit_first(cursors[0].current());
        cursors[0].next();
        cursors[0].emitAll(emit);
        return;
    }

    ReaderStreamQueue heap;
    for (auto & c : cursors)
        heap.push(ReaderStreamCursorNode{&c});

    /// Emit first doc
    {
        auto cur = heap.top();
        heap.pop();
        emit_first(cur.cursor->current());
        cur.cursor->next();
        if (!cur.cursor->empty())
            heap.push(cur);
    }

    while (!heap.empty())
    {
        auto cur = heap.top();
        heap.pop();

        if (heap.empty())
            cur.cursor->emitAll(emit);
        else
            cur.cursor->emitUntil(heap.top().cursor->current(), emit);

        if (!cur.cursor->empty())
            heap.push(cur);
    }
}

PostingListPtr ReaderStreamEntry::materializeLargeBlockIntoBitmap(
    LargePostingListReaderStream & stream, UInt32 last_doc_id, UInt32 block_doc_count, UInt64 offset, bool include_first_doc)
{
    LOG_DEBUG(
        &::Poco::Logger::get("amosbird"),
        "last_doc_id = {}, block_doc_count = {}, offset = {}, include_first_doc = {}",
        last_doc_id,
        block_doc_count,
        offset,
        include_first_doc);

    stream.seek(offset);

    ReaderStreamCursor cursor(
        &stream,
        last_doc_id,
        block_doc_count,
        offset,
        false /* do_seek: already positioned at Data Section */,
        include_first_doc);
    return cursor.materializeIntoBitmap();
}

std::string LargePostingBlockMeta::toString() const
{
    return fmt::format("{{last_doc_id: {}, block_doc_count: {}, offset: {}, index_offset: {}}}", last_doc_id, block_doc_count, offset, index_offset);
}

std::string ReaderStreamEntry::toString() const
{
    std::vector<std::string> block_strs;
    block_strs.reserve(large_posting_blocks.size());
    for (const auto & block : large_posting_blocks)
        block_strs.push_back(block.toString());

    return fmt::format(
        "ReaderStreamEntry(stream_ptr: {}, first_doc_id: {}, doc_count: {}, blocks: [{}])",
        static_cast<const void *>(stream.get()),
        first_doc_id,
        doc_count,
        fmt::join(block_strs, ", "));
}

std::string ReaderStreamVector::toString() const
{
    if (entries.empty())
        return "ReaderStreamVector(empty)";

    std::vector<std::string> entry_strs;
    entry_strs.reserve(entries.size());
    for (const auto & entry : entries)
        entry_strs.push_back(entry.toString());

    return fmt::format("ReaderStreamVector(size: {}) [\n  {}\n]", entries.size(), fmt::join(entry_strs, ",\n  "));
}

LazyPostingStream::LazyPostingStream(const UInt32 * embedded_postings, UInt32 num_embedded_docs, ReaderStreamVector streams_)
    : merged_embedded_postings(embedded_postings, embedded_postings + num_embedded_docs)
    , streams(std::move(streams_))
{
}

LazyPostingStream::~LazyPostingStream() = default;

void PostingListStream::read(ReadBuffer & in, const LargePostingListReaderStreamPtr & stream, const MergeTreeIndexTextParams & index_params, size_t format_version)
{
    VarInt::readUInt32(doc_count, in);
    if (doc_count == 0)
        return;

    /// Last document id, used as base for delta decoding
    UInt32 last_doc_id;
    VarInt::readUInt32(last_doc_id, in);

    chassert(stream);

    if (doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
    {
        embedded_postings[0] = last_doc_id;
        if (doc_count > 1)
        {
            UInt32 bytes;
            VarInt::readUInt32(bytes, in);
            if (in.available() >= bytes)
            {
                uint8_t * packed_buffer_end
                    = turbopfor::p4D1Dec32(reinterpret_cast<uint8_t *>(in.position()), doc_count - 1, &embedded_postings[1], last_doc_id);
                in.position() = reinterpret_cast<char *>(packed_buffer_end);
            }
            else
            {
                chassert(bytes <= 512);
                uint8_t * packed_buffer = stream->packed_buffer;
                in.readStrict(reinterpret_cast<char *>(packed_buffer), bytes);
                uint8_t * packed_buffer_end = turbopfor::p4D1Dec32(packed_buffer, doc_count - 1, &embedded_postings[1], last_doc_id);
                chassert(packed_buffer_end - packed_buffer == bytes);
            }
        }

        if (stream->merged_part_offsets)
            stream->merged_part_offsets->mapOffsets(stream->part_index, embedded_postings, doc_count);

        return;
    }

    UInt32 num_large_blocks;
    VarInt::readUInt32(num_large_blocks, in);

    chassert(num_large_blocks >= 1);

    UInt32 remaining_docs = doc_count - 1;
    const UInt32 docs_per_large_block = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);

    LargePostingBlockMetas large_posting_blocks;
    large_posting_blocks.reserve(num_large_blocks);

    for (UInt32 i = 0; i < num_large_blocks; ++i)
    {
        UInt32 docs_in_large_block = std::min(remaining_docs, docs_per_large_block);
        UInt32 id;
        UInt64 offset;
        VarInt::readUInt32(id, in);
        readVarUInt(offset, in);

        UInt64 index_offset = 0;
        if (postingListFormatHasBlockIndex(format_version))
            readVarUInt(index_offset, in);

        large_posting_blocks.emplace_back(id, docs_in_large_block, offset, index_offset);
        remaining_docs -= docs_in_large_block;
    }

    lazy_posting_stream = std::make_unique<LazyPostingStream>(
        nullptr, 0, ReaderStreamVector{stream, last_doc_id, doc_count, std::move(large_posting_blocks)});
    lazy_posting_stream->streams.format_version = format_version;
}

void PostingListStream::write(WriteBuffer & wb, LargePostingListWriterStream & stream, const MergeTreeIndexTextParams & index_params) const
{
    VarInt::writeUInt32(doc_count, wb);
    if (doc_count == 0)
        return;

    /// --------------------------------------------
    /// Small posting list
    /// --------------------------------------------
    if (doc_count == 1)
    {
        VarInt::writeUInt32(embedded_postings[0], wb);
        return;
    }

    UInt32 * doc_delta_buffer = stream.doc_buffer;
    uint8_t * packed_buffer = stream.packed_buffer;

    if (doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
    {
        VarInt::writeUInt32(embedded_postings[0], wb);
        for (UInt32 i = 1; i < doc_count; ++i)
            doc_delta_buffer[i - 1] = embedded_postings[i] - embedded_postings[i - 1] - 1;
        uint8_t * end = turbopfor::p4Enc32(doc_delta_buffer, doc_count - 1, packed_buffer);
        UInt32 len = static_cast<UInt32>(end - packed_buffer);
        VarInt::writeUInt32(len, wb);
        wb.write(reinterpret_cast<const char *>(packed_buffer), len);
        return;
    }

    /// --------------------------------------------
    /// Large posting list
    /// --------------------------------------------

    chassert(lazy_posting_stream);

    std::vector<ReaderStreamCursor> cursors;
    if (!lazy_posting_stream->merged_embedded_postings.empty())
        cursors.emplace_back(lazy_posting_stream->merged_embedded_postings.data(), lazy_posting_stream->merged_embedded_postings.size());
    for (const auto & lazy_stream : lazy_posting_stream->streams)
    {
        chassert(!lazy_stream.large_posting_blocks.empty());
        const auto & blocks = lazy_stream.large_posting_blocks;

        lazy_stream.stream->seek(blocks.front().offset);

        cursors.emplace_back(
            lazy_stream.stream.get(),
            lazy_stream.first_doc_id,
            lazy_stream.doc_count - 1, /* remaining doc count (first doc is inline) */
            static_cast<UInt64>(lazy_stream.stream->getPosition()),
            false,
            true);

        /// In V2 format, Index Sections are interleaved between Data Sections.
        /// Set up skip schedule so the cursor seeks past each Index Section
        /// at large block boundaries.
        if (postingListFormatHasBlockIndex(lazy_posting_stream->streams.format_version))
            cursors.back().setLargeBlockSkips(blocks);
    }

    UInt32 last_doc_id;

    /// Align to TURBOPFOR_BLOCK_SIZE-doc blocks
    const UInt32 docs_per_large_block = (static_cast<UInt32>(index_params.posting_list_block_size) + TURBOPFOR_BLOCK_SIZE - 1) & ~(TURBOPFOR_BLOCK_SIZE - 1);
    const UInt32 large_doc_count = doc_count - 1;
    const UInt32 num_large_blocks = (large_doc_count + docs_per_large_block - 1) / docs_per_large_block;
    LargePostingBlockWriter block_writer(wb, stream.plain_hashing, docs_per_large_block,
        postingListFormatHasBlockIndex(resolvePostingListFormatVersion(index_params.posting_list_version)));

    UInt32 buffered = 0;
    auto flush128 = [&]()
    {
        uint8_t * end = turbopfor::p4Enc128v32(doc_delta_buffer, TURBOPFOR_BLOCK_SIZE, packed_buffer);
        block_writer.addBlock(last_doc_id, reinterpret_cast<const char *>(packed_buffer), static_cast<UInt32>(end - packed_buffer));
        buffered = 0;
    };

    auto flush_tail = [&]()
    {
        if (buffered == 0)
            return;

        uint8_t * end = turbopfor::p4Enc32(doc_delta_buffer, buffered, packed_buffer);
        block_writer.addBlock(last_doc_id, reinterpret_cast<const char *>(packed_buffer), static_cast<UInt32>(end - packed_buffer));
        buffered = 0;
    };

    mergePostingCursors(
        cursors,
        [&](UInt32 first_doc_id)
        {
            last_doc_id = first_doc_id;
            VarInt::writeUInt32(first_doc_id, wb);
            VarInt::writeUInt32(num_large_blocks, wb);
        },
        [&](UInt32 doc_id)
        {
            chassert(doc_id > last_doc_id);
            doc_delta_buffer[buffered++] = doc_id - last_doc_id - 1;
            last_doc_id = doc_id;
            if (buffered == TURBOPFOR_BLOCK_SIZE)
                flush128();
        });

    flush_tail();
    block_writer.finish(num_large_blocks);
}

void PostingListStream::collect(UInt32 * buf) const
{
    if (doc_count == 0)
        return;

    /// --------------------------------------------
    /// Small posting list
    /// --------------------------------------------
    if (doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
    {
        memcpy(buf, embedded_postings, doc_count * sizeof(UInt32));
        return;
    }

    /// --------------------------------------------
    /// Large posting list
    /// --------------------------------------------

    chassert(lazy_posting_stream);

    std::vector<ReaderStreamCursor> cursors;
    if (!lazy_posting_stream->merged_embedded_postings.empty())
        cursors.emplace_back(lazy_posting_stream->merged_embedded_postings.data(), lazy_posting_stream->merged_embedded_postings.size());
    for (const auto & lazy_stream : lazy_posting_stream->streams)
    {
        chassert(!lazy_stream.large_posting_blocks.empty());
        const auto & blocks = lazy_stream.large_posting_blocks;

        lazy_stream.stream->seek(blocks.front().offset);

        cursors.emplace_back(
            lazy_stream.stream.get(),
            lazy_stream.first_doc_id,
            lazy_stream.doc_count - 1,
            static_cast<UInt64>(lazy_stream.stream->getPosition()),
            false,
            true);

        if (postingListFormatHasBlockIndex(lazy_posting_stream->streams.format_version))
            cursors.back().setLargeBlockSkips(blocks);
    }

    UInt32 buffered = 0;
    auto emit = [&](UInt32 doc_id) { buf[buffered++] = doc_id; };
    mergePostingCursors(cursors, emit, emit);
}

void PostingListStream::merge(const PostingListStream & other)
{
    if (other.doc_count == 0)
        return;

    // -----------------------------
    // Case 0: this is empty, take other
    // -----------------------------
    if (doc_count == 0)
    {
        /// TODO(amos): check if this const cast is safe
        *this = PostingListStream(std::move(const_cast<PostingListStream &>(other)));
        return;
    }

    const bool lhs_embedded = doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS;
    const bool rhs_embedded = other.doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS;

    /// TODO(amos): if const cast is not valid, use the following code
    // if (doc_count == 0)
    // {
    //     if (rhs_embedded)
    //     {
    //         std::copy(other.embedded_postings, other.embedded_postings + other.doc_count, embedded_postings);
    //     }
    //     else
    //     {
    //         lazy_posting_stream = std::make_unique<LazyPostingStream>(
    //             other.lazy_posting_stream->merged_embedded_postings.data(),
    //             static_cast<UInt32>(other.lazy_posting_stream->merged_embedded_postings.size()),
    //             other.lazy_posting_stream->streams);
    //     }
    //     doc_count = other.doc_count;
    //     return;
    // }

    // -----------------------------
    // Case 1: both embedded
    // -----------------------------
    if (lhs_embedded && rhs_embedded)
    {
        chassert(!lazy_posting_stream);
        chassert(!other.lazy_posting_stream);

        UInt32 total_count = doc_count + other.doc_count;

        if (total_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
        {
            // in-place merge
            std::copy(other.embedded_postings, other.embedded_postings + other.doc_count, embedded_postings + doc_count);
            std::inplace_merge(embedded_postings, embedded_postings + doc_count, embedded_postings + total_count);
            doc_count = total_count;
            return;
        }

        lazy_posting_stream = std::make_unique<LazyPostingStream>();
        lazy_posting_stream->merged_embedded_postings.reserve(total_count);

        std::merge(
            embedded_postings,
            embedded_postings + doc_count,
            other.embedded_postings,
            other.embedded_postings + other.doc_count,
            std::back_inserter(lazy_posting_stream->merged_embedded_postings));

        doc_count = total_count;
        return;
    }

    // -----------------------------
    // Case 2: at least one side is lazy
    // -----------------------------
    if (lhs_embedded)
    {
        chassert(!lazy_posting_stream);
        lazy_posting_stream = std::make_unique<LazyPostingStream>(embedded_postings, doc_count);
    }
    else
    {
        chassert(lazy_posting_stream);
    }

    UInt32 sz = static_cast<UInt32>(lazy_posting_stream->merged_embedded_postings.size());
    if (rhs_embedded)
    {
        chassert(!other.lazy_posting_stream);
        lazy_posting_stream->merged_embedded_postings.insert(
            lazy_posting_stream->merged_embedded_postings.end(), other.embedded_postings, other.embedded_postings + other.doc_count);
    }
    else
    {
        chassert(other.lazy_posting_stream);
        lazy_posting_stream->merged_embedded_postings.insert(
            lazy_posting_stream->merged_embedded_postings.end(),
            other.lazy_posting_stream->merged_embedded_postings.begin(),
            other.lazy_posting_stream->merged_embedded_postings.end());
    }

    // in-place merge (assume both halves sorted, no duplicates)
    const auto mid = lazy_posting_stream->merged_embedded_postings.begin() + sz;
    std::inplace_merge(lazy_posting_stream->merged_embedded_postings.begin(), mid, lazy_posting_stream->merged_embedded_postings.end());

    // merge streams
    if (other.lazy_posting_stream)
        lazy_posting_stream->streams.merge(other.lazy_posting_stream->streams);

    doc_count += other.doc_count;

    chassert(doc_count > MAX_SIZE_OF_EMBEDDED_POSTINGS);
}

// void PostingListStream::merge(const PostingListWriter & other, Arena * arena)
// {
//     if (other.doc_count == 0)
//         return;

//     UInt32 total_count = doc_count + other.doc_count;
//     if (total_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
//     {
//         embedded_postings[doc_count] = other.first_doc_id;
//         for (size_t i = 1; i < other.doc_count; ++i)
//             embedded_postings[doc_count + i] = embedded_postings[doc_count + i - 1] + other.doc_delta_buffer[i - 1] + 1;
//         std::inplace_merge(embedded_postings, embedded_postings + doc_count, embedded_postings + total_count);
//         doc_count = total_count;
//         return;
//     }

//     if (!lazy_posting_stream)
//         lazy_posting_stream = std::make_unique<LazyPostingStream>();

//     lazy_posting_stream->streams.entries.emplace_back(other.blocks_head, other.doc_delta_buffer, other.first_doc_id, other.doc_count);
//     doc_count += other.doc_count;
//     chassert(doc_count > MAX_SIZE_OF_EMBEDDED_POSTINGS);
// }

}
