#include <Storages/MergeTree/ProjectionIndex/ProjectionTokenInfo.h>

#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <abpfor.h>

#include <tuple>

namespace ProfileEvents
{
    extern const Event TextIndexReadPostings;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

ProjectionTokenInfoPtr ProjectionTokenInfo::buildFromPostingStream(
    const PostingListStream & stream,
    LargePostingListReaderStream * pidx_stream,
    bool phrase_enabled)
{
    auto info = std::make_shared<ProjectionTokenInfo>();
    info->cardinality = stream.doc_count;
    info->first_doc_freq = stream.first_doc_freq;
    if (stream.lazy && !stream.lazy->streams.entries.empty())
        info->first_doc_pos_offset = stream.lazy->streams.entries.front().first_doc_pos_offset;

    if (stream.doc_count == 0)
    {
        return nullptr;
    }
    else if (stream.lazy)
    {
        chassert(stream.lazy->streams.size() == 1);
        const auto & entry = stream.lazy->streams.entries.front();
        size_t num_large_blocks = entry.large_posting_blocks.size();

        if (num_large_blocks == 0)
        {
            info->ranges.emplace_back(entry.first_doc_id, entry.first_doc_id);
        }
        else
        {
            info->large_block_metas.reserve(num_large_blocks);
            info->ranges.reserve(num_large_blocks);
            for (size_t b = 0; b < num_large_blocks; ++b)
            {
                info->large_block_metas.push_back(entry.large_posting_blocks[b]);
                UInt32 range_begin = (b == 0) ? entry.first_doc_id : entry.large_posting_blocks[b].first_doc_id;
                info->ranges.emplace_back(range_begin, entry.large_posting_blocks[b].last_doc_id);
            }

            if (pidx_stream)
            {
                info->block_index_data.resize(num_large_blocks);
                for (size_t b = 0; b < num_large_blocks; ++b)
                    info->block_index_data[b] = LargeBlockData::decodeFromIndex(
                        *pidx_stream, entry.large_posting_blocks[b], phrase_enabled);
            }
        }
    }
    else if (stream.doc_count == 1)
    {
        info->ranges.emplace_back(stream.first_doc_id, stream.first_doc_id);
    }

    if (info->cardinality == 0)
        return nullptr;

    return info;
}

bool ProjectionTokenInfo::hasDocInRange(
    const RowsRange & current_range,
    UInt32 range_begin,
    UInt32 range_end,
    std::unique_ptr<DecodedBlockCache> & cache,
    LargePostingListReaderStream * pst_stream) const
{
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (!current_range.intersects(ranges[i]))
            continue;

        if (i == 0)
        {
            UInt32 first_doc_id = static_cast<UInt32>(ranges[0].begin);
            if (first_doc_id >= range_begin && first_doc_id <= range_end)
                return true;
        }

        if (i >= block_index_data.size() || !block_index_data[i])
            return true;

        const auto & lb = *block_index_data[i];
        if (lb.packed_block_ranges.empty())
            return true;

        size_t num_blocks = lb.numPackedBlocks();
        size_t lo = 0;
        size_t hi = num_blocks;
        while (lo < hi)
        {
            size_t mid = lo + (hi - lo) / 2;
            if (lb.lastDocIdOf(mid) < range_begin)
                lo = mid + 1;
            else
                hi = mid;
        }
        if (lo >= num_blocks)
            continue;
        if (lb.firstDocIdOf(lo) > range_end)
            continue;
        if (lb.firstDocIdOf(lo) >= range_begin || lb.lastDocIdOf(lo) <= range_end)
            return true;

        /// Need precise decode — check contiguous fast-path first.
        UInt32 count_meta = (lo + 1 == lb.block_count && lb.tail_size > 0)
            ? static_cast<UInt32>(lb.tail_size)
            : static_cast<UInt32>(ABPFOR_BLOCK_SIZE);
        uint32_t delta_base_meta = 0;
        if (lo == 0)
        {
            if (i > 0)
                delta_base_meta = static_cast<uint32_t>(ranges[i - 1].end);
            else
                delta_base_meta = static_cast<uint32_t>(ranges[0].begin);
        }
        else
        {
            delta_base_meta = lb.lastDocIdOf(lo - 1);
        }
        if (lb.lastDocIdOf(lo) - delta_base_meta == count_meta)
        {
            UInt32 first = delta_base_meta + 1;
            UInt32 last = lb.lastDocIdOf(lo);
            if (first <= range_end && last >= range_begin)
                return true;
            continue;
        }

        const auto * cached = cache ? cache->get(&lb, lo) : nullptr;
        if (!cached && pst_stream)
        {
            UInt32 blk_offset = (lo == 0) ? 0 : lb.packed_block_cum_bytes[lo - 1];
            UInt32 bytes = lb.packed_block_cum_bytes[lo] - blk_offset;
            UInt32 count = count_meta;
            uint32_t delta_base = delta_base_meta;

            auto entry = std::make_unique<DecodedBlockCache::Entry>();
            entry->count = count;

            pst_stream->seek(lb.data_section_start + blk_offset);
            auto & dbuf = pst_stream->decodeBuffer();
            dbuf.reset();
            const uint8_t * ptr = dbuf.ptr();
            /// Deliberately discard the decoded doc-block length: `bytes` spans doc + freq
            /// (see `packed_block_cum_bytes`), and the buffer must advance past both so the
            /// next block starts at the right offset.
            if (count == ABPFOR_BLOCK_SIZE)
                std::ignore = abpfor::b256::decodeBlockDelta1(ptr, entry->doc_ids, delta_base);
            else
                std::ignore = abpfor::b256::decodeTailDelta1(ptr, count, entry->doc_ids, delta_base);
            dbuf.advance(bytes);

            /// Validate the decoded doc ids: they must be strictly monotonically increasing and
            /// within `[delta_base + 1, lb.lastDocIdOf(lo)]`. A corrupted `.pidx` can point this
            /// decode at the wrong bytes, and the `lower_bound` below would then silently miss a
            /// real hit and prune a mark that does contain the token. Report `INCORRECT_DATA`
            /// rather than returning wrong query results. Mirrors the same check in
            /// `ProjectionPostingListCursor::ensureBlockDecoded`.
            if (count > 1)
            {
                bool monotonic = true;
                for (UInt32 vi = 1; vi < count && monotonic; ++vi)
                    monotonic = (entry->doc_ids[vi] > entry->doc_ids[vi - 1]);
                if (!monotonic || entry->doc_ids[0] <= delta_base || entry->doc_ids[count - 1] > lb.lastDocIdOf(lo))
                {
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Corrupted projection text index: decoded posting block has invalid doc ids "
                        "(block_idx={} count={} delta_base={} expected_max={} first={} last={} monotonic={})",
                        lo, count, delta_base, lb.lastDocIdOf(lo), entry->doc_ids[0], entry->doc_ids[count - 1], monotonic);
                }
            }

            if (!cache)
                cache = std::make_unique<DecodedBlockCache>();
            cached = cache->put(&lb, lo, std::move(entry));
            ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
        }

        if (!cached)
            return true;

        const auto * begin_ptr = cached->doc_ids;
        const auto * end_ptr = begin_ptr + cached->count;
        const auto * it = std::lower_bound(begin_ptr, end_ptr, range_begin);
        if (it != end_ptr && *it <= range_end)
            return true;
    }
    return false;
}

size_t ProjectionTokenInfo::bytesAllocated() const
{
    size_t result = sizeof(ProjectionTokenInfo)
        + large_block_metas.size() * sizeof(LargePostingBlockMeta)
        + ranges.size() * sizeof(RowsRange);

    for (const auto & lb : block_index_data)
    {
        if (!lb)
            continue;
        result += sizeof(LargeBlockData)
            + lb->packed_block_ranges.capacity() * sizeof(UInt32)
            + lb->packed_block_cum_bytes.capacity() * sizeof(UInt32)
            + lb->pos_cum_deltas.capacity() * sizeof(UInt64)
            + lb->pos_cum_bytes.capacity() * sizeof(UInt64);
    }

    return result;
}

}
