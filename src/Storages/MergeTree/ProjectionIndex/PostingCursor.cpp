#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>

#include <Storages/MergeTree/ProjectionIndex/PostingListCursor.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

inline bool isAllZero(const UInt8 * data, size_t count)
{
    size_t i = 0;
    for (; i + 8 <= count; i += 8)
    {
        uint64_t w = 0;
        memcpy(&w, data + i, 8);
        if (w)
            return false;
    }
    for (; i < count; ++i)
    {
        if (data[i])
            return false;
    }
    return true;
}

/// Fused AND + count: dst[i] &= src[i], returns count of nonzero bytes in dst after AND.
inline size_t bitwiseAndCount(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t count)
{
    size_t result = 0;
    size_t j = 0;
    for (; j + 8 <= count; j += 8)
    {
        uint64_t d = 0;
        uint64_t s = 0;
        memcpy(&d, dst + j, 8);
        memcpy(&s, src + j, 8);
        d &= s;
        memcpy(dst + j, &d, 8);
        result += (d * 0x0101010101010101ULL) >> 56;
    }
    for (; j < count; ++j)
    {
        dst[j] &= src[j];
        result += dst[j];
    }
    return result;
}

inline size_t countNonZero(const UInt8 * data, size_t count)
{
    size_t result = 0;
    size_t i = 0;
    for (; i + 8 <= count; i += 8)
    {
        uint64_t w = 0;
        memcpy(&w, data + i, 8);
        result += (w * 0x0101010101010101ULL) >> 56;
    }
    for (; i < count; ++i)
        result += data[i];
    return result;
}

} // anonymous namespace
OrCursor::OrCursor(std::vector<PostingCursorPtr> children_)
    : children(std::move(children_))
{
}

void OrCursor::fill(UInt8 * out, size_t row_offset, size_t num_rows)
{
    for (auto & child : children)
        child->fill(out, row_offset, num_rows);
}
AndCursor::AndCursor(std::vector<PostingCursorPtr> children_)
    : children(std::move(children_))
{
    all_children_are_leaf = true;
    for (auto & child : children)
    {
        if (!dynamic_cast<ProjectionPostingListCursor *>(child.get()))
        {
            all_children_are_leaf = false;
            break;
        }
    }
}

/// Multi-token AND intersection strategy.
///
/// Children are sorted by cardinality ascending (sparsest first).
///
/// All-leaf path (all children are ProjectionPostingListCursor):
///   The sparsest child batch-collects doc_ids via collectDocIds (TurboPFor
///   block decode into a vector), then seek-verifies against each remaining
///   child. No bitmap materialization needed — O(driver_cardinality) seeks.
///
/// Mixed path (some children are compound cursors):
///   1. fillSeekPath (moderate result set after first fill):
///      First child fills the bitmap, then collect set doc_ids and seek-verify
///      remaining children. Switches from bitmap path when set_count drops.
///
///   2. fillBitmapPath (dense cursors):
///      Each child fills a UInt8[num_rows] bitmap independently, then bitwiseAnd.
void AndCursor::fill(UInt8 * out, size_t row_offset, size_t num_rows)
{
    if (children.empty())
        return;

    if (children.size() == 1)
    {
        children[0]->fill(out, row_offset, num_rows);
        return;
    }

    if (all_children_are_leaf)
    {
        auto * driver = static_cast<ProjectionPostingListCursor *>(children[0].get());
        double estimated_matches = static_cast<double>(num_rows) * driver->density();

        if (estimated_matches < static_cast<double>(num_rows) / 8)
        {
            doc_ids_buf.clear();
            driver->collectDocIds(doc_ids_buf, row_offset, num_rows);

            if (doc_ids_buf.empty())
                return;

            for (size_t i = 1; i < children.size(); ++i)
            {
                auto * leaf = static_cast<ProjectionPostingListCursor *>(children[i].get());
                size_t write_pos = 0;
                size_t dense_misses = 0;
                uint32_t first_miss = 0;
                uint32_t first_miss_value = 0;
                bool first_miss_valid_flag = false;
                const bool is_dense = leaf->density() >= 1.0;
                const uint32_t leaf_first = leaf->firstDocId();
                const uint32_t leaf_last = leaf->lastDocId();
                for (uint32_t doc_id : doc_ids_buf)
                {
                    leaf->seek(doc_id);
                    bool hit = leaf->valid() && leaf->value() == doc_id;
                    if (hit)
                        doc_ids_buf[write_pos++] = doc_id;
                    else if (is_dense && doc_id >= leaf_first && doc_id <= leaf_last)
                    {
                        if (dense_misses == 0)
                        {
                            first_miss = doc_id;
                            first_miss_valid_flag = leaf->valid();
                            first_miss_value = first_miss_valid_flag ? leaf->value() : 0;
                        }
                        ++dense_misses;
                    }
                }
                doc_ids_buf.resize(write_pos);
                if (dense_misses > 0)
                    LOG_TRACE(getLogger("AndCursor"),
                        "all-leaf dense child invariant violation: child={} row_offset={} num_rows={} "
                        "leaf_first={} leaf_last={} leaf_cardinality={} dense_misses={} first_miss_target={} first_miss_valid={} first_miss_value={}",
                        i, row_offset, num_rows,
                        leaf_first, leaf_last, leaf->cardinality(),
                        dense_misses, first_miss, first_miss_valid_flag, first_miss_value);
                if (doc_ids_buf.empty())
                    return;
            }

            for (uint32_t doc_id : doc_ids_buf)
                out[doc_id - row_offset] = 1;
            return;
        }
    }

    children[0]->fill(out, row_offset, num_rows);

    size_t set_count = countNonZero(out, num_rows);
    if (set_count == 0)
        return;

    size_t dynamic_threshold = std::max(SEEK_THRESHOLD, num_rows / 8);
    if (set_count <= dynamic_threshold)
    {
        fillSeekPath(out, row_offset, num_rows, set_count, 1);
        return;
    }

    fillBitmapPath(out, row_offset, num_rows, 1);
}

void AndCursor::fillBitmapPath(UInt8 * out, size_t row_offset, size_t num_rows, size_t start_child)
{
    if (tmp_buf.size() < num_rows)
        tmp_buf.resize(num_rows);

    static constexpr size_t CHUNK_SIZE = 8192;

    for (size_t i = start_child; i < children.size(); ++i)
    {
        size_t set_count = 0;
        for (size_t off = 0; off < num_rows; off += CHUNK_SIZE)
        {
            size_t chunk_len = std::min(CHUNK_SIZE, num_rows - off);

            if (isAllZero(out + off, chunk_len))
                continue;

            memset(tmp_buf.data() + off, 0, chunk_len);
            children[i]->fill(tmp_buf.data() + off, row_offset + off, chunk_len);
            set_count += bitwiseAndCount(out + off, tmp_buf.data() + off, chunk_len);
        }

        if (set_count == 0)
            return;

        if (set_count <= std::max(SEEK_THRESHOLD, num_rows / 8) && i + 1 < children.size())
        {
            fillSeekPath(out, row_offset, num_rows, set_count, i + 1);
            return;
        }
    }
}

void AndCursor::fillSeekPath(UInt8 * out, size_t row_offset, size_t num_rows, size_t set_count, size_t start_child)
{
    doc_ids_buf.clear();
    doc_ids_buf.reserve(set_count);
    for (size_t j = 0; j < num_rows; ++j)
    {
        if (out[j])
            doc_ids_buf.push_back(static_cast<uint32_t>(row_offset + j));
    }

    for (size_t i = start_child; i < children.size(); ++i)
    {
        auto * leaf = dynamic_cast<ProjectionPostingListCursor *>(children[i].get());
        if (!leaf)
        {
            memset(out, 0, num_rows);
            for (uint32_t doc_id : doc_ids_buf)
                out[doc_id - row_offset] = 1;
            fillBitmapPath(out, row_offset, num_rows, i);
            return;
        }

        size_t write_pos = 0;
        size_t dense_misses = 0;
        uint32_t first_miss = 0;
        uint32_t first_miss_value = 0;
        bool first_miss_valid_flag = false;
        const bool is_dense = leaf->density() >= 1.0;
        const uint32_t leaf_first = leaf->firstDocId();
        const uint32_t leaf_last = leaf->lastDocId();
        for (unsigned int k : doc_ids_buf)
        {
            leaf->seek(k);
            bool hit = leaf->valid() && leaf->value() == k;
            if (hit)
                doc_ids_buf[write_pos++] = k;
            else if (is_dense && k >= leaf_first && k <= leaf_last)
            {
                if (dense_misses == 0)
                {
                    first_miss = k;
                    first_miss_valid_flag = leaf->valid();
                    first_miss_value = first_miss_valid_flag ? leaf->value() : 0;
                }
                ++dense_misses;
            }
        }
        doc_ids_buf.resize(write_pos);

        if (dense_misses > 0)
            LOG_TRACE(getLogger("AndCursor"),
                "fillSeekPath dense child invariant violation: child={} row_offset={} num_rows={} set_count={} "
                "leaf_first={} leaf_last={} leaf_cardinality={} dense_misses={} first_miss_target={} first_miss_valid={} first_miss_value={}",
                i, row_offset, num_rows, set_count,
                leaf_first, leaf_last, leaf->cardinality(),
                dense_misses, first_miss, first_miss_valid_flag, first_miss_value);

        if (doc_ids_buf.empty())
        {
            memset(out, 0, num_rows);
            return;
        }
    }

    memset(out, 0, num_rows);
    for (uint32_t doc_id : doc_ids_buf)
        out[doc_id - row_offset] = 1;
}

}
