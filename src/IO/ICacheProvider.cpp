#include <IO/ICacheProvider.h>

namespace DB
{

ICacheProvider::Resolution ICacheProvider::lookAt(
    const StoredObject & object, size_t object_file_offset, size_t pos_in_file)
{
    /// One probe covers many steps: 8 MiB matches the largest fill cell, so a
    /// cell is always derived whole within one chunk.
    static constexpr size_t PROBE_CHUNK = 8 * 1024 * 1024;

    const size_t object_end_in_file = object_file_offset + object.bytes_size;
    if (pos_in_file >= object_end_in_file)
        return {};

    const bool memo_valid = probe_memo
        && probe_memo->object_path == object.remote_path
        && probe_memo->object_file_offset == object_file_offset
        && pos_in_file >= probe_memo->span.offset
        && pos_in_file < probe_memo->span.end();
    if (!memo_valid)
    {
        ProbeMemo memo;
        memo.object_path = object.remote_path;
        memo.object_file_offset = object_file_offset;
        const size_t chunk_end = std::min(pos_in_file + PROBE_CHUNK, object_end_in_file);
        memo.view = planResidencyView(object, object_file_offset, ByteRange{pos_in_file, chunk_end - pos_in_file});
        /// The probed entries tile the chunk and the miss cells may overhang it
        /// (object-end-clamped only); the memo's span is what the entries
        /// actually cover, so the next chunk starts past any straddling cell
        /// and a chunk edge never splits one.
        size_t covered_end = chunk_end;
        if (!memo.view->hits().empty())
            covered_end = std::max(covered_end, memo.view->hits().back().range.end());
        if (!memo.view->misses().empty())
            covered_end = std::max(covered_end, memo.view->misses().back().range.end());
        memo.span = ByteRange{pos_in_file, covered_end - pos_in_file};
        probe_memo = std::move(memo);
    }

    auto & memo = *probe_memo;
    auto & hits = memo.view->hit_entries;
    const auto & misses = memo.view->misses();
    /// A backward re-ask inside the memo rewinds the cursors; forward steps
    /// advance them monotonically.
    if (memo.hit_idx > 0 && memo.hit_idx <= hits.size()
        && hits[memo.hit_idx - 1].range.end() > pos_in_file)
        memo.hit_idx = 0;
    if (memo.miss_idx > 0 && memo.miss_idx <= misses.size()
        && misses[memo.miss_idx - 1].range.end() > pos_in_file)
        memo.miss_idx = 0;
    while (memo.hit_idx < hits.size() && hits[memo.hit_idx].range.end() <= pos_in_file)
        ++memo.hit_idx;
    while (memo.miss_idx < misses.size() && misses[memo.miss_idx].range.end() <= pos_in_file)
        ++memo.miss_idx;

    Resolution res;
    if (memo.hit_idx < hits.size() && hits[memo.hit_idx].range.offset <= pos_in_file)
    {
        res.kind = Resolution::Kind::Hit;
        res.range = hits[memo.hit_idx].range;
        res.reader = std::move(hits[memo.hit_idx].reader);
        return res;
    }
    if (memo.miss_idx < misses.size() && misses[memo.miss_idx].range.offset <= pos_in_file)
    {
        res.kind = Resolution::Kind::Miss;
        res.range = misses[memo.miss_idx].range;
        return res;
    }
    return res;
}

}
