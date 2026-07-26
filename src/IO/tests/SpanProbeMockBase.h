#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ResidencyIterator.h>

namespace DB::tests
{

/// Adapts a span-probe MOCK to the step interface: the mock keeps its whole
/// residency logic in `buildProbeView` (sorted hits + one-cell misses tiling
/// a requested span) and
/// this base steps a memoized chunk of it through `lookAt`. Test-only - the
/// real providers run native cursors.
class SpanProbeMockBase : public ICacheProvider
{
public:
    std::unique_ptr<IProbeCursor> probe() override
    {
        return std::make_unique<Cursor>(*this);
    }

protected:
    /// The mock's residency logic, in the span-probe shape: sorted disjoint
    /// hit/miss entries tiling the (object-clamped) request, one cell per miss.
    virtual CacheViewPtr buildProbeView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) = 0;

private:
    class Cursor : public IProbeCursor
    {
    public:
        explicit Cursor(SpanProbeMockBase & mock_) : mock(mock_) {}

        Resolution lookAt(const StoredObject & object, size_t object_file_offset, size_t pos_in_file) override
        {
        static constexpr size_t PROBE_CHUNK = 8 * 1024 * 1024;

        const size_t object_end_in_file = object_file_offset + object.bytes_size;
        if (pos_in_file >= object_end_in_file)
            return {};

        const bool memo_valid = memo
            && memo->object_path == object.remote_path
            && memo->object_file_offset == object_file_offset
            && pos_in_file >= memo->span.offset
            && pos_in_file < memo->span.end();
        if (!memo_valid)
        {
            ProbeMemo m;
            m.object_path = object.remote_path;
            m.object_file_offset = object_file_offset;
            const size_t chunk_end = std::min(pos_in_file + PROBE_CHUNK, object_end_in_file);
            m.view = mock.buildProbeView(object, object_file_offset, ByteRange{pos_in_file, chunk_end - pos_in_file});
            size_t covered_end = chunk_end;
            if (!m.view->hits().empty())
                covered_end = std::max(covered_end, m.view->hits().back().range.end());
            if (!m.view->misses().empty())
                covered_end = std::max(covered_end, m.view->misses().back().range.end());
            m.span = ByteRange{pos_in_file, covered_end - pos_in_file};
            memo = std::move(m);
        }

        auto & hits = memo->view->hit_entries;
        const auto & misses = memo->view->misses();
        if (memo->hit_idx > 0 && memo->hit_idx <= hits.size()
            && hits[memo->hit_idx - 1].range.end() > pos_in_file)
            memo->hit_idx = 0;
        if (memo->miss_idx > 0 && memo->miss_idx <= misses.size()
            && misses[memo->miss_idx - 1].range.end() > pos_in_file)
            memo->miss_idx = 0;
        while (memo->hit_idx < hits.size() && hits[memo->hit_idx].range.end() <= pos_in_file)
            ++memo->hit_idx;
        while (memo->miss_idx < misses.size() && misses[memo->miss_idx].range.end() <= pos_in_file)
            ++memo->miss_idx;

        Resolution res;
        if (memo->hit_idx < hits.size() && hits[memo->hit_idx].range.offset <= pos_in_file)
        {
            res.kind = Resolution::Kind::Hit;
            res.range = hits[memo->hit_idx].range;
            res.reader = std::move(hits[memo->hit_idx].reader);
            return res;
        }
        if (memo->miss_idx < misses.size() && misses[memo->miss_idx].range.offset <= pos_in_file)
        {
            res.kind = Resolution::Kind::Miss;
            res.range = misses[memo->miss_idx].range;
            return res;
        }
        return res;
    }

    private:
        SpanProbeMockBase & mock;

        struct ProbeMemo
    {
        String object_path;
        size_t object_file_offset = 0;
        ByteRange span{};
        CacheViewPtr view;
        size_t hit_idx = 0;
        size_t miss_idx = 0;
    };
        std::optional<ProbeMemo> memo;
    };
};

}
