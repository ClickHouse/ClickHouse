/// Equivalence harness for the residency iterator: an independent
/// `ResidencyIterator` walk folded by `ResolutionFold` must reproduce EXACTLY
/// the geometry `observeAndSchedule` publishes - same resident runs, same
/// surviving miss cells, same prune - over real providers (FileCache tiling,
/// partial segments, page blocks) and randomized layouts. The harness walks
/// first (read-only), the executor observes the same untouched cache state
/// after, and the two are compared entry by entry - the regression net for
/// the probe cursors, the fold, and the plan build.

#include <IO/ResidencyIterator.h>

#include <IO/ChainedBuffers.h>
#include <IO/DiskCacheProvider.h>
#include <IO/ICacheProvider.h>
#include <IO/IFileBasedSourceReader.h>
#include <IO/LongConnectionLimit.h>
#include <IO/PageCacheProvider.h>
#include <IO/PrefetchThreadPool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReaderExecutor.h>
#include <IO/tests/ReaderExecutorInspector.h>

#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/ThreadStatus.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/tests/gtest_global_context.h>

#include <Core/Defines.h>
#include <Core/ServerUUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheSettings.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <unordered_map>

namespace fs = std::filesystem;

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsBool load_metadata_asynchronously;
    extern const FileCacheSettingsFileCachePolicy cache_policy;
}

using namespace DB;

namespace
{

/// Compressed production geometry (same scheme as the metric harness): all
/// size ratios preserved so the tiling/prune shapes match production.
constexpr size_t SEGMENT = 32 * 1024;
constexpr size_t ALIGNMENT = 4 * 1024;
constexpr size_t WINDOW = 8 * 1024;
constexpr size_t BLOCK = 1024;
constexpr size_t MIN_BYTES_FOR_SEEK = 2 * 1024;
constexpr size_t MAX_TAIL_FOR_DRAIN = 512;
constexpr size_t PLAN_WINDOW = 4 * WINDOW;
constexpr size_t FILE_SIZE = 32 * SEGMENT;

/// In-memory source honoring `setReadUntilPosition`, so the executor exercises
/// its production connection paths.
class MemBoundedBuffer : public ReadBufferFromFileBase
{
public:
    explicit MemBoundedBuffer(String data_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0), data(std::move(data_)) {}

    String getFileName() const override { return "MemBoundedBuffer"; }
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t p) override { read_until = p; }

    off_t seek(off_t off, int whence) override
    {
        if (whence == SEEK_SET)
            file_offset = static_cast<size_t>(off);
        else if (whence == SEEK_CUR)
            file_offset += static_cast<size_t>(off);
        resetWorkingBuffer();
        return static_cast<off_t>(file_offset);
    }

    off_t getPosition() override { return static_cast<off_t>(file_offset); }
    size_t getFileOffsetOfBufferEnd() const override { return file_offset; }

private:
    bool nextImpl() override
    {
        const size_t end = read_until ? std::min(*read_until, data.size()) : data.size();
        if (file_offset >= end)
            return false;
        const size_t n = std::min(end - file_offset, internal_buffer.size());
        memcpy(internal_buffer.begin(), data.data() + file_offset, n);
        working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + n);
        file_offset += n;
        return true;
    }

    String data;
    size_t file_offset = 0;
    std::optional<size_t> read_until;
};

class MemBoundedSource : public IFileBasedSourceReader
{
public:
    explicit MemBoundedSource(std::unordered_map<String, String> data_) : data(std::move(data_)) {}

    std::unique_ptr<ReadBufferFromFileBase> open(const StoredObject & object) override
    {
        auto it = data.find(object.remote_path);
        if (it == data.end())
            return nullptr;
        return std::make_unique<MemBoundedBuffer>(it->second);
    }

    String name() const override { return "MemBoundedSource"; }

private:
    std::unordered_map<String, String> data;
};

String makePattern(size_t size)
{
    String s;
    s.resize(size);
    for (size_t i = 0; i < size; ++i)
        s[i] = static_cast<char>('A' + (i % 26));
    return s;
}

/// A contiguous file-level chain carrying the true pattern bytes of
/// `[offset, offset + size)`.
ChainedBuffers patternChain(const String & content, size_t offset, size_t size)
{
    ChainedBuffers r;
    auto buf = std::make_shared<OwnedChainedBuffer>(size);
    std::memcpy(buf->data(), content.data() + offset, size);
    r.append(ChainedBufferNode{std::move(buf), 0, size, offset});
    return r;
}

class ResidencyEquivalence : public ::testing::Test
{
public:
    ResidencyEquivalence()
    {
        current_thread = nullptr;
        getContext();
    }
    ~ResidencyEquivalence() override { current_thread = MainThreadStatus::get(); }

    void SetUp() override
    {
        ServerUUID::setRandomForUnitTests();
        thread_status.emplace();

        Poco::XML::DOMParser dom_parser;
        std::string xml(R"CONFIG(<clickhouse></clickhouse>)CONFIG");
        Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
        getMutableContext().context->setConfig(config);

        query_context = Context::createCopy(getContext().context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId("residency_equivalence");
        query_scope_holder.emplace(QueryScope::create(query_context));

        cache_root = fs::current_path() / "residency_equivalence_cache";
        if (fs::exists(cache_root))
            fs::remove_all(cache_root);
        fs::create_directories(cache_root);

        content = makePattern(FILE_SIZE);
        data = {{"obj", content}};
        objects.clear();
        objects.emplace_back("obj", "", FILE_SIZE);
    }

    void TearDown() override
    {
        query_scope_holder.reset();
        query_context.reset();
        thread_status.reset();
        if (fs::exists(cache_root))
            fs::remove_all(cache_root);
    }

    std::shared_ptr<FileCache> makeFileCache(const String & name) const
    {
        FileCacheSettings settings;
        settings[FileCacheSetting::path] = (cache_root / name).string();
        settings[FileCacheSetting::max_size] = 64u << 20;
        settings[FileCacheSetting::max_elements] = 100000;
        settings[FileCacheSetting::max_file_segment_size] = SEGMENT;
        settings[FileCacheSetting::boundary_alignment] = ALIGNMENT;
        settings[FileCacheSetting::load_metadata_asynchronously] = false;
        settings[FileCacheSetting::cache_policy] = FileCachePolicy::LRU;

        auto fc = std::make_shared<FileCache>(name, settings);
        fc->initialize();
        return fc;
    }

    std::shared_ptr<DiskCacheProvider> makeDiskProvider(const std::shared_ptr<FileCache> & fc)
    {
        FilesystemCacheSettings cache_settings;
        cache_settings.reserve_space_wait_lock_timeout_milliseconds = 1000;
        cache_settings.boundary_alignment = ALIGNMENT;
        return std::make_shared<DiskCacheProvider>(fc, cache_settings, /*query_id_=*/"q");
    }

    static std::shared_ptr<PageCache> makePageCache()
    {
        constexpr size_t cap = 64ull << 20;
        return std::make_shared<PageCache>(
            std::chrono::milliseconds(2000), "LRU", 0.5,
            /*min_size_in_bytes=*/cap, /*max_size_in_bytes=*/cap,
            /*free_memory_ratio=*/0.0, /*num_shards=*/1);
    }

    static std::shared_ptr<PageCacheProvider> makePageProvider(const std::shared_ptr<PageCache> & pc)
    {
        PageCacheFile file;
        file.path = "obj";
        return std::make_shared<PageCacheProvider>(
            pc, std::move(file), BLOCK, /*inject_eviction=*/false,
            /*bypass_if_missing=*/false, FILE_SIZE);
    }

    ReaderExecutor::Options makeOptions() const
    {
        ReaderExecutor::Options options;
        options.window_size = WINDOW;
        options.min_bytes_for_seek = MIN_BYTES_FOR_SEEK;
        options.block_size = BLOCK;
        options.log_file_path = {};
        options.max_tail_for_drain = MAX_TAIL_FOR_DRAIN;
        options.long_connection_limit = std::make_shared<LongConnectionLimit>(0);
        options.plan_look_ahead_max_window = PLAN_WINDOW;
        return options;
    }

    /// Read `[offset, offset + want)` through a fresh executor over `caches`,
    /// populating them - the layout builder.
    void warmReads(
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches,
        const std::vector<std::pair<size_t, size_t>> & reads)
    {
        auto src = std::make_shared<MemBoundedSource>(data);
        ReaderExecutor executor(src, objects, std::move(caches), makeOptions());
        for (const auto & [offset, want] : reads)
        {
            if (static_cast<size_t>(executor.getPosition()) != offset)
                executor.seek(offset);
            const size_t end = offset + want;
            size_t got = 0;
            while (got < want)
            {
                executor.setReadExtent(std::min(end, executor.getPosition() + WINDOW));
                auto chain = executor.readNextWindow();
                if (chain.empty())
                    break;
                got += chain.range().size;
            }
            ASSERT_EQ(got, want);
        }
    }

    /// Leave a PARTIALLY_DOWNLOADED segment behind: open a writer over one
    /// explicit cell and commit only a prefix under a claim.
    void fabricatePartialCell(ICacheProvider & provider, ByteRange cell, size_t prefix)
    {
        auto view = std::make_unique<CacheView>();
        view->miss_entries.push_back(MissEntry{cell, nullptr});
        for (auto & e : view->miss_entries)
            e.writer = provider.openWriter(objects.front(), /*object_file_offset=*/0, e.range);
        ASSERT_EQ(view->misses().size(), 1u);
        auto & writer = *view->misses().front().writer;
        auto claim = writer.claim(writer.range());
        ASSERT_EQ(writer.write(patternChain(content, cell.offset, prefix)), prefix);
    }

    ByteRange expectedSpan(size_t start) const
    {
        const size_t end = std::min(start + std::max(WINDOW, PLAN_WINDOW), FILE_SIZE);
        return ByteRange{start, end - start};
    }

    static VectorWithMemoryTracking<ResolutionFold::TierTraits>
    traitsOf(const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & chain)
    {
        VectorWithMemoryTracking<ResolutionFold::TierTraits> traits;
        for (const auto & p : chain)
            traits.push_back(ResolutionFold::TierTraits{p->tier(), p->fillsWholeCell(), p->populatesOnMiss()});
        return traits;
    }

    struct FoldResult
    {
        VectorWithMemoryTracking<GeometryEntry> entries;
        size_t covered_end = 0;
    };

    /// Walk `span` with the iterator, checking the stride tiling invariants,
    /// and fold the resolutions into geometry entries. Strides tile the span
    /// gaplessly; only the LAST stride may overshoot the span end (a tier's
    /// true extent), and the walk's covered end is that overshoot.
    FoldResult foldOverSpan(
        const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & chain, ByteRange span)
    {
        ResidencyIterator it(chain, objects.front(), /*object_file_offset=*/0, span);
        ResolutionFold fold(traitsOf(chain), span);
        size_t pos = span.offset;
        while (pos < span.end())
        {
            const auto res = it.lookAt(pos);
            EXPECT_EQ(res.range.offset, pos) << "stride must start at the looked-at position";
            EXPECT_GT(res.range.size, 0u) << "stride must advance, pos=" << pos;
            EXPECT_EQ(res.tiers.size(), chain.size());
            fold.add(res);
            pos = res.range.end();
        }
        EXPECT_GE(pos, span.end()) << "strides must cover the span";

        auto slots = fold.finish();
        FoldResult result;
        result.covered_end = pos;
        for (auto & e : slots)
            if (!e.resident.empty() || !e.aligned_miss.empty())
                result.entries.push_back(std::move(e));
        return result;
    }

    /// The equivalence gate: iterated fold vs the live executor's plan geometry
    /// over the identical cache state. The iterator goes FIRST - its probe is
    /// read-only, while the executor's plan opens write buffers (creating
    /// segments) and its serve fills them.
    void expectFoldMatchesExecutor(
        const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & iterator_chain,
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches,
        size_t start,
        const String & label)
    {
        const ByteRange span = expectedSpan(start);
        const auto fold_result = foldOverSpan(iterator_chain, span);
        const auto & folded = fold_result.entries;

        auto src = std::make_shared<MemBoundedSource>(data);
        ReaderExecutor executor(src, objects, std::move(executor_caches), makeOptions());
        if (start != 0)
            executor.seek(start);
        executor.setReadExtent(FILE_SIZE);
        auto chain = executor.readNextWindow();
        ASSERT_FALSE(chain.empty()) << label;

        auto snap = inspect(executor).planGeometry();
        ASSERT_TRUE(snap) << label;
        ASSERT_EQ(snap->plan_start, span.offset) << label;
        /// The covered end, not the requested end: both walks must keep the
        /// same last-stride overshoot.
        ASSERT_EQ(snap->plan_end, fold_result.covered_end) << label;

        ASSERT_EQ(folded.size(), snap->entries.size()) << label << ": entry count";
        for (size_t i = 0; i < folded.size(); ++i)
        {
            const auto & f = folded[i];
            const auto & e = snap->entries[i];
            EXPECT_EQ(f.tier, e.tier) << label << ": entry " << i;
            EXPECT_EQ(f.whole_cell, e.whole_cell) << label << ": entry " << i;

            ASSERT_EQ(f.resident.size(), e.resident.size()) << label << ": entry " << i << " resident count";
            for (size_t k = 0; k < f.resident.size(); ++k)
            {
                EXPECT_EQ(f.resident[k].offset, e.resident[k].offset) << label << ": entry " << i << " resident " << k;
                EXPECT_EQ(f.resident[k].size, e.resident[k].size) << label << ": entry " << i << " resident " << k;
            }

            ASSERT_EQ(f.aligned_miss.size(), e.aligned_miss.size()) << label << ": entry " << i << " cell count";
            for (size_t k = 0; k < f.aligned_miss.size(); ++k)
            {
                EXPECT_EQ(f.aligned_miss[k].offset, e.aligned_miss[k].offset) << label << ": entry " << i << " cell " << k;
                EXPECT_EQ(f.aligned_miss[k].size, e.aligned_miss[k].size) << label << ": entry " << i << " cell " << k;
            }
        }
    }

    std::optional<ThreadStatus> thread_status;
    ContextMutablePtr query_context;
    std::optional<QueryScope> query_scope_holder;
    fs::path cache_root;

    String content;
    std::unordered_map<String, String> data;
    StoredObjects objects;
};

/// The iterator's own invariants: strides tile, a backward lookAt rewinds and
/// re-resolves the identical column.
TEST_F(ResidencyEquivalence, StridesTileTheSpanAndRewind)
{
    auto fc = makeFileCache("strides");
    warmReads({makeDiskProvider(fc)}, {{SEGMENT, SEGMENT}, {4 * SEGMENT, 2 * SEGMENT}});
    fabricatePartialCell(*makeDiskProvider(fc), ByteRange{8 * SEGMENT, SEGMENT}, SEGMENT / 4);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> chain;
    chain.push_back(makeDiskProvider(fc));
    const ByteRange span{0, 10 * SEGMENT};

    ResidencyIterator it(chain, objects.front(), 0, span);
    std::vector<ChainResolution> forward;
    size_t pos = span.offset;
    while (pos < span.end())
    {
        forward.push_back(it.lookAt(pos));
        pos = forward.back().range.end();
    }
    EXPECT_GT(forward.size(), 3u);

    for (auto walked = forward.rbegin(); walked != forward.rend(); ++walked)
    {
        const auto again = it.lookAt(walked->range.offset);
        EXPECT_EQ(again.range.offset, walked->range.offset);
        EXPECT_EQ(again.range.size, walked->range.size);
        ASSERT_EQ(again.tiers.size(), walked->tiers.size());
        for (size_t i = 0; i < again.tiers.size(); ++i)
        {
            EXPECT_EQ(again.tiers[i].state, walked->tiers[i].state);
            EXPECT_EQ(again.tiers[i].extent.offset, walked->tiers[i].extent.offset);
            EXPECT_EQ(again.tiers[i].extent.size, walked->tiers[i].extent.size);
        }
    }
}

TEST_F(ResidencyEquivalence, DiskTierFoldMatchesExecutorGeometry)
{
    auto fc = makeFileCache("disk_tier");
    warmReads({makeDiskProvider(fc)}, {{0, SEGMENT}, {3 * SEGMENT + ALIGNMENT, SEGMENT}});
    fabricatePartialCell(*makeDiskProvider(fc), ByteRange{6 * SEGMENT, SEGMENT}, SEGMENT / 2);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> iterator_chain;
    iterator_chain.push_back(makeDiskProvider(fc));
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches;
    executor_caches.push_back(makeDiskProvider(fc));

    expectFoldMatchesExecutor(iterator_chain, std::move(executor_caches), /*start=*/ALIGNMENT / 2, "disk");
}

TEST_F(ResidencyEquivalence, PageTierFoldMatchesExecutorGeometry)
{
    auto pc = makePageCache();
    /// The edge blocks are warmed so both span edges land INSIDE resident
    /// blocks: page hits are block-ceiled and overhang the span, which is the
    /// one case where the fold's hit clamp differs from the raw view.
    warmReads({makePageProvider(pc)}, {{0, 2 * BLOCK}, {2 * BLOCK, 6 * BLOCK}, {32 * BLOCK, 2 * BLOCK}});

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> iterator_chain;
    iterator_chain.push_back(makePageProvider(pc));
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches;
    executor_caches.push_back(makePageProvider(pc));

    expectFoldMatchesExecutor(iterator_chain, std::move(executor_caches), /*start=*/BLOCK / 2, "page");
}

TEST_F(ResidencyEquivalence, TwoTierFoldMatchesExecutorGeometry)
{
    auto fc = makeFileCache("two_tier");
    auto pc = makePageCache();

    /// Both tiers over one region (page hits prune nothing of fs - fs holds it
    /// too), fs-only over another (page cells become promote targets over the
    /// fs hit), plus a partial fs cell.
    {
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> both;
        both.push_back(makePageProvider(pc));
        both.push_back(makeDiskProvider(fc));
        warmReads(std::move(both), {{0, 2 * SEGMENT}});
    }
    warmReads({makeDiskProvider(fc)}, {{4 * SEGMENT, SEGMENT}});
    fabricatePartialCell(*makeDiskProvider(fc), ByteRange{7 * SEGMENT, SEGMENT}, SEGMENT / 4);

    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> iterator_chain;
    iterator_chain.push_back(makePageProvider(pc));
    iterator_chain.push_back(makeDiskProvider(fc));
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches;
    executor_caches.push_back(makePageProvider(pc));
    executor_caches.push_back(makeDiskProvider(fc));

    expectFoldMatchesExecutor(iterator_chain, std::move(executor_caches), /*start=*/SEGMENT / 2, "two-tier");
}

/// The EXTEND+SLIDE gate: a sequential stream crossing `plan_end` grows the
/// plan in place instead of rebuilding it - ONE observation for the whole
/// stream - while every extension slides the passed territory out, so the
/// retained span (entries and held buffers) is bounded by the reuse reach
/// plus the plan window, not by the stream length. The unaligned start makes
/// the cold miss cells straddle the (unaligned) plan end, so every extension
/// also exercises the straddle-cell dedup against the old entries' writers.
TEST_F(ResidencyEquivalence, SequentialStreamExtendsInsteadOfRebuilding)
{
    auto fc = makeFileCache("extend_gate");
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    const size_t start = ALIGNMENT / 2;
    const size_t want = FILE_SIZE - start;

    auto src = std::make_shared<MemBoundedSource>(data);
    ReaderExecutor executor(src, objects, std::move(caches), makeOptions());
    executor.seek(start);

    String served;
    while (served.size() < want)
    {
        executor.setReadExtent(std::min(start + want, executor.getPosition() + WINDOW));
        auto chain = executor.readNextWindow();
        if (chain.empty())
            break;
        for (const auto & node : chain.getNodes())
            served.append(node.data(), node.size);
    }
    ASSERT_EQ(served.size(), want);
    EXPECT_EQ(served, content.substr(start, want)) << "extended plans must serve the same bytes";

    /// One observation for the whole stream; every plan_end crossing extends.
    const size_t ceiling = std::max(WINDOW, PLAN_WINDOW);
    EXPECT_EQ(inspect(executor).observationCount(), 1u) << "plan_end crossings must extend, not rebuild";
    EXPECT_GE(inspect(executor).extensionCount(), want / ceiling - 2)
        << "extensions must carry the crossings";

    /// The slide keeps the retained span bounded: plan_start follows the cursor
    /// at the reuse reach, and the passed entries are released.
    const auto snap = inspect(executor).planGeometry();
    ASSERT_TRUE(snap);
    EXPECT_GE(snap->plan_start, FILE_SIZE - MIN_BYTES_FOR_SEEK - ceiling - SEGMENT)
        << "plan_start must slide behind the cursor";
    EXPECT_LE(snap->entries.size(), 8u) << "released entries must not accumulate";
}

/// The REUSE gate: seeks landing inside the live plan span keep the plan and
/// stop the teardown churn - the compact-merge ping-pong pattern (two column
/// streams alternating within one plan) runs on ONE observation with zero
/// prefetch cancels, and every served byte still matches the source.
TEST_F(ResidencyEquivalence, InPlanSeeksReusePlan)
{
    auto fc = makeFileCache("reuse_gate");
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;
    caches.push_back(makeDiskProvider(fc));

    /// Merge-like geometry: the stream separation sits UNDER the bridge bound
    /// (`min_bytes_for_seek`), as a compact merge's column streams sit under the
    /// production 2 MiB - these are the seeks the plan absorbs.
    auto options = makeOptions();
    options.min_bytes_for_seek = PLAN_WINDOW;

    auto src = std::make_shared<MemBoundedSource>(data);
    ReaderExecutor executor(src, objects, std::move(caches), std::move(options));
    executor.setReadExtent(FILE_SIZE);

    /// Two interleaved streams inside one plan span [0, PLAN_WINDOW).
    const size_t stream_b = PLAN_WINDOW / 2;
    std::vector<size_t> pattern;
    for (size_t step = 0; step + ALIGNMENT <= stream_b; step += ALIGNMENT)
    {
        pattern.push_back(step);
        pattern.push_back(stream_b + step);
    }
    ASSERT_GE(pattern.size(), 6u);

    for (const size_t pos : pattern)
    {
        if (static_cast<size_t>(executor.getPosition()) != pos)
            executor.seek(pos);
        auto chain = executor.readNextWindow();
        ASSERT_FALSE(chain.empty()) << "pos " << pos;
        String got;
        for (const auto & node : chain.getNodes())
            got.append(node.data(), node.size);
        EXPECT_EQ(got, content.substr(pos, got.size())) << "pos " << pos;
    }

    EXPECT_EQ(inspect(executor).observationCount(), 1u) << "in-plan seeks must reuse the plan";
    EXPECT_EQ(inspect(executor).prefetchCancelledCount(), 0u) << "in-plan seeks must not cancel prefetches";
    EXPECT_EQ(inspect(executor).prefetchDiscardedCount(), 0u) << "in-plan seeks must not discard running prefetches";
}

/// The residue-publication gate: a CACHELESS executor (no populating tier -
/// the machine's residue is the only producer-to-consumer pipe) serving an
/// interleaved two-stream pattern with merge-shaped extent stepping. The
/// worker's published preview lets the display serve the swings without
/// joining the machine, so the whole pattern runs on one plan with a handful
/// of source requests instead of one per swing.
TEST_F(ResidencyEquivalence, CachelessInterleavedStreamsServeFromPublishedResidue)
{
    VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> caches;

    auto options = makeOptions();
    options.min_bytes_for_seek = PLAN_WINDOW;
    options.prefetch_pool = std::make_shared<PrefetchThreadPool>(2);

    auto src = std::make_shared<MemBoundedSource>(data);
    ReaderExecutor executor(src, objects, std::move(caches), std::move(options));

    /// Two streams one ALIGNMENT apart (inside the one-window residue cap),
    /// consumed in extent-stepped chunks like a merge's granule advances.
    const size_t stream_b = ALIGNMENT;
    const size_t chunk = ALIGNMENT / 2;
    String got_a;
    String got_b;
    for (size_t step = 0; step + chunk <= 4 * WINDOW; step += chunk)
    {
        for (const bool b : {false, true})
        {
            const size_t pos = (b ? stream_b : 0) + step;
            String & got = b ? got_b : got_a;
            if (static_cast<size_t>(executor.getPosition()) != pos)
                executor.seek(pos);
            executor.setReadExtent(pos + chunk);
            size_t need = chunk;
            while (need > 0)
            {
                auto chain = executor.readNextWindow();
                ASSERT_FALSE(chain.empty()) << "pos " << pos;
                for (const auto & node : chain.getNodes())
                    got.append(node.data(), node.size);
                need -= std::min(need, chain.range().size);
            }
        }
        executor.setReadExtent(FILE_SIZE);
    }
    /// Each stream's consumed chunks are contiguous per stream but interleaved
    /// between them; verify byte identity chunk by chunk.
    for (size_t step = 0; step + chunk <= 4 * WINDOW; step += chunk)
    {
        EXPECT_EQ(got_a.substr(step, chunk), content.substr(step, chunk)) << "stream A at " << step;
        EXPECT_EQ(got_b.substr(step, chunk), content.substr(stream_b + step, chunk)) << "stream B at " << step;
    }

    EXPECT_EQ(inspect(executor).observationCount(), 1u) << "the interleave must stay on one plan";
    EXPECT_EQ(inspect(executor).prefetchDiscardedCount(), 0u) << "no producer work may be wasted";
    /// 18 today (was 32 without the preview + bank retention): the producer still
    /// relaunches per window and its reach grows only as the consumed run warms.
    /// The launch-cadence/allowance follow-up is what turns this into ~8.
    EXPECT_LE(inspect(executor).sourceRequests(), 20u)
        << "swings must serve from the published residue and the retained bank";
}

/// Randomized layout matrix: deterministic pseudo-randomness on purpose.
TEST_F(ResidencyEquivalence, RandomizedTwoTierMatrix)
{
    std::mt19937 rng(12345); /// NOLINT(cert-msc32-c,cert-msc51-cpp)

    for (size_t round = 0; round < 8; ++round)
    {
        auto fc = makeFileCache("matrix_" + std::to_string(round));
        auto pc = makePageCache();

        const size_t start = rng() % (FILE_SIZE - PLAN_WINDOW);

        /// Half the rounds warm the blocks containing the span's edges, so
        /// block-ceiled page hits straddle the span boundaries.
        if (rng() % 2)
        {
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> edge;
            edge.push_back(makePageProvider(pc));
            const size_t head_block = start / BLOCK * BLOCK;
            const size_t tail_block = std::min(start + PLAN_WINDOW, FILE_SIZE - 1) / BLOCK * BLOCK;
            warmReads(std::move(edge), {{head_block, BLOCK}, {tail_block, BLOCK}});
        }

        const size_t chain_warms = rng() % 3;
        for (size_t w = 0; w < chain_warms; ++w)
        {
            const size_t off = (rng() % (FILE_SIZE / BLOCK)) * BLOCK;
            const size_t want = std::min<size_t>(FILE_SIZE - off, (1 + rng() % 8) * BLOCK);
            VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> both;
            both.push_back(makePageProvider(pc));
            both.push_back(makeDiskProvider(fc));
            warmReads(std::move(both), {{off, want}});
        }

        const size_t fs_warms = rng() % 3;
        for (size_t w = 0; w < fs_warms; ++w)
        {
            const size_t off = (rng() % (FILE_SIZE / ALIGNMENT)) * ALIGNMENT;
            const size_t want = std::min<size_t>(FILE_SIZE - off, (1 + rng() % 4) * ALIGNMENT);
            warmReads({makeDiskProvider(fc)}, {{off, want}});
        }

        if (rng() % 2)
        {
            const size_t cell_idx = rng() % (FILE_SIZE / SEGMENT);
            const size_t prefix = ALIGNMENT * (1 + rng() % 3);
            fabricatePartialCell(
                *makeDiskProvider(fc), ByteRange{cell_idx * SEGMENT, SEGMENT}, std::min(prefix, SEGMENT - 1));
        }

        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> iterator_chain;
        iterator_chain.push_back(makePageProvider(pc));
        iterator_chain.push_back(makeDiskProvider(fc));
        VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> executor_caches;
        executor_caches.push_back(makePageProvider(pc));
        executor_caches.push_back(makeDiskProvider(fc));

        expectFoldMatchesExecutor(
            iterator_chain, std::move(executor_caches), start,
            "round " + std::to_string(round) + " start " + std::to_string(start));
    }
}

}
