#include <Processors/QueryResultPreview.h>

#include <Core/Settings.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool query_result_previews;
    extern const SettingsUInt64 query_result_previews_min_interval_ms;
    extern const SettingsUInt64 query_result_previews_min_rows;
    extern const SettingsUInt64 query_result_previews_min_bytes;
    extern const SettingsUInt64 query_result_previews_max_result_rows;
    extern const SettingsUInt64 query_result_previews_max_result_bytes;
}

bool isQueryResultPreview(const Chunk & chunk)
{
    return chunk.getChunkInfos().has<QueryResultPreviewInfo>();
}

void markAsQueryResultPreview(Chunk & chunk)
{
    if (!isQueryResultPreview(chunk))
        chunk.getChunkInfos().add(std::make_shared<QueryResultPreviewInfo>());
}

QueryResultPreviewsSettings QueryResultPreviewsSettings::fromSettings(const Settings & settings)
{
    QueryResultPreviewsSettings res;
    res.enabled = settings[Setting::query_result_previews];
    res.min_interval_ms = settings[Setting::query_result_previews_min_interval_ms];
    res.min_rows = settings[Setting::query_result_previews_min_rows];
    res.min_bytes = settings[Setting::query_result_previews_min_bytes];
    res.max_result_rows = settings[Setting::query_result_previews_max_result_rows];
    res.max_result_bytes = settings[Setting::query_result_previews_max_result_bytes];
    return res;
}

QueryResultPreviewsControl::QueryResultPreviewsControl(const QueryResultPreviewsSettings & settings_, size_t num_participants)
    : settings(settings_)
{
    participant_mutexes.reserve(num_participants);
    for (size_t i = 0; i < num_participants; ++i)
        participant_mutexes.emplace_back(std::make_unique<std::mutex>());

    /// The first preview is emitted no earlier than `min_interval_ms` after the start.
    last_preview_ns.store(clockNanoseconds(), std::memory_order_relaxed);
}

UInt64 QueryResultPreviewsControl::clockNanoseconds()
{
    /// The coarse clock is enough for the preview frequency thresholds, and `Common/Stopwatch.h`
    /// (via `base/time.h`) defines a portable fallback for platforms without it.
    return clock_gettime_ns(CLOCK_MONOTONIC_COARSE);
}

bool QueryResultPreviewsControl::accountAndCheckThresholds(UInt64 rows, UInt64 bytes)
{
    if (isDisabled() || !isActivated())
        return false;

    UInt64 total_rows = rows_since_last_preview.fetch_add(rows, std::memory_order_relaxed) + rows;
    UInt64 total_bytes = bytes_since_last_preview.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    if (total_rows < settings.min_rows || total_bytes < settings.min_bytes)
        return false;

    if (settings.min_interval_ms
        && clockNanoseconds() - last_preview_ns.load(std::memory_order_relaxed) < settings.min_interval_ms * 1'000'000ULL)
        return false;

    return true;
}

void QueryResultPreviewsControl::startNextRound()
{
    rows_since_last_preview.store(0, std::memory_order_relaxed);
    bytes_since_last_preview.store(0, std::memory_order_relaxed);
    last_preview_ns.store(clockNanoseconds(), std::memory_order_relaxed);
}

}
