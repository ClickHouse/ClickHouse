#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
// #include <Interpreters/Streaming/TableFunctionDescription.h>
// #include <Interpreters/Streaming/TimeTransformHelper.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Processors/Chunk.h>
#include <Storages/SelectQueryInfo.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
// #include <Common/VersionRevision.h>
#include <Common/logger_useful.h>

#include <magic_enum.hpp>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INCORRECT_QUERY;
}

namespace Streaming
{
namespace
{
/// Default periodic interval
// const std::pair<Int64, IntervalKind> DEFAULT_PERIODIC_INTERVAL = {2, IntervalKind::Second};

void mergeEmitQuerySettings(const ASTPtr & emit_query, WatermarkStamperParams & params)
{
    if (!emit_query)
    {
        return;
    }

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
    {
        /// proton: porting notes. TODO: remove
        // if (emit->after_watermark)
        //     throw Exception("Streaming doesn't support having both any watermark and periodic emit", ErrorCodes::INCORRECT_QUERY);

        // if (emit->delay_interval)
        //     throw Exception("Streaming doesn't support having both delay and periodic emit", ErrorCodes::INCORRECT_QUERY);

        // if (emit->timeout_interval)
        //     throw Exception("Streaming doesn't support having both timeout and periodic emit", ErrorCodes::INCORRECT_QUERY);

        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());

        params.mode = WatermarkStamperParams::EmitMode::PERIODIC;
    }
    /// proton: porting notes. TODO: remove
    // else if (emit->after_watermark)
    // {
    //     if (!params.window_params)
    //         throw Exception(
    //             ErrorCodes::INCORRECT_QUERY, "Watermark emit is only supported in streaming queries with streaming window function");

    //     params.mode = params.window_params->type == WindowType::SESSION ? WatermarkStamperParams::EmitMode::WATERMARK_PER_ROW
    //                                                                     : WatermarkStamperParams::EmitMode::WATERMARK;
    // }
    else
        params.mode = WatermarkStamperParams::EmitMode::NONE;

    /// proton: porting notes. TODO: remove
    // if (emit->timeout_interval)
    //     params.timeout_interval = extractInterval(emit->timeout_interval->as<ASTFunction>());

    // if (emit->delay_interval)
    //     params.delay_interval = extractInterval(emit->delay_interval->as<ASTFunction>());
}
}

WatermarkStamperParams::WatermarkStamperParams(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result)
{
    const auto * select_query = query->as<ASTSelectQuery>();
    assert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    if (syntax_analyzer_result->aggregates.empty() && !syntax_analyzer_result->has_group_by)
    {
        /// For streaming non-aggregation query
        if (mode != EmitMode::TAIL && mode != EmitMode::NONE)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming tail mode doesn't support any watermark or periodic emit");

        /// Set default emit mode
        if (mode == EmitMode::NONE)
            mode = EmitMode::TAIL;
    }
    else
    {
        /// For streaming aggregation query
        if (mode == EmitMode::TAIL)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming aggregation doesn't support tail emit");

        /// Set default emit mode
        if (mode == EmitMode::NONE)
        {
            /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
            mode = EmitMode::PERIODIC;
            periodic_interval.interval = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first;
            periodic_interval.unit = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second;
        }
    }
}

void WatermarkStamper::preProcess(const Block &)
{
    switch (params.mode)
    {
        case WatermarkStamperParams::EmitMode::PERIODIC: {
            initPeriodicTimer(params.periodic_interval);
            break;
        }
        default:
            break;
    }
}

void WatermarkStamper::process(Chunk & chunk)
{
    switch (params.mode)
    {
        case WatermarkStamperParams::EmitMode::PERIODIC: {
            processPeriodic(chunk);
            break;
        }
        default:
            break;
    }

    // processTimeout(chunk);
    // logLateEvents();
}

#include <iostream>
void WatermarkStamper::processPeriodic(Chunk & chunk)
{
    assert(next_periodic_emit_ts);

    /// FIXME: use a Timer.
    auto now = MonotonicNanoseconds::now();
    if (now < next_periodic_emit_ts)
        return;

    if (chunk.getNumRows() == 0)
    {
        std::cout << "logger" << std::endl;
    }

    next_periodic_emit_ts = now + periodic_interval;

    chunk.getOrCreateChunkContext()->setWatermark(now);

    LOG_DEBUG(log, "Periodic emit time={}, rows={}", now, chunk.getNumRows());
}

// void WatermarkStamper::processTimeout(Chunk & chunk)
// {
//     if (next_timeout_emit_ts == 0)
//         return;

//     /// FIXME: use a Timer.
//     auto now = MonotonicNanoseconds::now();

//     /// Update next timeout ts if emitted
//     if (chunk.hasRows())
//     {
//         next_timeout_emit_ts = now + timeout_interval;
//         return;
//     }

//     if (now < next_timeout_emit_ts)
//         return;

//     watermark_ts = max_event_ts + 1;
//     next_timeout_emit_ts = now + timeout_interval;

//     chunk.getOrCreateChunkContext()->setWatermark(TIMEOUT_WATERMARK);
//     LOG_DEBUG(log, "Timeout emit time={}, rows={}", now, chunk.getNumRows());
// }

// void WatermarkStamper::logLateEvents()
// {
//     if (late_events > last_logged_late_events)
//     {
//         if (MonotonicSeconds::now() - last_logged_late_events_ts >= LOG_LATE_EVENTS_INTERVAL_SECONDS)
//         {
//             LOG_INFO(log, "Found {} late events for data. Last projected watermark={}", late_events, watermark_ts);
//             last_logged_late_events_ts = MonotonicSeconds::now();
//             last_logged_late_events = late_events;
//         }
//     }
// }

template <typename TimeColumnType, bool apply_watermark_per_row>
void WatermarkStamper::processWatermark(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    // assert(params.window_params);

    std::function<Int64(Int64)> calc_watermark_ts;
    // if (params.delay_interval)
    // {
    //     calc_watermark_ts = [this](Int64 event_ts) {
    //         auto event_ts_bias = addTime(
    //             event_ts,
    //             params.delay_interval.unit,
    //             -1 * params.delay_interval.interval,
    //             *params.window_params->time_zone,
    //             params.window_params->time_scale);

    //         if constexpr (apply_watermark_per_row)
    //             return event_ts_bias;
    //         else
    //             return calculateWatermark(event_ts_bias);
    //     };
    // }
    // else
    // {
    if constexpr (apply_watermark_per_row)
        calc_watermark_ts = [](Int64 event_ts) { return event_ts; };
    else
        calc_watermark_ts = [this](Int64 event_ts) { return calculateWatermark(event_ts); };
    // }

    Int64 event_ts_watermark = watermark_ts;

    /// [Process chunks]
    /// 1) filter and collect late events by param watermark_ts
    /// 2) update max event timestamp
    auto columns = chunk.detachColumns();
    const auto & time_vec = assert_cast<const TimeColumnType &>(*columns[time_col_pos]).getData();

    /// FIXME, use simple FilterTransform to do this ?
    auto rows = time_vec.size();
    IColumn::Filter filter(rows, 1);

    // UInt64 late_events_in_chunk = 0;
    // for (size_t i = 0; i < rows; ++i)
    // {
    //     const auto & event_ts = time_vec[i];
    //     if (event_ts > max_event_ts)
    //     {
    //         max_event_ts = event_ts;

    //         if constexpr (apply_watermark_per_row)
    //             event_ts_watermark = calc_watermark_ts(max_event_ts);
    //     }

    //     if (unlikely(event_ts < event_ts_watermark))
    //     {
    //         filter[i] = 0;
    //         ++late_events_in_chunk;
    //     }
    // }

    if constexpr (!apply_watermark_per_row)
        event_ts_watermark = calc_watermark_ts(max_event_ts);

    // if (late_events_in_chunk > 0)
    // {
    //     late_events += late_events_in_chunk;
    //     for (auto & column : columns)
    //         column = column->filter(filter, rows - late_events_in_chunk);
    // }

    chunk.setColumns(columns, columns[0]->size());

    /// [Update new watermark]
    /// Use max event time as new watermark
    if (event_ts_watermark > watermark_ts)
    {
        auto chunk_ctx = chunk.getOrCreateChunkContext();
        chunk_ctx->setWatermark(event_ts_watermark);
        watermark_ts = event_ts_watermark;
    }
}

/// proton: porting notes. TODO: remove comments. Do we need to keep this function?
Int64 WatermarkStamper::calculateWatermark([[maybe_unused]] Int64 event_ts) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "calculateWatermark() not implemented in {}", getName());
}

void WatermarkStamper::initPeriodicTimer(const WindowInterval & interval)
{
    if (interval.unit > IntervalKind::Day)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The maximum interval kind of streaming periodic emit policy is day, but got {}",
            magic_enum::enum_name(interval.unit));

    periodic_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Nanosecond);
    next_periodic_emit_ts = MonotonicNanoseconds::now() + periodic_interval;
}

// void WatermarkStamper::initTimeoutTimer(const WindowInterval & interval)
// {
//     if (interval.unit > IntervalKind::Day)
//         throw Exception(
//             ErrorCodes::NOT_IMPLEMENTED,
//             "The maximum interval kind of emit timeout is day, but got {}",
//             magic_enum::enum_name(interval.unit));

//     timeout_interval = BaseScaleInterval::toBaseScale(interval).toIntervalKind(IntervalKind::Nanosecond);
//     next_timeout_emit_ts = MonotonicNanoseconds::now() + timeout_interval;
// }

// VersionType WatermarkStamper::getVersionFromRevision(UInt64 revision) const
// {
//     if (version)
//         return *version;

//     return static_cast<VersionType>(revision);
// }

// VersionType WatermarkStamper::getVersion() const
// {
//     auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

//     if (!version)
//         version = ver;

//     return ver;
// }

// void WatermarkStamper::serialize(WriteBuffer & wb) const
// {
//     /// WatermarkStamper has its own version than WatermarkTransform
//     writeIntBinary(getVersion(), wb);

//     writeIntBinary(max_event_ts, wb);
//     writeIntBinary(watermark_ts, wb);
//     writeIntBinary(late_events, wb);
//     writeIntBinary(last_logged_late_events, wb);
//     writeIntBinary(last_logged_late_events_ts, wb);
// }

// void WatermarkStamper::deserialize(ReadBuffer & rb)
// {
//     version = 0;

//     readIntBinary(*version, rb);
//     readIntBinary(max_event_ts, rb);
//     readIntBinary(watermark_ts, rb);
//     readIntBinary(late_events, rb);
//     readIntBinary(last_logged_late_events, rb);
//     readIntBinary(last_logged_late_events_ts, rb);
// }
}
}
