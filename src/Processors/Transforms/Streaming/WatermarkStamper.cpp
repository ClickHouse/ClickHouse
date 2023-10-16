#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Processors/Chunk.h>
#include <Storages/SelectQueryInfo.h>
#include <base/ClockUtils.h>

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
const std::pair<Int64, IntervalKind> DEFAULT_PERIODIC_INTERVAL = {2, IntervalKind::Second};

void mergeEmitQuerySettings(const ASTPtr & emit_query, WatermarkStamperParams & params)
{
    if (!emit_query)
        return;

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
    {
        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());

        params.mode = WatermarkStamperParams::EmitMode::PERIODIC;
    }
    else
        params.mode = WatermarkStamperParams::EmitMode::NONE;
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
            periodic_interval.interval = DEFAULT_PERIODIC_INTERVAL.first;
            periodic_interval.unit = DEFAULT_PERIODIC_INTERVAL.second;
        }
    }
}

void WatermarkStamper::preProcess(const Block &)
{
    switch (params.mode)
    {
        case WatermarkStamperParams::EmitMode::PERIODIC:
        {
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
        case WatermarkStamperParams::EmitMode::PERIODIC:
        {
            processPeriodic(chunk);
            break;
        }
        default:
            break;
    }
}

void WatermarkStamper::processPeriodic(Chunk & chunk)
{
    auto now = MonotonicNanoseconds::now();
    if (now < next_periodic_emit_ts)
        return;

    next_periodic_emit_ts = now + periodic_interval;

    chunk.getOrCreateChunkContext()->setWatermark(now);
}

template <typename TimeColumnType, bool apply_watermark_per_row>
void WatermarkStamper::processWatermark(Chunk & chunk)
{
    if (!chunk.hasRows())
        return;

    std::function<Int64(Int64)> calc_watermark_ts;

    if constexpr (apply_watermark_per_row)
        calc_watermark_ts = [](Int64 event_ts) { return event_ts; };
    else
        calc_watermark_ts = [this](Int64 event_ts) { return calculateWatermark(event_ts); };

    Int64 event_ts_watermark = watermark_ts;

    /// [Process chunks]
    /// 1) filter and collect late events by param watermark_ts
    /// 2) update max event timestamp
    auto columns = chunk.detachColumns();
    const auto & time_vec = assert_cast<const TimeColumnType &>(*columns[time_col_pos]).getData();

    auto rows = time_vec.size();
    IColumn::Filter filter(rows, 1);

    if constexpr (!apply_watermark_per_row)
        event_ts_watermark = calc_watermark_ts(max_event_ts);

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

}
}
