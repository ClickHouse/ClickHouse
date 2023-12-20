#include <Processors/Transforms/Streaming/WatermarkTransform.h>

#include <Analyzer/EmitNode.h>
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

    auto * emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
    {
        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());

        params.mode = WatermarkStamperParams::EmitMode::PERIODIC;
    }
    else
        params.mode = WatermarkStamperParams::EmitMode::NONE;
}

void setDefaultWatermarkParams(WatermarkStamperParams & params, bool has_aggregates, bool has_group_by)
{
    if (!has_aggregates && !has_group_by)
    {
        /// For streaming non-aggregation query
        if (params.mode != WatermarkStamperParams::EmitMode::TAIL &&
            params.mode != WatermarkStamperParams::EmitMode::NONE)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming tail mode doesn't support any watermark or periodic emit");

        /// Set default emit mode
        if (params.mode == WatermarkStamperParams::EmitMode::NONE)
            params.mode = WatermarkStamperParams::EmitMode::TAIL;
    }
    else
    {
        /// For streaming aggregation query
        if (params.mode == WatermarkStamperParams::EmitMode::TAIL)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming aggregation doesn't support tail emit");

        /// Set default emit mode
        if (params.mode == WatermarkStamperParams::EmitMode::NONE)
        {
            /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
            params.mode = WatermarkStamperParams::EmitMode::PERIODIC;
            params.periodic_interval.interval = DEFAULT_PERIODIC_INTERVAL.first;
            params.periodic_interval.unit = DEFAULT_PERIODIC_INTERVAL.second;
        }
    }
}
}

WatermarkStamperParams::WatermarkStamperParams(ASTPtr query, bool has_aggregates, bool has_group_by)
{
    const auto * select_query = query->as<ASTSelectQuery>();
    assert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    setDefaultWatermarkParams(*this, has_aggregates, has_group_by);
}

WatermarkStamperParams::WatermarkStamperParams(const QueryNode & query_node, bool has_aggregates)
{
    if (query_node.hasEmit())
    {
        auto & emit_node = query_node.getEmit()->as<EmitNode &>();
        if (emit_node.hasIntervalFunction())
        {
            periodic_interval = emit_node.getWindowInterval();
            switch (emit_node.getEmitType())
            {
                case EmitType::PERIODIC:
                    mode = WatermarkStamperParams::EmitMode::PERIODIC;
                    break;
            }
        }
        else
            mode = WatermarkStamperParams::EmitMode::NONE;
    }

    setDefaultWatermarkParams(*this, has_aggregates, query_node.hasGroupBy());
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
