#pragma once

#include <Core/Types.h>
#include <Analyzer/QueryNode.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TreeRewriter.h>


namespace Poco
{
class Logger;
}

namespace DB
{
struct SelectQueryInfo;
class Chunk;

namespace Streaming
{
struct WatermarkStamperParams
{
public:
    WatermarkStamperParams(ASTPtr query, bool has_aggregates, bool has_group_by);
    WatermarkStamperParams(const QueryNode & query_node, bool has_aggregates);

    enum class EmitMode
    {
        NONE,
        TAIL,
        PERIODIC
    };

    EmitMode mode = EmitMode::NONE;

    WindowInterval periodic_interval;
};

using WatermarkStamperParamsPtr = std::shared_ptr<const WatermarkStamperParams>;

class WatermarkStamper
{
public:
    WatermarkStamper(const WatermarkStamperParams & params_, Poco::Logger * log_) : params(params_), log(log_) { }
    WatermarkStamper(const WatermarkStamper &) = default;
    ~WatermarkStamper() { }

    std::unique_ptr<WatermarkStamper> clone() const { return std::make_unique<WatermarkStamper>(*this); }

    String getName() const { return "WatermarkStamper"; }

    void preProcess(const Block & header);
    void process(Chunk & chunk);

private:
    template <typename TimeColumnType, bool apply_watermark_per_row>
    void processWatermark(Chunk & chunk);

    void processPeriodic(Chunk & chunk);

    Int64 calculateWatermark(Int64 event_ts) const;

    void initPeriodicTimer(const WindowInterval & interval);

protected:
    const WatermarkStamperParams & params;
    Poco::Logger * log;

    ssize_t time_col_pos = -1;

    /// For periodic
    Int64 next_periodic_emit_ts = 0;
    Int64 periodic_interval = 0;

    /// max event time observed so far
    Int64 max_event_ts = 0;

    /// max watermark projected so far
    Int64 watermark_ts = 0;

};

using WatermarkStamperPtr = std::unique_ptr<WatermarkStamper>;
}
}
