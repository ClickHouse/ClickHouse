#pragma once

#include <Common/Logger_fwd.h>
#include <Core/Names.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <IO/WriteBuffer.h>

namespace DB
{
class StorageTimeSeries;
class PrometheusQueryTree;
class PullingAsyncPipelineExecutor;
enum class PrometheusQueryResultType;

/// Helper class to support the query and metadata endpoints of the Prometheus HTTP API.
/// Implements /api/v1/query, /api/v1/query_range, /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values, /api/v1/metadata
class PrometheusHTTPProtocolAPI : public WithMutableContext
{
public:
    PrometheusHTTPProtocolAPI(ConstStoragePtr time_series_storage_, const ContextMutablePtr & context_);
    ~PrometheusHTTPProtocolAPI();

    enum class Type
    {
        Instant,
        Range,
    };

    struct Params
    {
        Type type;
        String promql_query;
        /// Only for Instant query
        String time_param;
        /// Only for Range query
        String start_param;
        String end_param;
        String step_param;
        String lookback_delta_param;
    };

    /// Execute an instant query (/api/v1/query) or range query (/api/v1/query_range)
    void executePromQLQuery(
        WriteBuffer & response,
        const Params & params,
        QueryFinishCallback query_finish_callback = {});

    /// Get series metadata (/api/v1/series): the union of the series matched by the `match[]` selectors, capped by `limit` (0 means no limit).
    void getSeries(
        WriteBuffer & response,
        const Strings & match_params,
        const String & start_param,
        const String & end_param,
        UInt64 limit,
        QueryFinishCallback query_finish_callback = {});

    /// Get metric metadata (/api/v1/metadata): the distinct (type, help, unit) entries stored in the Metrics target table,
    /// grouped by metric family. `metric` (if not empty) restricts the result to one metric family;
    /// `limit` caps the number of returned metric families (a negative value means no limit, 0 returns an empty result);
    /// `limit_per_metric` caps the number of returned entries per metric family (zero and negative values mean no limit).
    void getMetadata(
        WriteBuffer & response,
        const String & metric_param,
        Int64 limit,
        Int64 limit_per_metric,
        QueryFinishCallback query_finish_callback = {});

    /// Get label names (/api/v1/labels): the sorted unique label names of the series matched by the `match[]`
    /// selectors (or of all series if no selectors are given), capped by `limit` (0 means no limit).
    void getLabels(
        WriteBuffer & response,
        const Strings & match_params,
        const String & start_param,
        const String & end_param,
        UInt64 limit,
        QueryFinishCallback query_finish_callback = {});

    /// Get values for a specific label (/api/v1/label/<name>/values)
    void getLabelValues(
        WriteBuffer & response,
        const String & label_name,
        const String & match_param,
        const String & start_param,
        const String & end_param);

private:
    /// Parses the `match[]` instant selectors and the optional `start` and `end` bounds of the metadata endpoints
    /// and makes a UNION ALL query selecting the ids (`series_id`) of the series matched by any of the selectors,
    /// with their tags registered for timeSeriesIdToTags.
    ASTPtr makeSeriesIDsQuery(const Strings & match_params, const String & start_param, const String & end_param);

    /// Writes the result of a prometheus query as a JSON.
    void writeQueryResponse(WriteBuffer & response, PullingAsyncPipelineExecutor & pulling_executor, PrometheusQueryResultType result_type);

    /// Helper methods.
    void writeQueryResponseHeader(WriteBuffer & response, PrometheusQueryResultType result_type);
    void writeQueryResponseFooter(WriteBuffer & response);
    void writeQueryResponseBlock(WriteBuffer & response, PrometheusQueryResultType result_type, const Block & result_block, bool first);
    void writeQueryResponseScalarBlock(WriteBuffer & response, const Block & result_block, bool first);
    void writeQueryResponseStringBlock(WriteBuffer & response, const Block & result_block, bool first);
    void writeQueryResponseInstantVectorBlock(WriteBuffer & response, const Block & result_block, bool first);
    void writeQueryResponseRangeVectorBlock(WriteBuffer & response, const Block & result_block, bool first);
    void writeTags(WriteBuffer & response, const Block & result_block, size_t row_index);
    void writeTimestamp(WriteBuffer & response, DateTime64 value, UInt32 scale);
    void writeScalar(WriteBuffer & response, Float64 value);

    std::shared_ptr<const StorageTimeSeries> time_series_storage;
    FormatSettings format_settings;
    LoggerPtr log;
};

}
