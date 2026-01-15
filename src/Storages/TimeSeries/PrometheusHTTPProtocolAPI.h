#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <IO/WriteBuffer.h>

namespace DB
{
class StorageTimeSeries;
class PrometheusQueryTree;

/// Helper class to support the Prometheus Query API endpoints.
/// Implements /api/v1/query, /api/v1/query_range, /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values
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
    };

    /// Execute an instant query (/api/v1/query) or range query (/api/v1/query_range)
    void executePromQLQuery(
        WriteBuffer & response,
        const Params & params);

    /// Get series metadata (/api/v1/series)
    void getSeries(
        WriteBuffer & response,
        const String & match_param,
        const String & start_param,
        const String & end_param);

    /// Get all label names (/api/v1/labels)
    void getLabels(
        WriteBuffer & response,
        const String & match_param,
        const String & start_param,
        const String & end_param);

    /// Get values for a specific label (/api/v1/label/<name>/values)
    void getLabelValues(
        WriteBuffer & response,
        const String & label_name,
        const String & match_param,
        const String & start_param,
        const String & end_param);

private:
    /// Convert timestamp parameter to Field
    Field parseTimestamp(const String & time_param);

    /// Convert step parameter to Field
    Field parseStep(const String & step_param);

    /// Write JSON response for instant query result (including scalars)
    void writeInstantQueryHeader(WriteBuffer & response);
    void writeScalarQueryResponse(WriteBuffer & response, const Block & result_block);
    void writeInstantQueryResponse(WriteBuffer & response, const Block & result_block);
    void writeInstantQueryFooter(WriteBuffer & response);

    /// Helper methods for writeInstantQueryResponse
    void writeScalarResult(WriteBuffer & response, const Block & result_block);
    void writeVectorResult(WriteBuffer & response, const Block & result_block);
    void writeMetricLabels(WriteBuffer & response, const Block & result_block, size_t row_index);

    /// Write JSON response for range query result
    void writeRangeQueryHeader(WriteBuffer & response);
    void writeRangeQueryFooter(WriteBuffer & response);
    void writeRangeQueryResponse(WriteBuffer & response, const Block & result_block);

    /// Write JSON response for series metadata
    void writeSeriesResponse(WriteBuffer & response, const Block & result_block);

    /// Write JSON response for labels
    void writeLabelsResponse(WriteBuffer & response, const Block & result_block);

    /// Write JSON response for label values
    void writeLabelValuesResponse(WriteBuffer & response, const Block & result_block);

    std::shared_ptr<const StorageTimeSeries> time_series_storage;
    Poco::LoggerPtr log;
};

}
