#pragma once

#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB
{

/// Represents a storage for table function prometheusQuery().
class StoragePrometheusQuery : public StorageWithCommonVirtualColumns
{
public:
    struct Configuration
    {
        std::shared_ptr<const PrometheusQueryTree> promql_query;
        PrometheusQueryEvaluationSettings evaluation_settings;
    };

    /// Everything the arguments of prometheusQuery() / prometheusQueryRange() determine without reading
    /// the catalog. The query is kept as text and the times as written, because parsing the query and
    /// converting the times both need the scale of the TimeSeries table's timestamps.
    struct Arguments
    {
        StorageID time_series_storage_id = StorageID::createEmpty();
        String promql_query;
        PrometheusQueryEvaluationMode mode = {};
        Field start_time;
        DataTypePtr start_time_type;
        Field end_time;
        DataTypePtr end_time_type;
        /// Only prometheusQueryRange() has a step; prometheusQuery() evaluates at a single instant.
        Field step;
        DataTypePtr step_type;
    };

    /// `parseArgumentsOnly()` must not read the catalog: a stored `AS prometheusQuery(...)` definition
    /// is replayed through it while metadata is loaded, so it has to succeed even when the objects it
    /// names are gone. Reading them is `resolveConfiguration()`'s job.
    static Arguments parseArgumentsOnly(ASTs & args, const ContextPtr & context, bool over_range);
    static Configuration resolveConfiguration(const Arguments & parsed_args, const ContextPtr & context);

    StoragePrometheusQuery(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "PrometheusQuery"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    Configuration config;
    LoggerPtr log;
};

}
