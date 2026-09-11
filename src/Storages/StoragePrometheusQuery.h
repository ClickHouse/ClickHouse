#pragma once

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

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context, bool over_range);

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
