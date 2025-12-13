#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB
{

/// Represents a storage for table function prometheusQuery().
class StoragePrometheusQuery : public IStorage
{
public:
    class Configuration
    {
    public:
        std::shared_ptr<const PrometheusQueryTree> promql_tree;
        PrometheusQueryEvaluationSettings evaluation_settings;
    };

    static Configuration getConfiguration(ASTs & args, ContextPtr context, bool over_range);

    StoragePrometheusQuery(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & configuration_);

    std::string getName() const override { return "PrometheusQuery"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    Configuration configuration;
    LoggerPtr log;
};

}
