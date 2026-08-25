#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{

/// Represents a storage for table function prometheusQuery().
class StoragePrometheusQuery : public IStorage
{
public:
    StoragePrometheusQuery(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const StorageID & time_series_storage_id_,
        const PrometheusQueryTree & promql_query_);

    void setEvaluationTime(const Field & time_);
    void setEvaluationRange(const PrometheusQueryEvaluationRange & range_);

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
    StorageID time_series_storage_id;
    PrometheusQueryTree promql_query;
    Field evaluation_time;
    PrometheusQueryEvaluationRange evaluation_range;

    LoggerPtr log;
};

}
