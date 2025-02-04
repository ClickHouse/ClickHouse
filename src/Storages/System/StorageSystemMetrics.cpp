
#include <atomic>
#include <Storages/ColumnsDescription.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentHistogramMetrics.h>
#include "DataTypes/DataTypeMap.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemMetrics.h>



namespace DB
{

ColumnsDescription StorageSystemMetrics::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeInt64>(), "Metric value."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
        {"labels", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Metric labels."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "metric"}
    });

    return description;
}

void StorageSystemMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        Int64 value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

        res_columns[0]->insert(CurrentMetrics::getName(CurrentMetrics::Metric(i)));
        res_columns[1]->insert(value);
        res_columns[2]->insert(CurrentMetrics::getDocumentation(CurrentMetrics::Metric(i)));
        res_columns[3]->insertDefault();
    }

    for (size_t i = 0, end = CurrentHistogramMetrics::end(); i < end; ++i)
    {
        const CurrentHistogramMetrics::MetricInfo & metric_info = CurrentHistogramMetrics::metrics[i];
        const auto & buckets = metric_info.buckets;
        const auto & counters = metric_info.counters;

        // _bucket metrics
        UInt64 partial_sum = 0;
        for (size_t counter_idx = 0; i < counters.size(); ++i)
        {
            partial_sum += counters[counter_idx].load(std::memory_order_relaxed);

            res_columns[0]->insert(metric_info.name + "_bucket");
            res_columns[1]->insert(partial_sum);
            res_columns[2]->insert(metric_info.documentation);

            Map labels;
            {
                const std::string le = counter_idx < buckets.size() ? std::to_string(buckets[counter_idx]) : "+Inf";

                Tuple pair;
                pair.push_back("le");
                pair.push_back(le);

                labels.push_back(std::move(pair));
            }
            res_columns[3]->insert(std::move(labels));
        }

        // _count metric
        res_columns[0]->insert(metric_info.name + "_count");
        res_columns[1]->insert(partial_sum);
        res_columns[2]->insert(metric_info.documentation);
        res_columns[3]->insertDefault();

        // _sum metric
        const Int64 values_sum = CurrentHistogramMetrics::sums[i].load(std::memory_order_relaxed);
        res_columns[0]->insert(metric_info.name + "_sum");
        res_columns[1]->insert(values_sum);
        res_columns[2]->insert(metric_info.documentation);
        res_columns[3]->insertDefault();
    }
}

}
