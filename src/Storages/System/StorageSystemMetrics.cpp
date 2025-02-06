
#include <atomic>
#include <Storages/ColumnsDescription.h>
#include <Common/CurrentMetrics.h>
#include <Common/HistogramMetrics.h>
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

    const auto & descriptors = HistogramMetrics::collect();
    for (const auto & metric_descriptor : descriptors)
    {
        const auto & counters = metric_descriptor.counters;
        const auto & buckets = metric_descriptor.buckets;

        Tuple extra_label;
        extra_label.push_back(metric_descriptor.label.first);
        extra_label.push_back(metric_descriptor.label.second);

        // _bucket metrics
        UInt64 partial_sum = 0;
        for (size_t counter_idx = 0; counter_idx < counters.size(); ++counter_idx)
        {
            partial_sum += counters[counter_idx].load(std::memory_order_relaxed);

            res_columns[0]->insert(metric_descriptor.name + "_bucket");
            res_columns[1]->insert(partial_sum);
            res_columns[2]->insert(metric_descriptor.documentation);

            Map labels;
            {
                Tuple le;
                le.push_back("le");
                le.push_back(counter_idx < buckets.size() ? std::to_string(buckets[counter_idx]) : "+Inf");
                labels.push_back(std::move(le));
            }

            labels.push_back(extra_label);

            res_columns[3]->insert(std::move(labels));
        }

        Map labels;
        labels.push_back(extra_label);

        // _count metric
        res_columns[0]->insert(metric_descriptor.name + "_count");
        res_columns[1]->insert(partial_sum);
        res_columns[2]->insert(metric_descriptor.documentation);
        res_columns[3]->insert(labels);

        // _sum metric
        res_columns[0]->insert(metric_descriptor.name + "_sum");
        res_columns[1]->insert(metric_descriptor.sum->load(std::memory_order_relaxed));
        res_columns[2]->insert(metric_descriptor.documentation);
        res_columns[3]->insert(labels);
    }
}

}
