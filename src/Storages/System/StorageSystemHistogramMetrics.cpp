#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/System/StorageSystemHistogramMetrics.h>
#include <Common/HistogramMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemHistogramMetrics::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"metric", std::make_shared<DataTypeString>(), "Metric name."},
        {"value", std::make_shared<DataTypeFloat64>(), "Metric value."},
        {"description", std::make_shared<DataTypeString>(), "Metric description."},
        {"labels", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Metric labels."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "metric"}
    });

    return description;
}

void StorageSystemHistogramMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = HistogramMetrics::Factory::instance();
    factory.forEachFamily([&res_columns](const HistogramMetrics::MetricFamily & family)
    {
        const auto & buckets = family.getBuckets();
        const auto & labels = family.getLabels();

        family.forEachMetric([&res_columns, &family, &buckets, &labels](const HistogramMetrics::LabelValues & label_values, const HistogramMetrics::Metric & metric)
        {
            Map labels_map;
            for (size_t i = 0; i < label_values.size(); ++i)
            {
                labels_map.push_back(Tuple{labels[i], label_values[i]});
            }

            UInt64 partial_sum = 0;
            for (size_t counter_idx = 0; counter_idx < buckets.size() + 1; ++counter_idx)
            {
                partial_sum += metric.getCounter(counter_idx);

                String le;
                if (counter_idx < buckets.size())
                {
                    WriteBufferFromOwnString wb;
                    wb << buckets[counter_idx];
                    le = std::move(wb.str());
                }
                else
                {
                    le = "+Inf";
                }
                labels_map.push_back(Tuple{"le", le});

                res_columns[0]->insert(family.getName());
                res_columns[1]->insert(static_cast<HistogramMetrics::Value>(partial_sum));
                res_columns[2]->insert(family.getDocumentation());
                res_columns[3]->insert(labels_map);

                labels_map.pop_back();
            }

            // _sum metric
            res_columns[0]->insert(family.getName() + "_sum");
            res_columns[1]->insert(metric.getSum());
            res_columns[2]->insert(family.getDocumentation());
            res_columns[3]->insert(labels_map);
        });
    });
}

}
