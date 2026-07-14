#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <Storages/System/StorageSystemDimensionalMetrics.h>
#include <Common/DimensionalMetrics.h>


namespace DB
{

ColumnsDescription StorageSystemDimensionalMetrics::getColumnsDescription()
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

void StorageSystemDimensionalMetrics::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & factory = DimensionalMetrics::Factory::instance();
    factory.forEachFamily([&res_columns](const DimensionalMetrics::MetricFamily & family)
    {
        const auto & labels = family.getLabels();
        family.forEachMetric([&res_columns, &family, &labels](const DimensionalMetrics::LabelValues & label_values, const DimensionalMetrics::Metric & metric)
        {
            Map labels_map;
            for (size_t i = 0; i < label_values.size(); ++i)
            {
                labels_map.push_back(Tuple{labels[i], label_values[i]});
            }
            res_columns[0]->insert(family.getName());
            res_columns[1]->insert(metric.get());
            res_columns[2]->insert(family.getDocumentation());
            res_columns[3]->insert(labels_map);
        });
    });
}

}
