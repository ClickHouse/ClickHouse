#include <Processors/QueryPlan/Optimizations/DataPropertyDerivation.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB::QueryPlanOptimizations
{

DataPropertySet deriveDataPropertiesForStorageRead(const Block & output_header, const StorageInMemoryMetadata * metadata)
{
    DataPropertySet properties;
    for (size_t position = 0; position < output_header.columns(); ++position)
    {
        const auto & column = output_header.getByPosition(position);
        if (!canContainNull(*column.type))
            properties.addNonNullColumn({position, column.name});
    }

    if (!metadata || !metadata->hasUniqueKey())
        return properties;

    const auto output_names = output_header.getNames();
    const auto unique_key_names = metadata->getUniqueKeyColumns();
    auto unique_key = resolveColumnSetByName(output_names, unique_key_names);
    if (unique_key)
        properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration(std::move(*unique_key)));
    return properties;
}

DataPropertySet deriveDataProperties(const IQueryPlanStep & step, std::span<const DataPropertySet> child_properties)
{
    if (!child_properties.empty() || !step.hasOutputHeader())
        return {};

    if (!dynamic_cast<const ISourceStep *>(&step))
        return {};

    const StorageInMemoryMetadata * metadata = nullptr;
    if (const auto * storage_source = dynamic_cast<const SourceStepWithFilter *>(&step))
    {
        const auto & snapshot = storage_source->getStorageSnapshot();
        if (snapshot && snapshot->metadata)
            metadata = snapshot->metadata.get();
    }

    return deriveDataPropertiesForStorageRead(*step.getOutputHeader(), metadata);
}

}
