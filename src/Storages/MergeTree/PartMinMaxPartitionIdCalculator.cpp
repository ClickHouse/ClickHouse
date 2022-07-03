#include "PartMinMaxPartitionIdCalculator.h"
#include <Storages/MergeTree/PartMetadataManagerOrdinary.h>

namespace DB {

std::pair<uint64_t, uint64_t>
PartMinMaxPartitionIdCalculator::calculate(const MergeTreeData::DataPartPtr & part, StorageMetadataPtr metadata, ContextPtr context) const
{
    auto min_partition_id = std::numeric_limits<uint64_t>::max();
    auto max_partition_id = std::numeric_limits<uint64_t>::min();

    IMergeTreeDataPart::MinMaxIndex min_max_index;

    auto metadata_manager = std::make_shared<PartMetadataManagerOrdinary>(part.get());
    auto block_with_min_max_values = min_max_index.loadIntoBlock(part->storage, metadata_manager);

    auto partition_id_columns = MergeTreePartition::executePartitionByExpression(metadata, block_with_min_max_values, context);

    for (const auto & partition_id_column : partition_id_columns) {
        const auto [extracted_min_partition_id, extracted_max_partition_id] = extractMinMaxValuesFromBlock(partition_id_column, block_with_min_max_values);

        min_partition_id = std::min(min_partition_id, extracted_min_partition_id);
        max_partition_id = std::max(max_partition_id, extracted_max_partition_id);
    }

    return std::make_pair(min_partition_id, max_partition_id);
}

std::pair<uint64_t, uint64_t>
PartMinMaxPartitionIdCalculator::extractMinMaxValuesFromBlock(const NameAndTypePair & partition_id_column, Block & block) const {
    auto & partition_column = block.getByName(partition_id_column.name);

    Field extracted_min_partition_id_field;
    Field extracted_max_partition_id_field;

//    extracted_min_partition_id_field.

    partition_column.column->get(0, extracted_min_partition_id_field);
    partition_column.column->get(1, extracted_max_partition_id_field);

    const auto extracted_min_partition_id = extracted_min_partition_id_field.get<uint64_t>();
    const auto extracted_max_partition_id = extracted_max_partition_id_field.get<uint64_t>();

    return std::make_pair(extracted_min_partition_id, extracted_max_partition_id);
}

}
