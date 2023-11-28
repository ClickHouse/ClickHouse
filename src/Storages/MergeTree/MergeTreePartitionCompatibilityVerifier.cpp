#include <Storages/MergeTree/MergeTreePartitionCompatibilityVerifier.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartitionGlobalMinMaxIdxCalculator.h>
#include <Interpreters/MonotonicityCheckVisitor.h>
#include <Interpreters/getTableExpressions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    bool isDestinationPartitionExpressionMonotonicallyIncreasing(
        const Range & range,
        const MergeTreeData & destination_storage
    )
    {
        auto destination_table_metadata = destination_storage.getInMemoryMetadataPtr();

        auto key_description = destination_table_metadata->getPartitionKey();
        auto definition_ast = key_description.definition_ast->clone();

        auto table_identifier = std::make_shared<ASTIdentifier>(destination_storage.getStorageID().getTableName());
        auto table_with_columns = TableWithColumnNamesAndTypes {
            DatabaseAndTableWithAlias(table_identifier, {}),
            destination_table_metadata->getColumns().getOrdinary()
        };

        MonotonicityCheckVisitor::Data data {{table_with_columns}, destination_storage.getContext(), {}};

        data.range = range;

        MonotonicityCheckVisitor(data).visit(definition_ast);

        return data.monotonicity.is_monotonic && data.monotonicity.is_positive;
    }

    void validatePartitionIds(
        const MergeTreeData & source_storage,
        const MergeTreeData & destination_storage,
        const Range & range
    )
    {
        // hyperrectangle, fix
        auto block_with_min_and_max_idx = IMergeTreeDataPart::MinMaxIndex::buildBlockWithMinAndMaxIndexes(
            source_storage,
            {range}
        );

        MergeTreePartition()
            .createAndValidateMinMaxPartitionIds(
                destination_storage.getInMemoryMetadataPtr(),
                block_with_min_and_max_idx,
                destination_storage.getContext()
            );
    }

    bool isExpressionDirectSubsetOf(const ASTPtr source, const ASTPtr destination)
    {
        auto source_expression_list = extractKeyExpressionList(source);
        auto destination_expression_list = extractKeyExpressionList(destination);

        std::unordered_set<std::string> source_columns;

        for (auto i = 0u; i < source_expression_list->children.size(); ++i)
        {
            source_columns.insert(source_expression_list->children[i]->getColumnName());
        }

        for (auto i = 0u; i < destination_expression_list->children.size(); ++i)
        {
            if (!source_columns.contains(destination_expression_list->children[i]->getColumnName()))
            {
                return false;
            }
        }

        return true;
    }

}

void MergeTreePartitionCompatibilityVerifier::verify(
    const MergeTreeData & source_storage,
    const MergeTreeData & destination_storage,
    const DataPartsVector & source_parts
)
{
    const auto source_partition_key_ast = source_storage.getInMemoryMetadataPtr()->getPartitionKeyAST();
    const auto destination_partition_key_ast = destination_storage.getInMemoryMetadataPtr()->getPartitionKeyAST();

    // If destination partition expression columns are a subset of source partition expression columns,
    // there is no need to check for monotonicity.
    if (isExpressionDirectSubsetOf(source_partition_key_ast, destination_partition_key_ast))
    {
        return;
    }

    auto src_global_min_max_indexes = MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(source_storage, source_parts);

    assert(!src_global_min_max_indexes.empty());

    // fix this
    auto [src_min_idx, src_max_idx] = src_global_min_max_indexes[0];

    if (src_min_idx.isNull() || src_max_idx.isNull())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "min_max_idx should never be null if a part exist");
    }

    auto range = Range(src_min_idx, true, src_max_idx, true);

    if (!isDestinationPartitionExpressionMonotonicallyIncreasing(range, destination_storage))
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Destination table partition expression is not monotonically increasing");
    }

    validatePartitionIds(source_storage, destination_storage, range);
}

}
