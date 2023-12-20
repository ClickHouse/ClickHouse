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
        const std::vector<Range> & hyperrectangle,
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

        auto expression_list = extractKeyExpressionList(definition_ast);

        MonotonicityCheckVisitor::Data data {{table_with_columns}, destination_storage.getContext(), {}};

        for (auto i = 0u; i < expression_list->children.size(); i++)
        {
            data.range = hyperrectangle[i];

            MonotonicityCheckVisitor(data).visit(expression_list->children[i]);

            if (!data.monotonicity.is_monotonic || !data.monotonicity.is_positive)
            {
                return false;
            }

        }

        return true;
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
    const auto source_metadata = source_storage.getInMemoryMetadataPtr();
    const auto destination_metadata = destination_storage.getInMemoryMetadataPtr();

    const auto source_partition_key_ast = source_metadata->getPartitionKeyAST();
    const auto destination_partition_key_ast = destination_metadata->getPartitionKeyAST();

    // If destination partition expression columns are a subset of source partition expression columns,
    // there is no need to check for monotonicity.
    if (isExpressionDirectSubsetOf(source_partition_key_ast, destination_partition_key_ast))
    {
        return;
    }

    const auto src_global_min_max_indexes = MergeTreePartitionGlobalMinMaxIdxCalculator::calculate(
        source_parts,
        destination_storage
    );

    assert(!src_global_min_max_indexes.hyperrectangle.empty());

    if (!isDestinationPartitionExpressionMonotonicallyIncreasing(src_global_min_max_indexes.hyperrectangle, destination_storage))
    {
        throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Destination table partition expression is not monotonically increasing");
    }

    MergeTreePartition()
        .createAndValidateMinMaxPartitionIds(
            destination_storage.getInMemoryMetadataPtr(),
            src_global_min_max_indexes.getBlock(destination_storage),
            destination_storage.getContext()
        );
}

}
