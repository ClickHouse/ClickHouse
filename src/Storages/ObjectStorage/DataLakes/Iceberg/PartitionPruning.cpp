#include "config.h"

#if USE_AVRO

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <Common/quoteString.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PartitionPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

using namespace DB;

namespace Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name, const String & column_name)
{
    std::shared_ptr<DB::ASTFunction> function = std::make_shared<ASTFunction>();
    function->arguments = std::make_shared<DB::ASTExpressionList>();
    function->children.push_back(function->arguments);

    if (transform_name == "year" || transform_name == "years")
    {
        function->name = "toYearNumSinceEpoch";
    }
    else if (transform_name == "month" || transform_name == "months")
    {
        function->name = "toMonthNumSinceEpoch";
    }
    else if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
    {
        function->name = "toRelativeDayNum";
    }
    else if (transform_name == "hour" || transform_name == "hours")
    {
        function->name = "toRelativeHourNum";
    }
    else if (transform_name == "identity")
    {
        return std::make_shared<ASTIdentifier>(column_name);
    }
    else if (transform_name.starts_with("truncate"))
    {
        function->name = "icebergTruncate";
        auto argument_start = transform_name.find('[');
        auto argument_width = transform_name.length() - 2 - argument_start;
        std::string width = transform_name.substr(argument_start + 1, argument_width);
        size_t truncate_width = DB::parse<size_t>(width);
        function->arguments->children.push_back(std::make_shared<DB::ASTLiteral>(truncate_width));
    }
    else if (transform_name == "void")
    {
        function->name = "tuple";
        return function;
    }
    else
    {
        return nullptr;
    }

    function->arguments->children.push_back(std::make_shared<DB::ASTIdentifier>(column_name));
    return function;
}

std::unique_ptr<DB::ActionsDAG> PartitionPruner::transformFilterDagForManifest(const DB::ActionsDAG * source_dag, Int32 manifest_schema_id, const std::vector<Int32> & partition_column_ids) const
{
    if (source_dag == nullptr)
        return nullptr;

    ActionsDAG dag_with_renames;
    for (const auto column_id : partition_column_ids)
    {
        auto column = schema_processor->tryGetFieldCharacteristics(current_schema_id, column_id);

        /// Columns which we dropped and doesn't exist in current schema
        /// cannot be queried in WHERE expression.
        if (!column.has_value())
            continue;

        /// We take data type from manifest schema, not latest type
        auto column_type = schema_processor->getFieldCharacteristics(manifest_schema_id, column_id).type;
        auto numeric_column_name = DB::backQuote(DB::toString(column_id));
        const auto * node = &dag_with_renames.addInput(numeric_column_name, column_type);
        node = &dag_with_renames.addAlias(*node, column->name);
        dag_with_renames.getOutputs().push_back(node);
    }
    auto result = std::make_unique<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(dag_with_renames), source_dag->clone()));
    result->removeUnusedActions();
    return result;

}

PartitionPruner::PartitionPruner(
    const DB::IcebergSchemaProcessor * schema_processor_,
    Int32 current_schema_id_,
    const DB::ActionsDAG * filter_dag,
    const ManifestFileContent & manifest_file,
    DB::ContextPtr context)
    : schema_processor(schema_processor_)
    , current_schema_id(current_schema_id_)
{
    if (manifest_file.hasPartitionKey())
    {
        partition_key = &manifest_file.getPartitionKeyDescription();
        auto transformed_dag = transformFilterDagForManifest(filter_dag, manifest_file.getSchemaId(), manifest_file.getPartitionKeyColumnIDs());
        if (transformed_dag != nullptr)
            key_condition.emplace(transformed_dag.get(), context, partition_key->column_names, partition_key->expression, true /* single_point */);
    }
}

bool PartitionPruner::canBePruned(const ManifestFileEntry & entry) const
{
    if (!key_condition.has_value())
        return false;

    const auto & partition_value = entry.partition_key_value;
    std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
    for (auto & field : index_value)
    {
        // NULL_LAST
        if (field.isNull())
            field = POSITIVE_INFINITY;
    }

    bool can_be_true = key_condition->mayBeTrueInRange(
        partition_value.size(), index_value.data(), index_value.data(), partition_key->data_types);

    return !can_be_true;
}


}

#endif
