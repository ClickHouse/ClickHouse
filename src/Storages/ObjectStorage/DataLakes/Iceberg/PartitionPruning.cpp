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

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name)
{
    std::string transform_name = Poco::toLower(transform_name_src);

    if (transform_name == "year" || transform_name == "years")
        return makeASTFunction("toYearNumSinceEpoch", std::make_shared<DB::ASTIdentifier>(column_name));

    if (transform_name == "month" || transform_name == "months")
        return makeASTFunction("toMonthNumSinceEpoch", std::make_shared<DB::ASTIdentifier>(column_name));

    if (transform_name == "day" || transform_name == "date" || transform_name == "days" || transform_name == "dates")
        return makeASTFunction("toRelativeDayNum", std::make_shared<DB::ASTIdentifier>(column_name));

    if (transform_name == "hour" || transform_name == "hours")
        return makeASTFunction("toRelativeHourNum", std::make_shared<DB::ASTIdentifier>(column_name));

    if (transform_name == "identity")
        return std::make_shared<ASTIdentifier>(column_name);

    if (transform_name == "void")
        return makeASTFunction("tuple");

    if (transform_name.starts_with("truncate"))
    {
        /// should look like transform[N]

        if (transform_name.back() != ']')
            return nullptr;

        auto argument_start = transform_name.find('[');

        if (argument_start == std::string::npos)
            return nullptr;

        auto argument_width = transform_name.length() - 2 - argument_start;
        std::string width = transform_name.substr(argument_start + 1, argument_width);
        size_t truncate_width;
        bool parsed = DB::tryParse<size_t>(truncate_width, width);

        if (!parsed)
            return nullptr;

        return makeASTFunction("icebergTruncate", std::make_shared<DB::ASTLiteral>(truncate_width), std::make_shared<DB::ASTIdentifier>(column_name));
    }
    else
    {
        return nullptr;
    }
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
