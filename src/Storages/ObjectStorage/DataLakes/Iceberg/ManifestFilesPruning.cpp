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
#include <fmt/ranges.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

using namespace DB;

namespace ProfileEvents
{
    extern const Event IcebergPartitionPrunnedFiles;
    extern const Event IcebergMinMaxIndexPrunnedFiles;
}


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

std::unique_ptr<DB::ActionsDAG> ManifestFilesPruner::transformFilterDagForManifest(const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter) const
{
    if (source_dag == nullptr)
        return nullptr;

    const auto & inputs = source_dag->getInputs();

    for (const auto & input : inputs)
    {
        if (input->type == ActionsDAG::ActionType::INPUT)
        {
            std::string input_name = input->result_name;
            std::optional<Int32> input_id = schema_processor.tryGetColumnIDByName(current_schema_id, input_name);
            if (input_id)
                used_columns_in_filter.push_back(*input_id);
        }
    }

    ActionsDAG dag_with_renames;
    for (const auto column_id : used_columns_in_filter)
    {
        auto column = schema_processor.tryGetFieldCharacteristics(current_schema_id, column_id);

        /// Columns which we dropped and don't exist in current schema
        /// cannot be queried in WHERE expression.
        if (!column.has_value())
            continue;

        /// We take data type from manifest schema, not latest type
        auto column_from_manifest = schema_processor.tryGetFieldCharacteristics(manifest_schema_id, column_id);
        if (!column_from_manifest.has_value())
            continue;

        auto numeric_column_name = DB::backQuote(DB::toString(column_id));
        const auto * node = &dag_with_renames.addInput(numeric_column_name, column_from_manifest->type);
        node = &dag_with_renames.addAlias(*node, column->name);
        dag_with_renames.getOutputs().push_back(node);
    }
    auto result = std::make_unique<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(dag_with_renames), source_dag->clone()));
    result->removeUnusedActions();
    return result;

}


ManifestFilesPruner::ManifestFilesPruner(
    const DB::IcebergSchemaProcessor & schema_processor_,
    Int32 current_schema_id_,
    const DB::ActionsDAG * filter_dag,
    const ManifestFileContent & manifest_file,
    DB::ContextPtr context)
    : schema_processor(schema_processor_)
    , current_schema_id(current_schema_id_)
    , manifest_schema_id(manifest_file.getSchemaId())
{
    std::unique_ptr<ActionsDAG> transformed_dag;
    std::vector<Int32> used_columns_in_filter;
    if (manifest_file.hasPartitionKey() || manifest_file.hasBoundsInfoInManifests())
        transformed_dag = transformFilterDagForManifest(filter_dag, used_columns_in_filter);

    if (manifest_file.hasPartitionKey())
    {
        partition_key = &manifest_file.getPartitionKeyDescription();
        if (transformed_dag != nullptr)
            partition_key_condition.emplace(transformed_dag.get(), context, partition_key->column_names, partition_key->expression, true /* single_point */);
    }

    if (manifest_file.hasBoundsInfoInManifests() && transformed_dag != nullptr)
    {
        {
            const auto & bounded_colums = manifest_file.getColumnsIDsWithBounds();
            for (Int32 used_column_id : used_columns_in_filter)
            {
                if (!bounded_colums.contains(used_column_id))
                    continue;

                NameAndTypePair name_and_type = schema_processor.getFieldCharacteristics(manifest_schema_id, used_column_id);
                name_and_type.name = DB::backQuote(DB::toString(used_column_id));

                ExpressionActionsPtr expression = std::make_shared<ExpressionActions>(
                    ActionsDAG({name_and_type}), ExpressionActionsSettings(context));

                min_max_key_conditions.emplace(used_column_id, KeyCondition(transformed_dag.get(), context, {name_and_type.name}, expression));
            }
        }
    }
}

bool ManifestFilesPruner::canBePruned(const ManifestFileEntry & entry) const
{
    if (partition_key_condition.has_value())
    {
        const auto & partition_value = entry.partition_key_value;
        std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
        for (auto & field : index_value)
        {
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
        }

        bool can_be_true = partition_key_condition->mayBeTrueInRange(
            partition_value.size(), index_value.data(), index_value.data(), partition_key->data_types);

        if (!can_be_true)
        {
            ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunnedFiles);
            return true;
        }
    }

    for (const auto & [column_id, key_condition] : min_max_key_conditions)
    {
        std::optional<NameAndTypePair> name_and_type = schema_processor.tryGetFieldCharacteristics(manifest_schema_id, column_id);

        /// There is no such column in this manifest file
        if (!name_and_type.has_value())
            continue;

        auto hyperrectangle = entry.columns_infos.at(column_id).hyperrectangle;
        if (hyperrectangle.has_value() && !key_condition.mayBeTrueInRange(1, &hyperrectangle->left, &hyperrectangle->right, {name_and_type->type}))
        {
            ProfileEvents::increment(ProfileEvents::IcebergMinMaxIndexPrunnedFiles);
            return true;
        }
    }

    return false;
}


}

#endif
