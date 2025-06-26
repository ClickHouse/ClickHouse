#include "PartitionPruner.h"

#if USE_DELTA_KERNEL_RS
#include <DataTypes/DataTypeNullable.h>
#include <Common/logger_useful.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Storages/KeyDescription.h>
#include <Storages/ColumnsDescription.h>
#include "ExpressionVisitor.h"
#include "KernelUtils.h"


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DeltaLake
{

namespace
{
    DB::ASTPtr createPartitionKeyAST(const DB::Names & partition_columns)
    {
        /// DeltaLake supports only plain partition keys,
        /// e.g. by column names without any functions.

        std::shared_ptr<DB::ASTFunction> partition_key_ast = std::make_shared<DB::ASTFunction>();
        partition_key_ast->name = "tuple";
        partition_key_ast->arguments = std::make_shared<DB::ASTExpressionList>();
        partition_key_ast->children.push_back(partition_key_ast->arguments);

        for (const auto & column_name : partition_columns)
        {
            auto partition_ast = std::make_shared<DB::ASTIdentifier>(column_name);
            partition_key_ast->arguments->children.emplace_back(std::move(partition_ast));
        }
        return partition_key_ast;
    }

    DB::ColumnsDescription getPartitionColumnsDescription(
        const DB::Names & partition_columns,
        const DB::NamesAndTypesList & table_schema)
    {
        DB::NamesAndTypesList names_and_types;
        for (const auto & column_name : partition_columns)
        {
            auto column = table_schema.tryGetByName(column_name);
            if (!column.has_value())
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Not found partition column in schema: {}", column_name);
            names_and_types.emplace_back(column_name, removeNullable(column->type));
        }
        return DB::ColumnsDescription(names_and_types);
    }
}

PartitionPruner::PartitionPruner(
    const DB::ActionsDAG & filter_dag,
    const DB::NamesAndTypesList & table_schema_,
    const DB::Names & partition_columns_,
    const DB::NameToNameMap & physical_names_map_,
    DB::ContextPtr context)
    : physical_partition_columns(partition_columns_)
{
    if (!partition_columns_.empty())
    {
        const auto partition_columns_description = getPartitionColumnsDescription(partition_columns_, table_schema_);
        const auto partition_key_ast = createPartitionKeyAST(partition_columns_);

        partition_key = DB::KeyDescription::getKeyFromAST(
            partition_key_ast,
            partition_columns_description,
            context);

        DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context);
        key_condition.emplace(
            inverted_dag, context, partition_key.column_names, partition_key.expression, true /* single_point */);
    }
    if (!physical_names_map_.empty())
    {
        for (auto & name : physical_partition_columns)
            name = getPhysicalName(name, physical_names_map_);
    }
}

bool PartitionPruner::canBePruned(const DB::ObjectInfo & object_info) const
{
    if (!key_condition.has_value())
        return false;

    if (!object_info.data_lake_metadata.has_value())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data lake metadata is not set");
    if (!object_info.data_lake_metadata->transform)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data lake expression transform is not set");

    const auto partition_values = DeltaLake::getConstValuesFromExpression(
        physical_partition_columns,
        *object_info.data_lake_metadata->transform);

    LOG_TEST(getLogger("DeltaLakePartitionPruner"), "Partition values: {}", partition_values.size());

    DB::Row partition_key_values;
    partition_key_values.reserve(partition_values.size());

    for (const auto & value : partition_values)
    {
        if (value.isNull())
            partition_key_values.push_back(DB::POSITIVE_INFINITY); /// NULL_LAST
        else
            partition_key_values.push_back(value);
    }

    std::vector<DB::FieldRef> partition_key_values_ref(partition_key_values.begin(), partition_key_values.end());
    bool can_be_true = key_condition->mayBeTrueInRange(
        partition_key_values_ref.size(),
        partition_key_values_ref.data(),
        partition_key_values_ref.data(),
        partition_key.data_types);

    return !can_be_true;
}

}

#endif
