#include "PartitionPruner.h"

#if USE_DELTA_KERNEL_RS
#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>


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
    DB::ContextPtr context)
{
    if (!partition_columns_.empty())
    {
        const auto partition_columns_description = getPartitionColumnsDescription(partition_columns_, table_schema_);
        const auto partition_key_ast = createPartitionKeyAST(partition_columns_);

        partition_key = DB::KeyDescription::getKeyFromAST(
            partition_key_ast,
            partition_columns_description,
            context);

        key_condition.emplace(
            &filter_dag, context, partition_key.column_names, partition_key.expression, true /* single_point */);
    }
}

bool PartitionPruner::canBePruned(const DB::ObjectInfoWithPartitionColumns & object_info) const
{
    if (!key_condition.has_value())
        return false;

    DB::Row partition_key_values;
    partition_key_values.reserve(object_info.partitions_info.size());

    for (const auto & [name_and_type, value] : object_info.partitions_info)
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
