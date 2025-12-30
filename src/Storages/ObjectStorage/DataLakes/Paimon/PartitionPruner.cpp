#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PartitionPruner.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace Paimon
{
    DB::ASTPtr createPartitionKeyAST(const DB::PaimonTableSchema & table_schema)
    {
        std::shared_ptr<DB::ASTFunction> partition_key_ast = std::make_shared<DB::ASTFunction>();
        partition_key_ast->name = "tuple";
        partition_key_ast->arguments = std::make_shared<DB::ASTExpressionList>();
        partition_key_ast->children.push_back(partition_key_ast->arguments);

        for (const auto & column_name : table_schema.partition_keys)
        {
            auto partition_ast = std::make_shared<DB::ASTIdentifier>(column_name);
            partition_key_ast->arguments->children.emplace_back(std::move(partition_ast));
        }
        return partition_key_ast;
    }

    DB::ColumnsDescription getPartitionColumnsDescription(
        const DB::PaimonTableSchema & table_schema)
    {
        DB::NamesAndTypesList names_and_types;
        for (const auto & column_name : table_schema.partition_keys)
        {
            auto column_idx_it = table_schema.fields_by_name_indexes.find(column_name);
            /// Only supports partition keys in table schema fields
            if (column_idx_it == table_schema.fields_by_name_indexes.end())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found partition column in schema: {}", column_name);
            auto column = table_schema.fields[column_idx_it->second];
            names_and_types.emplace_back(column_name, removeNullable(column.type.clickhouse_data_type));
        }
        return DB::ColumnsDescription(names_and_types);
    }

    PartitionPruner::PartitionPruner(const PaimonTableSchema & table_schema_,
                                     const DB::ActionsDAG & filter_dag_,
                                     DB::ContextPtr context_):
        table_schema(table_schema_)
    {
        if (!table_schema.partition_keys.empty())
        {
            const auto partition_columns_description = getPartitionColumnsDescription(table_schema);
            const auto partition_key_ast = createPartitionKeyAST(table_schema);

            partition_key = DB::KeyDescription::getKeyFromAST(
                partition_key_ast,
                partition_columns_description,
                context_);

            DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag_.getOutputs().front(), context_);
            key_condition.emplace(
                inverted_dag, context_, partition_key.column_names, partition_key.expression, true /* single_point */);
        }
    }

    bool PartitionPruner::canBePruned(const DB::PaimonManifestEntry & manifest_entry)
    {
        if (!key_condition.has_value())
            return false;


        DB::Row partition_key_values = Paimon::getPartitionFields(manifest_entry.partition, table_schema);
        for (auto & value : partition_key_values)
        {
            if (value.isNull())
                value = POSITIVE_INFINITY;
        }
        if (partition_key_values.empty())
            return false;
        std::vector<DB::FieldRef> partition_key_values_ref(partition_key_values.begin(), partition_key_values.end());
        return !key_condition->mayBeTrueInRange(partition_key_values_ref.size(), partition_key_values_ref.data(), partition_key_values_ref.data(), partition_key.data_types);
    }
}
#endif
