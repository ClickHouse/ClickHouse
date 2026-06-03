#include <config.h>

#if USE_AVRO

#include <unordered_set>
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
    static boost::intrusive_ptr<DB::IAST> createPartitionKeyAST(const DB::PaimonTableSchema & table_schema)
    {
        auto partition_key_ast = DB::make_intrusive<DB::ASTFunction>();
        partition_key_ast->name = "tuple";
        partition_key_ast->arguments = DB::make_intrusive<DB::ASTExpressionList>();
        partition_key_ast->children.push_back(partition_key_ast->arguments);

        for (const auto & column_name : table_schema.partition_keys)
        {
            auto partition_ast = DB::make_intrusive<DB::ASTIdentifier>(column_name);
            partition_key_ast->arguments->children.emplace_back(std::move(partition_ast));
        }
        return partition_key_ast;
    }

    static DB::ColumnsDescription getPartitionColumnsDescription(
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
                {},
                context_);

            DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag_.getOutputs().front(), context_);
            key_condition.emplace(
                inverted_dag, context_, partition_key.column_names, partition_key.expression, true /* single_point */);
        }
    }

    bool PartitionPruner::canBePruned(const DB::PaimonManifestEntry & manifest_entry) const
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

    MinMaxIndexPruner::MinMaxIndexPruner(
        const DB::PaimonTableSchema & table_schema_,
        const DB::ActionsDAG & filter_dag,
        DB::ContextPtr context)
        : log(getLogger("MinMaxIndexPruner"))
    {
        if (filter_dag.getOutputs().empty())
            return;

        std::unordered_set<String> partition_key_set(
            table_schema_.partition_keys.begin(), table_schema_.partition_keys.end());

        DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context);

        column_conditions.reserve(table_schema_.fields.size());
        for (Int32 field_idx = 0; field_idx < static_cast<Int32>(table_schema_.fields.size()); ++field_idx)
        {
            const auto & field = table_schema_.fields[field_idx];
            if (partition_key_set.contains(field.name))
                continue;

            auto col_ast = DB::make_intrusive<DB::ASTFunction>();
            col_ast->name = "tuple";
            col_ast->arguments = DB::make_intrusive<DB::ASTExpressionList>();
            col_ast->children.push_back(col_ast->arguments);
            col_ast->arguments->children.emplace_back(DB::make_intrusive<DB::ASTIdentifier>(field.name));

            DB::NamesAndTypesList names_and_types;
            names_and_types.emplace_back(field.name, removeNullable(field.type.clickhouse_data_type));
            DB::ColumnsDescription col_desc(names_and_types);

            ColumnCondition cc;
            cc.key = DB::KeyDescription::getKeyFromAST(col_ast, col_desc, {}, context);
            auto cond = std::make_unique<DB::KeyCondition>(
                inverted_dag, context, cc.key.column_names, cc.key.expression, false /* not single_point */);

            if (cond->alwaysUnknownOrTrue())
                continue;

            cc.column_name = field.name;
            cc.schema_idx = field_idx;
            cc.condition = std::move(cond);
            cc.data_type = field.type;
            column_conditions.push_back(std::move(cc));
        }
    }

    bool MinMaxIndexPruner::canBePruned(const DB::PaimonManifestEntry & manifest_entry)
    {
        if (column_conditions.empty())
            return false;

        const auto & file = manifest_entry.file;

        if (file.value_stats.min_values.empty() || file.value_stats.max_values.empty())
            return false;

        BinaryRow min_row(file.value_stats.min_values);
        BinaryRow max_row(file.value_stats.max_values);

        /// Determine column -> BinaryRow position mapping based on _VALUE_STATS_COLS:
        ///   null or empty (legacy mode) : position i = schema field index i
        ///     Note: the Avro deserializer may return an empty Array for Avro null values,
        ///     so we treat both null and empty the same as legacy mode.
        ///   non-empty list (dense mode) : position j = column named valueStatsCols[j]
        const bool legacy_mode = !file.value_stats_cols.has_value() || file.value_stats_cols->empty();

        std::unordered_map<String, Int32> col_to_pos;
        if (!legacy_mode)
        {
            const auto & stats_cols = *file.value_stats_cols;
            col_to_pos.reserve(stats_cols.size());
            for (size_t i = 0; i < stats_cols.size(); ++i)
                col_to_pos[stats_cols[i].safeGet<String>()] = static_cast<Int32>(i);
        }

        for (const auto & col_cond : column_conditions)
        {
            Int32 pos;
            if (!legacy_mode)
            {
                /// Dense mode: look up column position; skip if column has no stats
                auto it = col_to_pos.find(col_cond.column_name);
                if (it == col_to_pos.end())
                    continue;
                pos = it->second;
            }
            else
            {
                /// Legacy mode: BinaryRow position = schema field index
                pos = col_cond.schema_idx;
            }

            /// Skip if stats are null for this column in this file
            if (min_row.isNullAt(pos) || max_row.isNullAt(pos))
                continue;

            DB::Field min_field = Paimon::getFieldFromBinaryRow(min_row, pos, col_cond.data_type);
            DB::Field max_field = Paimon::getFieldFromBinaryRow(max_row, pos, col_cond.data_type);

            /// Check if the filter condition can be satisfied anywhere in [min, max]
            DB::Row min_row_values = {min_field};
            DB::Row max_row_values = {max_field};
            std::vector<DB::FieldRef> min_refs(min_row_values.begin(), min_row_values.end());
            std::vector<DB::FieldRef> max_refs(max_row_values.begin(), max_row_values.end());

            bool can_be_true = col_cond.condition->mayBeTrueInRange(
                1, min_refs.data(), max_refs.data(), col_cond.key.data_types);

            if (!can_be_true)
                return true;
        }

        return false;
    }
}
#endif
