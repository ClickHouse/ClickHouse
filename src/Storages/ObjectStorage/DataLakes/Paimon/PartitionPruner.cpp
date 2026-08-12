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
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace Paimon
{
    /// Whether a column's min/max stats can be decoded by `getFieldFromBinaryRow` and used for pruning.
    /// This must stay in sync with the `switch` in `getFieldFromBinaryRow`: building a `ColumnCondition`
    /// for a type that `getFieldFromBinaryRow` cannot decode (e.g. `BINARY`/`VARBINARY`/`ARRAY`/`MAP`/`ROW`,
    /// or a `TIMESTAMP` with precision > 3) would turn `use_paimon_minmax_index_pruning=1` into a query
    /// exception whenever the predicate references such a column. For those columns pruning is simply disabled.
    static bool canDecodeMinMaxStats(const DataType & type)
    {
        switch (type.root_type)
        {
            case RootDataType::CHAR:
            case RootDataType::VARCHAR:
            case RootDataType::BOOLEAN:
            case RootDataType::DECIMAL:
            case RootDataType::TINYINT:
            case RootDataType::SMALLINT:
            case RootDataType::INTEGER:
            case RootDataType::BIGINT:
            case RootDataType::FLOAT:
            case RootDataType::DOUBLE:
            case RootDataType::DATE:
            case RootDataType::TIME_WITHOUT_TIME_ZONE:
                return true;
            case RootDataType::TIMESTAMP_WITHOUT_TIME_ZONE:
            case RootDataType::TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            {
                /// `BinaryRow::getTimestamp` only supports DateTime64 with scale <= 3; a higher-precision
                /// timestamp column (e.g. TIMESTAMP(6)) would otherwise throw while reading the bound.
                const auto * dt64 = typeid_cast<const DB::DataTypeDateTime64 *>(removeNullable(type.clickhouse_data_type).get());
                return dt64 && dt64->getScale() <= 3;
            }
            default:
                return false;
        }
    }

    bool legacyValueStatsArePositional(Int32 stats_arity, size_t null_counts_size, size_t schema_field_count)
    {
        return stats_arity >= 0
            && static_cast<size_t>(stats_arity) == schema_field_count
            && null_counts_size == schema_field_count;
    }

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

            DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag_.getOutputs().front(), context_, /* boolean_context */ true);
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
        : schema_id(table_schema_.id)
        , schema_field_count(table_schema_.fields.size())
        , log(getLogger("MinMaxIndexPruner"))
    {
        if (filter_dag.getOutputs().empty())
            return;

        std::unordered_set<String> partition_key_set(
            table_schema_.partition_keys.begin(), table_schema_.partition_keys.end());

        DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context, /* boolean_context */ true);

        column_conditions.reserve(table_schema_.fields.size());
        for (Int32 field_idx = 0; field_idx < static_cast<Int32>(table_schema_.fields.size()); ++field_idx)
        {
            const auto & field = table_schema_.fields[field_idx];
            if (partition_key_set.contains(field.name))
                continue;

            /// Skip columns whose min/max stats cannot be decoded safely, so that enabling
            /// `use_paimon_minmax_index_pruning` never turns a valid table into a query exception.
            if (!canDecodeMinMaxStats(field.type))
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

    bool MinMaxIndexPruner::canBePruned(const DB::PaimonManifestEntry & manifest_entry) const
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

        /// Value stats are `BinaryRow`s encoded in the data file's own (write-time) schema, and this pruner
        /// decodes them with the schema it was built from (`schema_id`). That is only safe when the file was
        /// written with that same schema; after schema evolution neither read mode can be trusted:
        ///   - Legacy mode maps stats positionally via `schema_idx` (the field index in this pruner's schema),
        ///     so an older file's position can refer to a different column or type.
        ///   - Dense mode (`_VALUE_STATS_COLS` present) matches stats to a column by name, which locates the
        ///     right slot, but still decodes the bytes with the current column's `DataType`. Paimon type
        ///     evolution can preserve a column name while changing its physical encoding (e.g. widening
        ///     `DECIMAL(10, 0)` to `DECIMAL(20, 0)` switches `getFieldFromBinaryRow` from the fixed-size to the
        ///     varlen decode path), so the old-file bytes would be misread into a wrong bound.
        /// In either case `mayBeTrueInRange` could throw or falsely prune a file that still contains matching
        /// rows, so fail closed and skip min/max pruning for any file whose `schema_id` differs (it is read in
        /// full, which is always correct).
        if (file.schema_id != schema_id)
            return false;

        const auto & null_counts = file.value_stats.null_counts;

        std::unordered_map<String, Int32> col_to_pos;
        if (legacy_mode)
        {
            /// A legacy stats row carries no column list, so the only mapping available is positional. It is
            /// trustworthy only when the row covers the whole schema; a file written with a projected write
            /// schema would silently shift every position and could prune a file that still matches.
            /// Fail closed and read such a file in full - see `legacyValueStatsArePositional`.
            if (!legacyValueStatsArePositional(min_row.getArity(), null_counts.size(), schema_field_count)
                || !legacyValueStatsArePositional(max_row.getArity(), null_counts.size(), schema_field_count))
            {
                LOG_TRACE(
                    log,
                    "Skipping min/max pruning for file {}: legacy value statistics cover {} column(s) "
                    "(null counts: {}) but the table schema has {} field(s)",
                    file.file_name,
                    min_row.getArity(),
                    null_counts.size(),
                    schema_field_count);
                return false;
            }
        }
        else
        {
            const auto & stats_cols = *file.value_stats_cols;
            /// The column list and the stats row describe the same tuple, so a length mismatch means the
            /// positions derived from the list do not address the row. Fail closed as well.
            if (static_cast<size_t>(min_row.getArity()) != stats_cols.size()
                || static_cast<size_t>(max_row.getArity()) != stats_cols.size())
            {
                LOG_TRACE(
                    log,
                    "Skipping min/max pruning for file {}: `_VALUE_STATS_COLS` lists {} column(s) but the "
                    "statistics rows have arity {}/{}",
                    file.file_name,
                    stats_cols.size(),
                    min_row.getArity(),
                    max_row.getArity());
                return false;
            }
            col_to_pos.reserve(stats_cols.size());
            for (size_t i = 0; i < stats_cols.size(); ++i)
                col_to_pos[stats_cols[i].safeGet<String>()] = static_cast<Int32>(i);
        }

        for (const auto & col_cond : column_conditions)
        {
            Int32 pos = -1;
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

            /// The min/max bounds only describe the non-null values of a column. If the file contains any null
            /// in this column, a predicate that matches NULL (e.g. `col IS NULL`) can still be satisfied even
            /// when the predicate is false everywhere in [min, max]. Only prune when the null count is known
            /// and equal to zero; otherwise skip pruning for this column to stay correct.
            if (pos >= static_cast<Int32>(null_counts.size()))
                continue;
            const auto & null_count = null_counts[pos];
            if (null_count.isNull() || DB::applyVisitor(DB::FieldVisitorConvertToNumber<Int64>(), null_count) != 0)
                continue;

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
