#pragma once
#include <config.h>

#if USE_AVRO

#include <memory>
#include <vector>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>
#include <Common/Logger.h>

namespace DB
{
struct PaimonManifestEntry;
}

namespace Paimon
{
    /// Whether a legacy value-stats row - one written without the `_VALUE_STATS_COLS` column list - may be
    /// indexed by table-schema field position.
    ///
    /// A legacy row carries no column names, so the only mapping available is "stats position i describes
    /// schema field i". That mapping holds when the row covers the whole table schema, and breaks when the
    /// file was written with a projected write schema (for example write columns `[f0, f2]` of a table whose
    /// schema is `[f0, f1, f2]`): a predicate on `f1` would then read `f2`'s bounds and could prune a file
    /// that still contains matching rows. A projection is a subset of the schema, so it is detectable by its
    /// cardinality - a stats row that covers every field must be the full schema, in schema order.
    ///
    /// `stats_arity` is the arity encoded in the `BinaryRow` header of the min (or max) row, and
    /// `null_counts_size` is the length of the parallel `_NULL_COUNTS` array; both must agree with the
    /// schema, otherwise the layout is unknown and pruning has to be skipped (the file is then read in
    /// full, which is always correct).
    bool legacyValueStatsArePositional(Int32 stats_arity, size_t null_counts_size, size_t schema_field_count);

    class PartitionPruner
    {
    public:
        PartitionPruner(const DB::PaimonTableSchema & table_schema,
                        const DB::ActionsDAG & filter_dag,
                        DB::ContextPtr context);
        bool canBePruned(const DB::PaimonManifestEntry & manifest_entry) const;
    private:
        const DB::PaimonTableSchema & table_schema;
        std::optional<DB::KeyCondition> key_condition;
        DB::KeyDescription partition_key;
    };

    class MinMaxIndexPruner
    {
    public:
        MinMaxIndexPruner(
            const DB::PaimonTableSchema & table_schema,
            const DB::ActionsDAG & filter_dag,
            DB::ContextPtr context);

        /// Returns true if the file can be safely pruned (filter condition is guaranteed false for all rows)
        bool canBePruned(const DB::PaimonManifestEntry & manifest_entry) const;

    private:
        struct ColumnCondition
        {
            String column_name;
            Int32 schema_idx = -1; /// Position in table_schema.fields (used for legacy mode without _VALUE_STATS_COLS)
            DB::KeyDescription key;
            std::unique_ptr<DB::KeyCondition> condition;
            DataType data_type;
        };

        std::vector<ColumnCondition> column_conditions;
        /// Id of the schema this pruner was built from. Legacy (positional) value stats are encoded in the
        /// data file's own schema field order, so positional pruning is only valid for files written with
        /// this same schema (see `canBePruned`).
        Int64 schema_id = -1;
        /// Number of fields in the schema this pruner was built from, used to validate the layout of legacy
        /// (positional) value stats - see `legacyValueStatsArePositional`.
        size_t schema_field_count = 0;
        LoggerPtr log;
    };
}
#endif
