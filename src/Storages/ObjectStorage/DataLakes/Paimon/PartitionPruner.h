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
            Int32 schema_idx; /// Position in table_schema.fields (used for legacy mode without _VALUE_STATS_COLS)
            DB::KeyDescription key;
            std::unique_ptr<DB::KeyCondition> condition;
            DataType data_type;
        };

        std::vector<ColumnCondition> column_conditions;
        LoggerPtr log;
    };
}
#endif
