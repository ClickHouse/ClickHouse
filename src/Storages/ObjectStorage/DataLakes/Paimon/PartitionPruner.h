#pragma once
#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>

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
        bool canBePruned(const DB::PaimonManifestEntry & manifest_entry);
    private:
        const DB::PaimonTableSchema & table_schema;
        std::optional<DB::KeyCondition> key_condition;
        DB::KeyDescription partition_key;
    };
}
#endif
