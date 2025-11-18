#pragma once
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/KeyDescription.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>


namespace Paimon 
{
    class PartitionPruner
    {
    public:
        PartitionPruner(const PaimonTableSchema & table_schema,
                        const DB::ActionsDAG & filter_dag,
                        DB::ContextPtr context);
        bool canBePruned(const PaimonManifestEntry & manifest_entry);
    private:
        const PaimonTableSchema & table_schema;
        std::optional<DB::KeyCondition> key_condition;
        DB::KeyDescription partition_key;
    };
}
