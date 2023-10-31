#pragma once

#include <Storages/IStorage.h>
#include "Core/NamesAndTypes.h"
#include "Core/QueryProcessingStage.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/StorageID.h"
#include "Storages/SelectQueryInfo.h"
#include "Storages/StorageSnapshot.h"

namespace DB
{

class Context;

class StorageSystemRpmallocStats final : public IStorage
{
public:
    explicit StorageSystemRpmallocStats(const StorageID & table_id_);

    std::string getName() const override { return "SystemRpmallocBins"; }

    static NamesAndTypesList getNamesAndTypes();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }
};

}
