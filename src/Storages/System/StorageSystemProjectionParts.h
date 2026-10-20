#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts' which allows to get information about projection parts for tables of MergeTree family.
  */
class StorageSystemProjectionParts final : public StorageSystemPartsBase
{
public:
    explicit StorageSystemProjectionParts(const StorageID & table_id_);

    std::string getName() const override { return "SystemProjectionParts"; }

protected:
    bool listsProjectionParts() const override { return true; }

    void processPart(
        ContextPtr context,
        MutableColumns & columns,
        const std::vector<UInt8> & columns_mask,
        const StoragesInfo & info,
        const MergeTreeData::DataPartPtr & part,
        MergeTreeData::DataPartState part_state,
        bool has_state_column,
        StorageProcessingState & storage_state) override;
};
}
