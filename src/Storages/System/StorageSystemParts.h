#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts : public StorageSystemPartsBase
{
public:
    explicit StorageSystemParts(const StorageID & table_id_);

    std::string getName() const override { return "SystemParts"; }

protected:
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
