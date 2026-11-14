#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts_columns' which allows to get information about
 * columns in projection parts for tables of MergeTree family.
 */
class StorageSystemProjectionPartsColumns final : public StorageSystemPartsBase
{
public:
    explicit StorageSystemProjectionPartsColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemProjectionPartsColumns"; }

protected:
    bool listsProjectionParts() const override { return true; }

    StorageProcessingStatePtr prepareStorage(ContextPtr context, const StoragesInfo & info, const std::vector<UInt8> & columns_mask) override;

    void processPart(
        ContextPtr context,
        MutableColumns & columns,
        const std::vector<UInt8> & columns_mask,
        const StoragesInfo & info,
        const MergeTreeData::DataPartPtr & part,
        MergeTreeData::DataPartState part_state,
        bool has_state_column,
        StorageProcessingState & storage_state) override;

private:
    struct ColumnsProcessingState;
};
}
