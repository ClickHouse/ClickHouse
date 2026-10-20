#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts_columns' which allows to get information about
 * columns in data parts for tables of MergeTree family.
 */
class StorageSystemPartsColumns final : public StorageSystemPartsBase
{
public:
    explicit StorageSystemPartsColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemPartsColumns"; }

protected:
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
