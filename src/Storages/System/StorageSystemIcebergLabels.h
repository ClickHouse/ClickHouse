#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements the `system.iceberg_labels` table: the labels which Iceberg REST catalogs attach to the tables
  * of the `DataLakeCatalog` databases, one row per label.
  *
  * Labels are catalog-owned metadata enrichment (ownership, classification, cost attribution, ...) which a catalog
  * returns with every table load. They are not a part of the table metadata, so they are asked from the catalog
  * on every query to this table and are never cached: one request to the catalog per table. A filter on
  * `database` and `table` is used to ask only for the tables the query needs.
  */
class StorageSystemIcebergLabels final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergLabels"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(
        [[maybe_unused]] MutableColumns & res_columns,
        [[maybe_unused]] ContextPtr context,
        [[maybe_unused]] const ActionsDAG::Node * predicate,
        std::vector<UInt8>) const override;

    Block getFilterSampleBlock() const override;
};

}
