#pragma once
#include <Columns/IColumnLazyHelper.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

using AliasToName = std::unordered_map<std::string, std::string>;
using AliasToNamePtr = std::shared_ptr<AliasToName>;

class MergeTreeLazilyReader : public IColumnLazyHelper
{
public:
    MergeTreeLazilyReader(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const LazilyReadInfoPtr & lazily_read_info_,
        const ContextPtr & context_,
        const AliasToNamePtr & alias_index_);

    void transformLazyColumns(
        const ColumnLazy & column_lazy,
        ColumnsWithTypeAndName & res_columns) override;

private:
    const MergeTreeData & storage;
    DataPartsInfoPtr data_parts_info;
    StorageSnapshotPtr storage_snapshot;
    bool use_uncompressed_cache;
    Names requested_column_names;
    ColumnsWithTypeAndName lazy_columns;
};

}
