#pragma once

#include "config.h"

#if USE_DELTA_KERNEL_RS

#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>

namespace DeltaLake
{

/// from_version or version_range.
using TableChangesVersionRange = std::pair<size_t, std::optional<size_t>>;

/// The change data feed of a Delta Lake table: the rows inserted, deleted and updated in a range of versions.
///
/// delta-kernel-rs only reads the log: it lists the data files that hold the changes and reads their
/// deletion vectors. The data files are read by ClickHouse, like the data files of a snapshot. Every file
/// comes with a transform which adds the partition values and the `_change_type`, `_commit_version` and
/// `_commit_timestamp` columns, and with the rows that its deletion vectors exclude or select.
class TableChanges
{
public:
    TableChanges(const TableChangesVersionRange & version_range_, KernelHelperPtr helper_);

    /// The table columns and the columns of the change data feed.
    DB::NamesAndTypesList getSchema() const;

    /// Iterates over the data files of the change data feed. `physical_names_map` is the column mapping of the table,
    /// as in the snapshot of the last version of the range. `filter_dag` is used to skip data files by their statistics.
    DB::ObjectIterator iterate(
        const DB::ActionsDAG * filter_dag,
        const DB::NameToNameMap & physical_names_map,
        DB::IDataLakeMetadata::FileProgressCallback callback,
        DB::ContextPtr context) const;

    static TableChangesVersionRange getVersionRange(int64_t start_version, int64_t end_version);

    /// The columns which the change data feed adds to the columns of the table.
    static bool isChangeDataFeedColumn(const std::string & name);

    static constexpr auto CHANGE_TYPE_COLUMN = "_change_type";

private:
    const TableChangesVersionRange version_range;
    const KernelHelperPtr helper;
    const LoggerPtr log;

    mutable std::mutex mutex;
    mutable std::optional<DB::NamesAndTypesList> schema;
};

using TableChangesPtr = std::shared_ptr<TableChanges>;

}

#endif
