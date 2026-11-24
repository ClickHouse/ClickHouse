#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Returns the name of the materialized view or the `TimeSeries` table in `database` owning its inner table `table_name`,
/// or an empty string. The owner is found by its UUID or name in the inner table's name (see
/// `StorageMaterializedView::generateInnerTableName`, `getTimeSeriesInnerTableName`).
String tryGetOwnerTable(const DatabasePtr & database, const String & table_name, const ContextPtr & context);

}
