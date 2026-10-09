#pragma once

#include <base/types.h>


namespace DB
{
class ASTCreateQuery;

/// Returns the value of `version` from the SETTINGS clause of a CREATE TABLE ... ENGINE=TimeSeries query,
/// or the latest version if the query doesn't specify it (the normalization pins an explicit version
/// into every query, so an absent setting means a new table getting the latest version).
/// The function works on the AST only (without the TimeSeriesSettings class), so it can be used while formatting the query.
UInt64 getTimeSeriesSettingVersion(const ASTCreateQuery & query);

}
