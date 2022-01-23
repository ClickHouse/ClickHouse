#pragma once

#include <base/types.h>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Outputs either "table db_name.table_name" or "temporary table table_name".
String formatTableNameOrTemporaryTableName(const DatabaseAndTableName & table_name);

}
