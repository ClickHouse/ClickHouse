#pragma once

#include <base/types.h>

#include <vector>


namespace DB
{

/// The table column that a column of a query result is a plain reference to.
/// All fields are empty if the result column is not a plain reference to a table column (e.g. it is an expression).
struct ResultColumnOrigin
{
    /// Database of the table. Empty for temporary tables.
    String database;
    /// Name of the table as written in the query: its alias if it has one, otherwise the table name.
    String table;
    /// Actual name of the table.
    String org_table;
    /// Name of the column in the table.
    String column;
};

using ResultColumnOrigins = std::vector<ResultColumnOrigin>;

}
