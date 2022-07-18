#pragma once


namespace DB
{
class IAST;

/// Checks that two table definitions are actually the same.
bool areTableDefinitionsSame(const IAST & table1, const IAST & table2);

/// Checks that two database definitions are actually the same.
bool areDatabaseDefinitionsSame(const IAST & database1, const IAST & database2);

/// Whether the data from the first table can be attached to the second table.
bool areTableDataCompatible(const IAST & src_table, const IAST & dest_table);

}
