#pragma once


namespace DB
{
class ASTCreateQuery;

/// Whether the data of the first table can be inserted to the second table.
bool hasCompatibleDataToRestoreTable(const ASTCreateQuery & query1, const ASTCreateQuery & query2);

}
