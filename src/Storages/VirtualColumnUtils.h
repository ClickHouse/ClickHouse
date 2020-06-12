#pragma once

#include <set>

#include <Core/Block.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class Context;
class NamesAndTypesList;


namespace VirtualColumnUtils
{

/// Adds to the select query section `WITH value AS column_name`, and uses func
/// to wrap the value (if any)
///
/// For example:
/// - `WITH 9000 as _port`.
/// - `WITH toUInt16(9000) as _port`.
void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value, const String & func = "");

/// Leave in the block only the rows that fit under the WHERE clause and the PREWHERE clause of the query.
/// Only elements of the outer conjunction are considered, depending only on the columns present in the block.
/// Returns true if at least one row is discarded.
void filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context);

/// Extract from the input stream a set of `name` column values
template <typename T>
std::multiset<T> extractSingleValueFromBlock(const Block & block, const String & name)
{
    std::multiset<T> res;
    const ColumnWithTypeAndName & data = block.getByName(name);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        res.insert((*data.column)[i].get<T>());
    return res;
}

}

}
