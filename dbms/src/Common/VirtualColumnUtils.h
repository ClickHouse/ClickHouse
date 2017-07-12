#pragma once

#include <set>

#include <Core/Block.h>
#include <Parsers/IAST.h>


namespace DB
{

class Context;
class NamesAndTypesList;


namespace VirtualColumnUtils
{

/// Calculate the minimum numeric suffix to add to the string so that it is not present in the set
String chooseSuffix(const NamesAndTypesList & columns, const String & name);

/// Calculate the minimum total numeric suffix to add to each string,
/// so that none is present in the set.
String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names);

/// Adds to the select query section `select column_name as value`
/// For example select _port as 9000.
void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field & value);

/// Leave in the block only the rows that fit under the WHERE clause and the PREWHERE clause of the query.
/// Only elements of the outer conjunction are considered, depending only on the columns present in the block.
/// Returns true if at least one row is discarded.
bool filterBlockWithQuery(const ASTPtr & query, Block & block, const Context & context);

/// Extract from the input stream a set of `name` column values
template <typename T1>
std::multiset<T1> extractSingleValueFromBlock(const Block & block, const String & name)
{
    std::multiset<T1> res;
    const ColumnWithTypeAndName & data = block.getByName(name);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
        res.insert((*data.column)[i].get<T1>());
    return res;
}

}

}
