#pragma once

#include <Parsers/IAST.h>
#include <Core/Block.h>


namespace DB
{

class WriteBuffer;
class Context;
struct ExecuteTableFunctions;


/** For SELECT query, determine names and types of columns of result,
  *  and if some columns are constant expressions, calculate their values.
  *
  * NOTE It's possible to memoize calculations, that happens under the hood
  *  and could be duplicated in subsequent analysis of subqueries.
  */
struct AnalyzeResultOfQuery
{
    void process(ASTPtr & ast, const Context & context, ExecuteTableFunctions & table_functions);

    /// Block will have non-nullptr columns for constant expressions.
    Block result;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
