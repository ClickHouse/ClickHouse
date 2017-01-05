#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Core/NamesAndTypes.h>


namespace DB
{

class WriteBuffer;
class Context;


/** For SELECT query, determine names and types of columns of result.
  *
  * NOTE It's possible to memoize calculations, that happens under the hood
  *  and could be duplicated in subsequent analysis of subqueries.
  */
struct AnalyzeResultOfQuery
{
	void process(ASTPtr & ast, Context & context);

	NamesAndTypes result;

	/// Debug output
	void dump(WriteBuffer & out) const;
};

}
