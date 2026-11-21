#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

class ColumnsDescription;

/// Columns of `columns` that `ast` reads, resolved by the query analyzer over a fake table. Subcolumns
/// are reported as columns of their own (`t.a`, `json.a`), an ALIAS column as the columns its expression
/// reads.
NamesAndTypes expressionSourceColumns(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context);

/// Their names, so a caller can look each of them up to compare types.
Names expressionSourceColumnNames(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context);

/// Only the columns they are stored in (`json.a` -> `json`), to compare with the columns a mutation or an
/// ALTER tracks.
Names expressionSourceColumnsInStorage(const ASTPtr & ast, const ColumnsDescription & columns, const ContextPtr & context);

}
