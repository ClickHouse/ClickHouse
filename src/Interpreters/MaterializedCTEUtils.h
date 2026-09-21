#pragma once

namespace DB
{

class IAST;

/// True when any `WITH` element of the query (at any depth) is declared `MATERIALIZED`.
bool hasMaterializedCTE(const IAST & ast);

/// Clears the `MATERIALIZED` flag of every `WITH` element: for pipelines that cannot materialize a CTE
/// and inline it instead, as they always did.
void treatMaterializedCTEsAsPlain(IAST & ast);

}
