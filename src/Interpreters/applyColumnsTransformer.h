#pragma once

#include <Parsers/IAST_fwd.h>

#include <memory>

namespace re2
{
    class RE2;
}

namespace DB
{

class ASTColumnsExceptTransformer;

/** Applying a `COLUMNS(...)` transformer to a list of columns.
  *
  * This is name resolution, not parsing: it needs the list of columns the matcher expanded to,
  * which only exists once the query is being analysed. It lives outside `src/Parsers` so that the
  * AST nodes stay pure syntax - in particular so that the parser does not depend on re2, which
  * only `EXCEPT('regexp')` needs.
  */
void applyColumnsTransformer(const ASTPtr & transformer, ASTs & nodes);

/// Compile the pattern of `EXCEPT('regexp')`. Throws if the pattern does not compile.
/// Returns nullptr when the transformer lists column names instead of a pattern.
std::shared_ptr<re2::RE2> getColumnsExceptMatcher(const ASTColumnsExceptTransformer & transformer);

}
