#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
struct ExplainTextRewriteResult
{
    ASTPtr query;
    bool one_line{false};
};

/// clone `query` and apply `actions` from left to right, and return the
/// rewritten query with its formatting mode
ExplainTextRewriteResult rewriteExplainTextQuery(const ASTPtr & query, const ASTPtr & actions);

}
