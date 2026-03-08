#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTFunction;

/// Replaces chains of OR with {i}like/match to match() with combined regex or multiSearchAny.
///
/// For example:
///   x LIKE '%foo%' OR x LIKE '%bar%' --> multiSearchAny(x, ['foo', 'bar'])
///   x LIKE 'foo%' OR x LIKE '%bar' --> match(x, '(^foo)|(bar$)')
///   x LIKE '%foo%' OR x ILIKE '%bar%' --> match(x, '(foo)|((?i)bar)')
///   x LIKE '%foo%' OR match(x, 'bar.*') --> match(x, '(foo)|(bar.*)')
///
/// If all patterns are simple substring searches (%substring%) with the same
/// case sensitivity, uses the faster multiSearchAny/multiSearchAnyCaseInsensitive.
/// Otherwise, uses match() with a combined regexp pattern using alternation.
class ConvertFunctionOrLikeData
{
public:
    using TypeToVisit = ASTFunction;

    static void visit(ASTFunction & function, ASTPtr & ast);
};

using ConvertFunctionOrLikeVisitor = InDepthNodeVisitor<OneTypeMatcher<ConvertFunctionOrLikeData>, true>;

}
