#include <Parsers/stripArtificialParens.h>

#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/IAST.h>

namespace DB
{

void stripArtificialParens(IAST & ast)
{
    ast.setParenthesized(false);

    for (const auto & child : ast.children)
        if (child)
            stripArtificialParens(*child);

    /// `group_by_key`, `group_by_assignments` and `recompression_codec` of a TTL element are not
    /// stored in `children`, so the walk above does not reach them.
    if (auto * ttl_element = ast.as<ASTTTLElement>())
    {
        for (const auto & expr : ttl_element->group_by_key)
            if (expr)
                stripArtificialParens(*expr);
        for (const auto & expr : ttl_element->group_by_assignments)
            if (expr)
                stripArtificialParens(*expr);
        if (ttl_element->recompression_codec)
            stripArtificialParens(*ttl_element->recompression_codec);
    }

    /// Same for the `parameters` and `lambda` of a projection's `APPLY` transformer.
    if (auto * apply_transformer = ast.as<ASTColumnsApplyTransformer>())
    {
        if (apply_transformer->parameters)
            stripArtificialParens(*apply_transformer->parameters);
        if (apply_transformer->lambda)
            stripArtificialParens(*apply_transformer->lambda);
    }
}

}
