#include <Interpreters/ConvertFunctionOrHasAnyVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/likePatternToRegexp.h>
#include <Common/typeid_cast.h>

namespace DB
{

/**
 * Optimizes chains of OR-ed hasAny calls by merging constant arrays.
 * Example: hasAny(col,[1,2]) OR hasAny(col,[3,4]) -> hasAny(col,[1,2,3,4])
 *
 * The optimization is applied only when:
 * - All hasAny calls reference the same column
 * - All patterns are constant arrays
 */
void ConvertFunctionOrHasAnyData::visit(ASTFunction & function, ASTPtr &)
{
    if (function.name != "or")
        return;

    std::unordered_map<String, std::shared_ptr<ASTLiteral>> identifier_to_patterns;
    for (auto & child : function.children)
    {
        if (auto * expr_list_fn = child->as<ASTExpressionList>())
        {
            ASTs unique_elems;
            for (const auto & child_expr_fn : expr_list_fn->children)
            {
                unique_elems.push_back(child_expr_fn);
                if (const auto * child_fn = child_expr_fn->as<ASTFunction>())
                {
                    const bool is_hasany = child_fn->name == "hasAny";

                    /// Not hasAny -> bail out.
                    if (!is_hasany)
                        continue;

                    const auto & arguments = child_fn->arguments->children;

                    /// They should have 2 arguments.
                    if (arguments.size() != 2)
                        continue;

                    auto identifier = arguments[0];
                    auto * pattern = arguments[1]->as<ASTLiteral>();
                    if (!identifier || !pattern || pattern->value.getType() != Field::Types::Array)
                        continue;

                    Array pattern_array = pattern->value.safeGet<Array>();

                    unique_elems.pop_back();
                    auto it = identifier_to_patterns.find(identifier->getAliasOrColumnName());

                    if (it == std::end(identifier_to_patterns))
                    {
                        it = identifier_to_patterns
                                 .insert({identifier->getAliasOrColumnName(), std::make_shared<ASTLiteral>(Field{Array{}})})
                                 .first;
                        auto match = makeASTFunction("hasAny");
                        match->arguments->children.push_back(arguments[0]);
                        match->arguments->children.push_back(it->second);
                        unique_elems.push_back(std::move(match));
                    }
                    auto & combined_array = it->second->value.safeGet<Array>();
                    combined_array.insert(std::end(combined_array), std::begin(pattern_array), std::end(pattern_array));
                    std::sort(std::begin(combined_array), std::end(combined_array));
                    combined_array.erase(std::unique(std::begin(combined_array), std::end(combined_array)), std::end(combined_array));
                }
            }

            /// OR must have at least two arguments.
            if (unique_elems.size() == 1)
                unique_elems.push_back(std::make_shared<ASTLiteral>(Field(false)));

            expr_list_fn->children = std::move(unique_elems);
        }
    }
}

}
