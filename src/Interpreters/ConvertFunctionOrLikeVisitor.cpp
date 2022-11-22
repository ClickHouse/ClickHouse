#include <Functions/likePatternToRegexp.h>
#include <Interpreters/ConvertFunctionOrLikeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>


namespace DB
{

void ConvertFunctionOrLikeData::visit(ASTFunction & function, ASTPtr &)
{
    if (function.name != "or")
        return;

    std::unordered_map<ASTPtr, std::shared_ptr<ASTLiteral>> identifier_to_literals;
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
                    const bool is_like = child_fn->name == "like";
                    const bool is_ilike = child_fn->name == "ilike";

                    /// Not {i}like -> bail out.
                    if (!is_like && !is_ilike)
                        continue;

                    const auto & arguments = child_fn->arguments->children;

                    /// They should have 2 arguments.
                    if (arguments.size() != 2)
                        continue;

                    /// Second one is string literal.
                    auto identifier = arguments[0];
                    auto * literal = arguments[1]->as<ASTLiteral>();
                    if (!identifier || !literal || literal->value.getType() != Field::Types::String)
                        continue;

                    String regexp = likePatternToRegexp(literal->value.get<String>());
                    /// Case insensitive. Works with UTF-8 as well.
                    if (is_ilike)
                        regexp = "(?i)" + regexp;

                    unique_elems.pop_back();
                    auto it = identifier_to_literals.find(identifier);
                    if (it == identifier_to_literals.end())
                    {
                        it = identifier_to_literals.insert({identifier, std::make_shared<ASTLiteral>(Field{Array{}})}).first;
                        auto match = makeASTFunction("multiMatchAny");
                        match->arguments->children.push_back(arguments[0]);
                        match->arguments->children.push_back(it->second);
                        unique_elems.push_back(std::move(match));
                    }
                    it->second->value.get<Array>().push_back(regexp);
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
