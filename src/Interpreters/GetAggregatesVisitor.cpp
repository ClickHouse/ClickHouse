#include <Interpreters/GetAggregatesVisitor.h>
#include <Common/checkStackSize.h>


namespace DB
{

struct WindowExpressionsCollectorChildInfo
{
    void update(const WindowExpressionsCollectorChildInfo & other)
    {
        window_function_in_subtree = window_function_in_subtree || other.window_function_in_subtree;
    }

    bool window_function_in_subtree = false;
};

// This visitor traverses the AST and collects the list of expressions which depend on
// evaluation of window functions. Expression is collected only if
// it's not a part of another expression.
//
// Information about window function dependency is used during ActionsDAG building process.
struct WindowExpressionsCollectorMatcher
{
    using ChildInfo = WindowExpressionsCollectorChildInfo;

    static bool needVisitChild(ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;

        if (auto * select = node->as<ASTSelectQuery>())
        {
            // We don't analyse the WITH statement because it might contain useless aggregates
            if (child == select->with())
                return false;
        }

        // We process every expression manually
        if (auto * /*func*/ _ = node->as<ASTFunction>())
            return false;

        return true;
    }

    WindowExpressionsCollectorChildInfo visitNode(
        ASTPtr & ast,
        const ASTPtr & parent,
        WindowExpressionsCollectorChildInfo const &)
    {
        return visitNode(ast, parent);
    }

    WindowExpressionsCollectorChildInfo visitNode(
        ASTPtr & ast,
        const ASTPtr & parent)
    {
        checkStackSize();

        if (auto * func = ast->as<ASTFunction>())
        {
            if (func->is_window_function)
                return { .window_function_in_subtree = true };

            WindowExpressionsCollectorChildInfo result;
            for (auto & arg : func->arguments->children)
            {
                auto subtree_result = visitNode(arg, ast);
                result.update(subtree_result);
            }

            // We mark functions if they should be computed after WindowStep
            if (result.window_function_in_subtree)
            {
                func->compute_after_window_functions = true;
                if ((!parent || !parent->as<ASTFunction>()))
                    expressions_with_window_functions.push_back(ast);
            }

            return result;
        }
        return {};
    }

    ASTs expressions_with_window_functions;
};

using WindowExpressionsCollectorVisitor = InDepthNodeVisitorWithChildInfo<WindowExpressionsCollectorMatcher>;

ASTs getExpressionsWithWindowFunctions(ASTPtr & ast)
{
    WindowExpressionsCollectorVisitor visitor;
    visitor.visit(ast);

    return std::move(visitor.expressions_with_window_functions);
}

}
