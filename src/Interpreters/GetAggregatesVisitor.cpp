#include <Interpreters/GetAggregatesVisitor.h>

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

// This visitor travers AST and collects the list of expressions which depend on
// evaluation of window functions. Expression is collected only if
// it's not a part of another expression.
//
// Also all collected AST nodes are maked as dependent on window function.
// This information is used during ActionsDAG building process.
struct WindowExpressionsCollectorMatcher
{
    using ChildInfo = WindowExpressionsCollectorChildInfo;

    static bool needVisitChild(ASTPtr & node, const ASTPtr & child)
    {
        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;
        if (auto * select = node->as<ASTSelectQuery>())
        {
            // We don't analysis WITH statement because it might contain useless aggregates
            if (child == select->with())
                return false;
        }
        // We procces every expression manually
        if (auto * func = node->as<ASTFunction>())
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
        if (auto * func = ast->as<ASTFunction>())
        {
            if (func->is_window_function)
                return { .window_function_in_subtree = true };

            bool window_function_in_subtree = false;
            for (auto & arg : func->arguments->children)
            {
                auto subtree_result = visitNode(arg, ast);
                window_function_in_subtree = window_function_in_subtree || subtree_result.window_function_in_subtree;
            }

            // We mark functions only on the top of AST
            if ((!parent || !parent->as<ASTFunction>()) && window_function_in_subtree)
            {
                expressions_with_window_functions.push_back(func);
                func->compute_after_window_functions = true;
            }

            return { .window_function_in_subtree = window_function_in_subtree };
        }
        return {};
    }

    std::vector<const ASTFunction *> expressions_with_window_functions {};
};

using WindowExpressionsCollectorVisitor = InDepthNodeVisitorWithChildInfo<WindowExpressionsCollectorMatcher>;

std::vector<const ASTFunction *> getExpressionsWithWindowFunctions(ASTPtr & ast)
{
    WindowExpressionsCollectorVisitor visitor;
    visitor.visit(ast);
    return std::move(visitor.expressions_with_window_functions);
}

}
