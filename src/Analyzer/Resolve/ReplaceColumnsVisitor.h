#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeFunction.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Used to replace columns that changed type because of JOIN to their original type
class ReplaceColumnsVisitor : public InDepthQueryTreeVisitor<ReplaceColumnsVisitor>
{
public:
    explicit ReplaceColumnsVisitor(const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_, const ContextPtr & context_)
        : replacement_map(replacement_map_)
        , context(context_)
    {}

    /// Apply replacement transitively, because column may change it's type twice, one to have a supertype and then because of `joun_use_nulls`
    static QueryTreeNodePtr findTransitiveReplacement(QueryTreeNodePtr node, const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_)
    {
        auto it = replacement_map_.find(node);
        QueryTreeNodePtr result_node = nullptr;
        for (; it != replacement_map_.end(); it = replacement_map_.find(result_node))
        {
            if (result_node && result_node->isEqual(*it->second))
            {
                Strings map_dump;
                for (const auto & [k, v]: replacement_map_)
                    map_dump.push_back(fmt::format("{} -> {} (is_equals: {}, is_same: {})",
                        k.node->dumpTree(), v->dumpTree(), k.node->isEqual(*v), k.node == v));
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Infinite loop in query tree replacement map: {}", fmt::join(map_dump, "; "));
            }
            chassert(it->second);

            result_node = it->second;
        }
        return result_node;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto replacement_node = findTransitiveReplacement(node, replacement_map))
            node = replacement_node;

        if (auto * function_node = node->as<FunctionNode>(); function_node && function_node->isResolved())
        {
            rerunFunctionResolve(function_node, context);
        }
        else if (auto * lambda_node = node->as<LambdaNode>())
        {
            /// The lambda caches its own result type, so replacing a column inside its body leaves that
            /// cache stale and the mismatch resurfaces later as a header type mismatch. Recompute it from
            /// the rewritten body, keeping the argument types.
            const auto * lambda_type = typeid_cast<const DataTypeFunction *>(lambda_node->getResultType().get());
            if (!lambda_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Lambda node {} result type is not a function type",
                    lambda_node->formatASTForErrorMessage());

            lambda_node->resolve(std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(), lambda_node->getExpression()->getResultType()));
        }
    }

    /// We want to re-run resolve for function _after_ its arguments are replaced
    bool shouldTraverseTopToBottom() const { return false; }

    bool needChildVisit(QueryTreeNodePtr & /* parent */, QueryTreeNodePtr & child)
    {
        /// Visit only expressions, but not subqueries.
        /// LAMBDA is an expression too: a matcher inside a lambda body (`arrayMap(x -> *, ...)`) is
        /// rewritten by the same JOIN type corrections as one outside it, so the replacement must reach
        /// it, otherwise the registered entry is never applied.
        return child->getNodeType() == QueryTreeNodeType::IDENTIFIER
            || child->getNodeType() == QueryTreeNodeType::LIST
            || child->getNodeType() == QueryTreeNodeType::FUNCTION
            || child->getNodeType() == QueryTreeNodeType::COLUMN
            || child->getNodeType() == QueryTreeNodeType::LAMBDA;
    }

private:
    const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map;
    const ContextPtr & context;
};

}
