#include <unordered_map>
#include <Analyzer/createUniqueTableAliases.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

namespace
{

class CreateUniqueTableAliasesVisitor : public InDepthQueryTreeVisitorWithContext<CreateUniqueTableAliasesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CreateUniqueTableAliasesVisitor>;

    explicit CreateUniqueTableAliasesVisitor(const ContextPtr & context)
        : Base(context)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        switch (node->getNodeType())
        {
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                if (getSubqueryDepth() == 1)
                    break;
                [[fallthrough]];
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
                [[fallthrough]];
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & alias = table_expression_to_alias[node];
                if (alias.empty())
                {
                    alias = fmt::format("__table{}", table_expression_to_alias.size());
                    node->setAlias(alias);
                }
                break;
            }
            default:
                break;
        }
    }
private:
    // We need to use raw pointer as a key, not a QueryTreeNodePtrWithHash.
    std::unordered_map<QueryTreeNodePtr, String> table_expression_to_alias;
};

}


void createUniqueTableAliases(QueryTreeNodePtr & node, const ContextPtr & context)
{
    CreateUniqueTableAliasesVisitor(context).visit(node);
}

}
