#include <Analyzer/Passes/CNF.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Functions/FunctionFactory.h>

#include <Common/checkStackSize.h>

namespace DB::Analyzer
{

namespace
{

bool isLogicalFunction(const FunctionNode & function_node)
{
    const std::string_view name = function_node.getFunctionName();
    return name == "and" || name == "or" || name == "not";
}

class SplitMultiLogicVisitor : public InDepthQueryTreeVisitorWithContext<SplitMultiLogicVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<SplitMultiLogicVisitor>;

    explicit SplitMultiLogicVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        auto * function_node = parent->as<FunctionNode>();

        if (!function_node)
            return false;

        return isLogicalFunction(*function_node);
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isLogicalFunction(*function_node))
        {
            ++atom_count;
            return;
        }

        const auto & name = function_node->getFunctionName();

        if (name == "and" || name == "or")
        {
            auto function_resolver = FunctionFactory::instance().get(name, getContext());

            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() > 2)
            {
                QueryTreeNodePtr current = arguments[0];
                for (size_t i = 1; i < arguments.size(); ++i)
                {
                    QueryTreeNodes new_arguments;
                    new_arguments.reserve(2);
                    new_arguments.push_back(std::move(current));
                    new_arguments.push_back(arguments[i]);
                    auto new_function_node = std::make_shared<FunctionNode>();
                    new_function_node->getArguments().getNodes() = std::move(new_arguments);
                    new_function_node->resolveAsFunction(function_resolver);
                    current = std::move(new_function_node);
                }

                auto & new_function_node = current->as<FunctionNode &>();
                function_node->getArguments().getNodes() = std::move(new_function_node.getArguments().getNodes());
                function_node->resolveAsFunction(function_resolver);
            }
        }
        else
        {
            assert(name == "not");
        }
    }

    size_t atom_count = 0;
};

}

bool CNF::AtomicFormula::operator==(const AtomicFormula & rhs) const
{
    return negative == rhs.negative && node_with_hash == rhs.node_with_hash;
}

std::string CNF::dump() const
{
    WriteBufferFromOwnString res;
    bool first = true;
    for (const auto & group : statements)
    {
        if (!first)
            res << " AND ";
        first = false;
        res << "(";
        bool first_in_group = true;
        for (const auto & atom : group)
        {
            if (!first_in_group)
                res << " OR ";
            first_in_group = false;
            if (atom.negative)
                res << " NOT ";
            res << atom.node_with_hash.node->formatASTForErrorMessage();
        }
        res << ")";
    }

    return res.str();
}

std::optional<CNF> CNF::tryBuildCNF(const QueryTreeNodePtr & node, ContextPtr context, size_t)
{
    auto node_cloned = node->clone();
    SplitMultiLogicVisitor split_visitor(std::move(context));
    split_visitor.visit(node_cloned);
//    size_t num_atoms = countAtoms(node);
    return std::nullopt;
}

}
