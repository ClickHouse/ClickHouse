#pragma once

#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>


namespace DB
{

class FunctionTreeNode : public AbstractFunction
{
public:
    class ArgumentTreeNode : public Argument
    {
    public:
        explicit ArgumentTreeNode(const IQueryTreeNode * argument_) : argument(argument_) {}
        std::unique_ptr<AbstractFunction> getFunction() const override
        {
            if (const auto * f = argument->as<FunctionNode>())
                return std::make_unique<FunctionTreeNode>(*f);
            return nullptr;
        }
        bool isIdentifier() const override { return argument->as<IdentifierNode>(); }
        bool tryGetString(String * res, bool allow_identifier) const override
        {
            if (const auto * literal = argument->as<ConstantNode>())
            {
                if (literal->getValue().getType() != Field::Types::String)
                    return false;
                if (res)
                    *res = literal->getValue().safeGet<String>();
                return true;
            }

            if (allow_identifier)
            {
                if (const auto * id = argument->as<IdentifierNode>())
                {
                    if (res)
                        *res = id->getIdentifier().getFullName();
                    return true;
                }
            }

            return false;
        }
    private:
        const IQueryTreeNode * argument = nullptr;
    };

    class ArgumentsTreeNode : public Arguments
    {
    public:
        explicit ArgumentsTreeNode(const QueryTreeNodes * arguments_) : arguments(arguments_) {}
        size_t size() const override { return arguments ? arguments->size() : 0; }
        std::unique_ptr<Argument> at(size_t n) const override { return std::make_unique<ArgumentTreeNode>(arguments->at(n).get()); }
    private:
        const QueryTreeNodes * arguments = nullptr;
    };

    explicit FunctionTreeNode(const FunctionNode & function_) : function(&function_)
    {
        if (const auto & nodes = function->getArguments().getNodes(); !nodes.empty())
            arguments = std::make_unique<ArgumentsTreeNode>(&nodes);
    }
    String name() const override { return function->getFunctionName(); }
private:
    const FunctionNode * function = nullptr;
};


/// Finds arguments of a specified function which should not be displayed for most users for security reasons.
/// That involves passwords and secret keys.
class FunctionSecretArgumentsFinderTreeNode : public FunctionSecretArgumentsFinder
{
public:
    explicit FunctionSecretArgumentsFinderTreeNode(const FunctionNode & function_)
        : FunctionSecretArgumentsFinder(std::make_unique<FunctionTreeNode>(function_))
    {
        if (!function->hasArguments())
            return;

        findOrdinaryFunctionSecretArguments();
    }

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }
};

}
