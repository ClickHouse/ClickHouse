#pragma once

#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/IdentifierNode.h>


namespace
{

template <typename FunctionNodeType>
String getFunctionNameImpl(const FunctionNodeType *);

template <>
[[maybe_unused]] String getFunctionNameImpl<DB::FunctionNode>(const DB::FunctionNode * function)
{
    return function->getFunctionName();
}

template <>
[[maybe_unused]] String getFunctionNameImpl<DB::TableFunctionNode>(const DB::TableFunctionNode * function)
{
    return function->getTableFunctionName();
}

template <typename FunctionNodeType>
class FunctionTreeNodeImpl : public DB::AbstractFunction
{
public:
    class ArgumentTreeNode : public Argument
    {
    public:
        explicit ArgumentTreeNode(const DB::IQueryTreeNode * argument_) : argument(argument_) {}
        std::unique_ptr<AbstractFunction> getFunction() const override
        {
            if (const auto * f = argument->as<FunctionNodeType>())
                return std::make_unique<FunctionTreeNodeImpl>(*f);
            return nullptr;
        }
        bool isIdentifier() const override { return argument->as<DB::IdentifierNode>(); }
        bool tryGetString(String * res, bool allow_identifier) const override
        {
            if (const auto * literal = argument->as<DB::ConstantNode>())
            {
                if (literal->getValue().getType() != DB::Field::Types::String)
                    return false;
                if (res)
                    *res = literal->getValue().safeGet<String>();
                return true;
            }

            if (allow_identifier)
            {
                if (const auto * id = argument->as<DB::IdentifierNode>())
                {
                    if (res)
                        *res = id->getIdentifier().getFullName();
                    return true;
                }
            }

            return false;
        }
    private:
        const DB::IQueryTreeNode * argument = nullptr;
    };

    class ArgumentsTreeNode : public Arguments
    {
    public:
        explicit ArgumentsTreeNode(const DB::QueryTreeNodes * arguments_) : arguments(arguments_) {}
        size_t size() const override { return arguments ? arguments->size() : 0; }
        std::unique_ptr<Argument> at(size_t n) const override { return std::make_unique<ArgumentTreeNode>(arguments->at(n).get()); }
    private:
        const DB::QueryTreeNodes * arguments = nullptr;
    };

    explicit FunctionTreeNodeImpl(const FunctionNodeType & function_) : function(&function_)
    {
        if (const auto & nodes = function->getArguments().getNodes(); !nodes.empty())
            arguments = std::make_unique<ArgumentsTreeNode>(&nodes);
    }
    String name() const override { return getFunctionNameImpl(function); }
private:
    const FunctionNodeType * function = nullptr;
};

/// Finds arguments of a specified function which should not be displayed for most users for security reasons.
/// That involves passwords and secret keys.
template <typename FunctionNodeType>
class FunctionSecretArgumentsFinderTreeNodeImpl : public DB::FunctionSecretArgumentsFinder
{
public:
    explicit FunctionSecretArgumentsFinderTreeNodeImpl(const FunctionNodeType & function_)
        : FunctionSecretArgumentsFinder(std::make_unique<FunctionTreeNodeImpl<FunctionNodeType>>(function_))
    {
        if (!function->hasArguments())
            return;

        findOrdinaryFunctionSecretArguments();
    }

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }
};

}


namespace DB
{

using FunctionSecretArgumentsFinderTreeNode = FunctionSecretArgumentsFinderTreeNodeImpl<FunctionNode>;
using TableFunctionSecretArgumentsFinderTreeNode = FunctionSecretArgumentsFinderTreeNodeImpl<TableFunctionNode>;

}
