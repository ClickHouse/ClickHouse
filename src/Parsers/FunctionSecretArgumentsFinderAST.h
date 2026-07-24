#pragma once

#include <Parsers/FunctionSecretArgumentsFinder.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

class FunctionAST : public AbstractFunction
{
public:
    class ArgumentAST : public Argument
    {
    public:
        explicit ArgumentAST(const IAST * argument_) : argument(argument_) {}
        std::unique_ptr<AbstractFunction> getFunction() const override
        {
            if (const auto * f = argument->as<ASTFunction>())
                return std::make_unique<FunctionAST>(*f);
            return nullptr;
        }
        bool isIdentifier() const override { return argument->as<ASTIdentifier>(); }
        bool tryGetString(String * res, bool allow_identifier) const override
        {
            if (const auto * literal = argument->as<ASTLiteral>())
            {
                if (literal->value.getType() != Field::Types::String)
                    return false;
                if (res)
                    *res = literal->value.safeGet<String>();
                return true;
            }

            if (allow_identifier)
            {
                if (const auto * id = argument->as<ASTIdentifier>())
                {
                    if (res)
                        *res = id->name();
                    return true;
                }
            }

            return false;
        }
    private:
        const IAST * argument = nullptr;
    };

    class ArgumentsAST : public Arguments
    {
    public:
        explicit ArgumentsAST(const ASTs * arguments_) : arguments(arguments_) {}
        size_t size() const override { return arguments ? arguments->size() : 0; }
        std::unique_ptr<Argument> at(size_t n) const override
        {
            return std::make_unique<ArgumentAST>(arguments->at(n).get());
        }
    private:
        const ASTs * arguments = nullptr;
    };

    explicit FunctionAST(const ASTFunction & function_) : function(&function_)
    {
        if (!function->arguments)
            return;

        const auto * expr_list = function->arguments->as<ASTExpressionList>();
        if (!expr_list)
            return;

        arguments = std::make_unique<ArgumentsAST>(&expr_list->children);
    }

    String name() const override { return function->name; }
private:
    const ASTFunction * function = nullptr;
};

/// Finds arguments of a specified function which should not be displayed for most users for security reasons.
/// That involves passwords and secret keys.
class FunctionSecretArgumentsFinderAST : public FunctionSecretArgumentsFinder
{
public:
    explicit FunctionSecretArgumentsFinderAST(const ASTFunction & function_)
        : FunctionSecretArgumentsFinder(std::make_unique<FunctionAST>(function_))
    {
        if (!function->hasArguments())
            return;

        switch (function_.kind)
        {
            case ASTFunction::Kind::ORDINARY_FUNCTION: findOrdinaryFunctionSecretArguments(); break;
            case ASTFunction::Kind::WINDOW_FUNCTION: break;
            case ASTFunction::Kind::LAMBDA_FUNCTION: break;
            case ASTFunction::Kind::CODEC: break;
            case ASTFunction::Kind::STATISTICS: break;
            case ASTFunction::Kind::TABLE_ENGINE: findTableEngineSecretArguments(); break;
            case ASTFunction::Kind::DATABASE_ENGINE: findDatabaseEngineSecretArguments(); break;
            case ASTFunction::Kind::BACKUP_NAME: findBackupNameSecretArguments(); break;
        }
    }

    FunctionSecretArgumentsFinder::Result getResult() const { return result; }
};


}
