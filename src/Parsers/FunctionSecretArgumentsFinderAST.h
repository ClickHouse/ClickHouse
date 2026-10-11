#pragma once

#include <Common/FieldVisitorToString.h>
#include <Parsers/SecretArguments.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>


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
        bool isSettings() const override { return argument->as<ASTSetQuery>(); }
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
        bool tryGetLiteralText(String * res) const override
        {
            const auto * literal = argument->as<ASTLiteral>();
            if (!literal)
                return false;
            if (res)
                *res = applyVisitor(FieldVisitorToString(), literal->value);
            return true;
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

/// Whether the locator of `Backup(database_name, S3(...))` is tagged as `BACKUP_NAME` and therefore masks
/// itself with the `BACKUP ... TO` rule when it is formatted.
inline bool backupS3LocatorMasksItself(const ASTFunction & backup)
{
    if (!backup.arguments || backup.arguments->children.size() != 2 || !backup.arguments->children[0]->as<ASTLiteral>())
        return false;

    const auto * locator = backup.arguments->children[1]->as<ASTFunction>();
    return locator && locator->name == "S3" && locator->getKind() == ASTFunction::Kind::BACKUP_NAME
        && !locator->parameters && locator->tryGetAlias().empty() && !locator->isWindowFunction()
        && locator->getNullsAction() == NullsAction::EMPTY;
}

/// The secret arguments of a function as the installed finder sees them.
inline SecretArgumentsResult findSecretArguments(const ASTFunction & function)
{
    /// The `Backup` database rule is chosen by name rather than by kind, so that the kind tags of a JSON AST
    /// supplied by a client cannot bypass it. A tagged `S3` locator masks itself as `BACKUP_NAME`.
    if (function.name == "Backup" && function.arguments && !function.arguments->children.empty())
    {
        if (backupS3LocatorMasksItself(function))
            return {};
        return getSecretArgumentsFinder().find(ASTFunction::Kind::DATABASE_ENGINE, FunctionAST(function));
    }

    /// Codecs, statistics, window functions and lambdas take no secrets, so the programs that format only
    /// those (`keeper` and `compressor` format codecs) need no finder.
    switch (function.getKind())
    {
        case ASTFunction::Kind::ORDINARY_FUNCTION:
        case ASTFunction::Kind::TABLE_ENGINE:
        case ASTFunction::Kind::DATABASE_ENGINE:
        case ASTFunction::Kind::BACKUP_NAME:
            break;
        case ASTFunction::Kind::WINDOW_FUNCTION:
        case ASTFunction::Kind::LAMBDA_FUNCTION:
        case ASTFunction::Kind::CODEC:
        case ASTFunction::Kind::STATISTICS:
            return {};
    }
    return getSecretArgumentsFinder().find(function.getKind(), FunctionAST(function));
}

}
