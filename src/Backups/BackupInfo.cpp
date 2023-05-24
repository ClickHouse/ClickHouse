#include <Backups/BackupInfo.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String BackupInfo::toString() const
{
    ASTPtr ast = toAST();
    return serializeAST(*ast);
}


BackupInfo BackupInfo::fromString(const String & str)
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    return fromAST(*ast);
}


ASTPtr BackupInfo::toAST() const
{
    auto func = std::make_shared<ASTFunction>();
    func->name = backup_engine_name;
    func->no_empty_args = true;

    auto list = std::make_shared<ASTExpressionList>();
    func->arguments = list;
    func->children.push_back(list);
    list->children.reserve(args.size() + !id_arg.empty());

    if (!id_arg.empty())
        list->children.push_back(std::make_shared<ASTIdentifier>(id_arg));

    for (const auto & arg : args)
        list->children.push_back(std::make_shared<ASTLiteral>(arg));

    return func;
}


BackupInfo BackupInfo::fromAST(const IAST & ast)
{
    const auto * func = ast.as<const ASTFunction>();
    if (!func)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected function, got {}", serializeAST(ast));

    BackupInfo res;
    res.backup_engine_name = func->name;

    if (func->arguments)
    {
        const auto * list = func->arguments->as<const ASTExpressionList>();
        if (!list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected list, got {}", serializeAST(*func->arguments));

        size_t index = 0;
        if (!list->children.empty())
        {
            const auto * id = list->children[0]->as<const ASTIdentifier>();
            if (id)
            {
                res.id_arg = id->name();
                ++index;
            }
        }

        res.args.reserve(list->children.size() - index);
        for (; index < list->children.size(); ++index)
        {
            const auto & elem = list->children[index];
            const auto * lit = elem->as<const ASTLiteral>();
            if (!lit)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected literal, got {}", serializeAST(*elem));
            res.args.push_back(lit->value);
        }
    }

    return res;
}


}
