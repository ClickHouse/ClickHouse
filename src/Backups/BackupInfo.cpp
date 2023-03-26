#include <Backups/BackupInfo.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
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
    auto func = std::make_shared<ASTFunction>();
    func->name = backup_engine_name;
    func->no_empty_args = true;

    auto list = std::make_shared<ASTExpressionList>();
    func->arguments = list;
    func->children.push_back(list);
    list->children.reserve(args.size());
    for (const auto & arg : args)
        list->children.push_back(std::make_shared<ASTLiteral>(arg));

    return serializeAST(*func);
}


BackupInfo BackupInfo::fromString(const String & str)
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    return fromAST(*ast);
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
        res.args.reserve(list->children.size());
        for (const auto & elem : list->children)
        {
            const auto * lit = elem->as<const ASTLiteral>();
            if (!lit)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected literal, got {}", serializeAST(*elem));
            res.args.push_back(lit->value);
        }
    }

    return res;
}


}
