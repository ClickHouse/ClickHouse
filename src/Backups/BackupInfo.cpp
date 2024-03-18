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
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return fromAST(*ast);
}


ASTPtr BackupInfo::toAST() const
{
    auto func = std::make_shared<ASTFunction>();
    func->name = backup_engine_name;
    func->no_empty_args = true;
    func->kind = ASTFunction::Kind::BACKUP_NAME;

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

        size_t args_size = list->children.size();
        res.args.reserve(args_size - index);
        for (; index < args_size; ++index)
        {
            const auto & elem = list->children[index];
            const auto * lit = elem->as<const ASTLiteral>();
            if (!lit)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected literal, got {}", serializeAST(*elem));
            }
            res.args.push_back(lit->value);
        }
    }

    return res;
}


String BackupInfo::toStringForLogging() const
{
    return toAST()->formatForLogging();
}

void BackupInfo::copyS3CredentialsTo(BackupInfo & dest) const
{
    /// named_collection case, no need to update
    if (!dest.id_arg.empty() || !id_arg.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup is not compatible with named_collections");

    if (backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", toStringForLogging());
    if (dest.backup_engine_name != "S3")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup supported only for S3, got {}", dest.toStringForLogging());
    if (args.size() != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "use_same_s3_credentials_for_base_backup requires access_key_id, secret_access_key, got {}", toStringForLogging());

    auto & dest_args = dest.args;
    dest_args.resize(3);
    dest_args[1] = args[1];
    dest_args[2] = args[2];
}

}
