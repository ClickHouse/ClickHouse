#include <Backups/BackupInfo.h>

#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/NamedCollectionsHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String BackupInfo::toString() const
{
    ASTPtr ast = toAST();
    return ast->formatWithSecretsOneLine();
}


BackupInfo BackupInfo::fromString(const String & str)
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return fromAST(*ast);
}


namespace
{
    /// Check if an AST node is a key-value assignment (e.g., url='...' parsed as equals(url, '...'))
    bool isKeyValueArg(const ASTPtr & ast)
    {
        const auto * func = ast->as<const ASTFunction>();
        return func && func->name == "equals";
    }
}

ASTPtr BackupInfo::toAST() const
{
    auto func = make_intrusive<ASTFunction>();
    func->name = backup_engine_name;
    func->no_empty_args = true;
    func->kind = ASTFunction::Kind::BACKUP_NAME;

    auto list = make_intrusive<ASTExpressionList>();
    func->arguments = list;
    func->children.push_back(list);
    list->children.reserve(args.size() + kv_args.size() + !id_arg.empty());

    if (!id_arg.empty())
        list->children.push_back(make_intrusive<ASTIdentifier>(id_arg));

    for (const auto & arg : args)
        list->children.push_back(make_intrusive<ASTLiteral>(arg));

    for (const auto & kv_arg : kv_args)
        list->children.push_back(kv_arg);

    if (function_arg)
        list->children.push_back(function_arg);

    return func;
}


BackupInfo BackupInfo::fromAST(const IAST & ast)
{
    const auto * func = ast.as<const ASTFunction>();
    if (!func)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected function, got {}", ast.formatForErrorMessage());

    BackupInfo res;
    res.backup_engine_name = func->name;

    if (func->arguments)
    {
        const auto * list = func->arguments->as<const ASTExpressionList>();
        if (!list)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected list, got {}", func->arguments->formatForErrorMessage());

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

            /// Check for key-value arguments (e.g., url='...' parsed as equals(url, '...'))
            if (isKeyValueArg(elem))
            {
                res.kv_args.push_back(elem);
                continue;
            }

            const auto * lit = elem->as<const ASTLiteral>();
            if (!lit)
            {
                if (index == args_size - 1 && elem->as<const ASTFunction>())
                {
                    res.function_arg = elem;
                    break;
                }
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected literal, got {}", elem->formatForErrorMessage());
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

NamedCollectionPtr BackupInfo::getNamedCollection(ContextPtr context) const
{
    if (id_arg.empty())
        return nullptr;

    /// Load named collections (both from config and SQL-defined)
    NamedCollectionFactory::instance().loadIfNot();

    auto collection = NamedCollectionFactory::instance().tryGet(id_arg);
    if (!collection)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no named collection `{}`", id_arg);

    /// Check access rights for the named collection
    context->checkAccess(AccessType::NAMED_COLLECTION, id_arg);

    /// Apply key-value overrides from the query (e.g., url='...', blob_path='...')
    if (!kv_args.empty())
    {
        auto mutable_collection = collection->duplicate();
        auto params_from_query = getParamsMapFromAST(kv_args, context);
        for (const auto & [key, value] : params_from_query)
            mutable_collection->setOrUpdate<String>(key, fieldToString(value), {});
        collection = std::move(mutable_collection);
    }

    return collection;
}

}
