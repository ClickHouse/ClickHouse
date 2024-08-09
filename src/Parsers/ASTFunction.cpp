#include <string_view>

#include <Parsers/ASTFunction.h>

#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/KnownObjectNames.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTSetQuery.h>
#include <Core/QualifiedTableName.h>


using namespace std::literals;


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int UNKNOWN_FUNCTION;
}


namespace
{
    /// Finds arguments of a specified function which should not be displayed for most users for security reasons.
    /// That involves passwords and secret keys.
    class FunctionSecretArgumentsFinder
    {
    public:
        explicit FunctionSecretArgumentsFinder(const ASTFunction & function_) : function(function_)
        {
            if (!function.arguments)
                return;

            const auto * expr_list = function.arguments->as<ASTExpressionList>();
            if (!expr_list)
                return;

            arguments = &expr_list->children;
            switch (function.kind)
            {
                case ASTFunction::Kind::ORDINARY_FUNCTION: findOrdinaryFunctionSecretArguments(); break;
                case ASTFunction::Kind::WINDOW_FUNCTION: break;
                case ASTFunction::Kind::LAMBDA_FUNCTION:  break;
                case ASTFunction::Kind::TABLE_ENGINE: findTableEngineSecretArguments(); break;
                case ASTFunction::Kind::DATABASE_ENGINE: findDatabaseEngineSecretArguments(); break;
                case ASTFunction::Kind::BACKUP_NAME: findBackupNameSecretArguments(); break;
            }
        }

        struct Result
        {
            /// Result constructed by default means no arguments will be hidden.
            size_t start = static_cast<size_t>(-1);
            size_t count = 0; /// Mostly it's either 0 or 1. There are only a few cases where `count` can be greater than 1 (e.g. see `encrypt`).
                              /// In all known cases secret arguments are consecutive
            bool are_named = false; /// Arguments like `password = 'password'` are considered as named arguments.
        };

        Result getResult() const { return result; }

    private:
        const ASTFunction & function;
        const ASTs * arguments = nullptr;
        Result result;

        void markSecretArgument(size_t index, bool argument_is_named = false)
        {
            if (!result.count)
            {
                result.start = index;
                result.are_named = argument_is_named;
            }
            chassert(index >= result.start); /// We always check arguments consecutively
            result.count = index + 1 - result.start;
            if (!argument_is_named)
                result.are_named = false;
        }

        void findOrdinaryFunctionSecretArguments()
        {
            if ((function.name == "mysql") || (function.name == "postgresql") || (function.name == "mongodb"))
            {
                /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
                /// postgresql('host:port', 'database', 'table', 'user', 'password', ...)
                /// mongodb('host:port', 'database', 'collection', 'user', 'password', ...)
                findMySQLFunctionSecretArguments();
            }
            else if ((function.name == "s3") || (function.name == "cosn") || (function.name == "oss"))
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                findS3FunctionSecretArguments(/* is_cluster_function= */ false);
            }
            else if (function.name == "s3Cluster")
            {
                /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                findS3FunctionSecretArguments(/* is_cluster_function= */ true);
            }
            else if ((function.name == "remote") || (function.name == "remoteSecure"))
            {
                /// remote('addresses_expr', 'db', 'table', 'user', 'password', ...)
                findRemoteFunctionSecretArguments();
            }
            else if ((function.name == "encrypt") || (function.name == "decrypt") ||
                     (function.name == "aes_encrypt_mysql") || (function.name == "aes_decrypt_mysql") ||
                     (function.name == "tryDecrypt"))
            {
                /// encrypt('mode', 'plaintext', 'key' [, iv, aad])
                findEncryptionFunctionSecretArguments();
            }
        }

        void findMySQLFunctionSecretArguments()
        {
            if (isNamedCollectionName(0))
            {
                /// mysql(named_collection, ..., password = 'password', ...)
                findSecretNamedArgument("password", 1);
            }
            else
            {
                /// mysql('host:port', 'database', 'table', 'user', 'password', ...)
                markSecretArgument(4);
            }
        }

        void findS3FunctionSecretArguments(bool is_cluster_function)
        {
            /// s3Cluster('cluster_name', 'url', ...) has 'url' as its second argument.
            size_t url_arg_idx = is_cluster_function ? 1 : 0;

            if (!is_cluster_function && isNamedCollectionName(0))
            {
                /// s3(named_collection, ..., secret_access_key = 'secret_access_key', ...)
                findSecretNamedArgument("secret_access_key", 1);
                return;
            }

            /// We're going to replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
            /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
            /// s3Cluster('cluster_name', 'url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

            /// But we should check the number of arguments first because we don't need to do any replacements in case of
            /// s3('url' [, 'format']) or s3Cluster('cluster_name', 'url' [, 'format'])
            if (arguments->size() < url_arg_idx + 3)
                return;

            if (arguments->size() >= url_arg_idx + 5)
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'structure', ...)
                markSecretArgument(url_arg_idx + 2);
            }
            else
            {
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...)
                /// We need to distinguish that from s3('url', 'format', 'structure' [, 'compression_method']).
                /// So we will check whether the argument after 'url' is a format.
                String format;
                if (!tryGetStringFromArgument(url_arg_idx + 1, &format, /* allow_identifier= */ false))
                {
                    /// We couldn't evaluate the argument after 'url' so we don't know whether it is a format or `aws_access_key_id`.
                    /// So it's safer to wipe the next argument just in case.
                    markSecretArgument(url_arg_idx + 2); /// Wipe either `aws_secret_access_key` or `structure`.
                    return;
                }

                if (KnownFormatNames::instance().exists(format))
                    return; /// The argument after 'url' is a format: s3('url', 'format', ...)

                /// The argument after 'url' is not a format so we do our replacement:
                /// s3('url', 'aws_access_key_id', 'aws_secret_access_key', ...) -> s3('url', 'aws_access_key_id', '[HIDDEN]', ...)
                markSecretArgument(url_arg_idx + 2);
            }
        }

        bool tryGetStringFromArgument(size_t arg_idx, String * res, bool allow_identifier = true) const
        {
            if (arg_idx >= arguments->size())
                return false;

            return tryGetStringFromArgument(*(*arguments)[arg_idx], res, allow_identifier);
        }

        static bool tryGetStringFromArgument(const IAST & argument, String * res, bool allow_identifier = true)
        {
            if (const auto * literal = argument.as<ASTLiteral>())
            {
                if (literal->value.getType() != Field::Types::String)
                    return false;
                if (res)
                    *res = literal->value.safeGet<String>();
                return true;
            }

            if (allow_identifier)
            {
                if (const auto * id = argument.as<ASTIdentifier>())
                {
                    if (res)
                        *res = id->name();
                    return true;
                }
            }

            return false;
        }

        void findRemoteFunctionSecretArguments()
        {
            if (isNamedCollectionName(0))
            {
                /// remote(named_collection, ..., password = 'password', ...)
                findSecretNamedArgument("password", 1);
                return;
            }

            /// We're going to replace 'password' with '[HIDDEN'] for the following signatures:
            /// remote('addresses_expr', db.table, 'user' [, 'password'] [, sharding_key])
            /// remote('addresses_expr', 'db', 'table', 'user' [, 'password'] [, sharding_key])
            /// remote('addresses_expr', table_function(), 'user' [, 'password'] [, sharding_key])

            /// But we should check the number of arguments first because we don't need to do any replacements in case of
            /// remote('addresses_expr', db.table)
            if (arguments->size() < 3)
                return;

            size_t arg_num = 1;

            /// Skip 1 or 2 arguments with table_function() or db.table or 'db', 'table'.
            const auto * table_function = (*arguments)[arg_num]->as<ASTFunction>();
            if (table_function && KnownTableFunctionNames::instance().exists(table_function->name))
            {
                ++arg_num;
            }
            else
            {
                std::optional<String> database;
                std::optional<QualifiedTableName> qualified_table_name;
                if (!tryGetDatabaseNameOrQualifiedTableName(arg_num, database, qualified_table_name))
                {
                    /// We couldn't evaluate the argument so we don't know whether it is 'db.table' or just 'db'.
                    /// Hence we can't figure out whether we should skip one argument 'user' or two arguments 'table', 'user'
                    /// before the argument 'password'. So it's safer to wipe two arguments just in case.
                    /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
                    /// before wiping it (because the `password` argument is always a literal string).
                    if (tryGetStringFromArgument(arg_num + 2, nullptr, /* allow_identifier= */ false))
                    {
                        /// Wipe either `password` or `user`.
                        markSecretArgument(arg_num + 2);
                    }
                    if (tryGetStringFromArgument(arg_num + 3, nullptr, /* allow_identifier= */ false))
                    {
                        /// Wipe either `password` or `sharding_key`.
                        markSecretArgument(arg_num + 3);
                    }
                    return;
                }

                /// Skip the current argument (which is either a database name or a qualified table name).
                ++arg_num;
                if (database)
                {
                    /// Skip the 'table' argument if the previous argument was a database name.
                    ++arg_num;
                }
            }

            /// Skip username.
            ++arg_num;

            /// Do our replacement:
            /// remote('addresses_expr', db.table, 'user', 'password', ...) -> remote('addresses_expr', db.table, 'user', '[HIDDEN]', ...)
            /// The last argument can be also a `sharding_key`, so we need to check that argument is a literal string
            /// before wiping it (because the `password` argument is always a literal string).
            bool can_be_password = tryGetStringFromArgument(arg_num, nullptr, /* allow_identifier= */ false);
            if (can_be_password)
                markSecretArgument(arg_num);
        }

        /// Tries to get either a database name or a qualified table name from an argument.
        /// Empty string is also allowed (it means the default database).
        /// The function is used by findRemoteFunctionSecretArguments() to determine how many arguments to skip before a password.
        bool tryGetDatabaseNameOrQualifiedTableName(
            size_t arg_idx,
            std::optional<String> & res_database,
            std::optional<QualifiedTableName> & res_qualified_table_name) const
        {
            res_database.reset();
            res_qualified_table_name.reset();

            String str;
            if (!tryGetStringFromArgument(arg_idx, &str, /* allow_identifier= */ true))
                return false;

            if (str.empty())
            {
                res_database = "";
                return true;
            }

            auto qualified_table_name = QualifiedTableName::tryParseFromString(str);
            if (!qualified_table_name)
                return false;

            if (qualified_table_name->database.empty())
                res_database = std::move(qualified_table_name->table);
            else
                res_qualified_table_name = std::move(qualified_table_name);
            return true;
        }

        void findEncryptionFunctionSecretArguments()
        {
            if (arguments->empty())
                return;

            /// We replace all arguments after 'mode' with '[HIDDEN]':
            /// encrypt('mode', 'plaintext', 'key' [, iv, aad]) -> encrypt('mode', '[HIDDEN]')
            result.start = 1;
            result.count = arguments->size() - 1;
        }

        void findTableEngineSecretArguments()
        {
            const String & engine_name = function.name;
            if (engine_name == "ExternalDistributed")
            {
                /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
                findExternalDistributedTableEngineSecretArguments();
            }
            else if ((engine_name == "MySQL") || (engine_name == "PostgreSQL") ||
                     (engine_name == "MaterializedPostgreSQL") || (engine_name == "MongoDB"))
            {
                /// MySQL('host:port', 'database', 'table', 'user', 'password', ...)
                /// PostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
                /// MaterializedPostgreSQL('host:port', 'database', 'table', 'user', 'password', ...)
                /// MongoDB('host:port', 'database', 'collection', 'user', 'password', ...)
                findMySQLFunctionSecretArguments();
            }
            else if ((engine_name == "S3") || (engine_name == "COSN") || (engine_name == "OSS"))
            {
                /// S3('url', ['aws_access_key_id', 'aws_secret_access_key',] ...)
                findS3TableEngineSecretArguments();
            }
        }

        void findExternalDistributedTableEngineSecretArguments()
        {
            if (isNamedCollectionName(1))
            {
                /// ExternalDistributed('engine', named_collection, ..., password = 'password', ...)
                findSecretNamedArgument("password", 2);
            }
            else
            {
                /// ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password')
                markSecretArgument(5);
            }
        }

        void findS3TableEngineSecretArguments()
        {
            if (isNamedCollectionName(0))
            {
                /// S3(named_collection, ..., secret_access_key = 'secret_access_key')
                findSecretNamedArgument("secret_access_key", 1);
                return;
            }

            /// We replace 'aws_secret_access_key' with '[HIDDEN'] for the following signatures:
            /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format')
            /// S3('url', 'aws_access_key_id', 'aws_secret_access_key', 'format', 'compression')

            /// But we should check the number of arguments first because we don't need to do that replacements in case of
            /// S3('url' [, 'format' [, 'compression']])
            if (arguments->size() < 4)
                return;

            markSecretArgument(2);
        }

        void findDatabaseEngineSecretArguments()
        {
            const String & engine_name = function.name;
            if ((engine_name == "MySQL") || (engine_name == "MaterializeMySQL") ||
                (engine_name == "MaterializedMySQL") || (engine_name == "PostgreSQL") ||
                (engine_name == "MaterializedPostgreSQL"))
            {
                /// MySQL('host:port', 'database', 'user', 'password')
                /// PostgreSQL('host:port', 'database', 'user', 'password')
                findMySQLDatabaseSecretArguments();
            }
        }

        void findMySQLDatabaseSecretArguments()
        {
            if (isNamedCollectionName(0))
            {
                /// MySQL(named_collection, ..., password = 'password', ...)
                findSecretNamedArgument("password", 1);
            }
            else
            {
                /// MySQL('host:port', 'database', 'user', 'password')
                markSecretArgument(3);
            }
        }

        void findBackupNameSecretArguments()
        {
            const String & engine_name = function.name;
            if (engine_name == "S3")
            {
                /// BACKUP ... TO S3(url, [aws_access_key_id, aws_secret_access_key])
                markSecretArgument(2);
            }
        }

        /// Whether a specified argument can be the name of a named collection?
        bool isNamedCollectionName(size_t arg_idx) const
        {
            if (arguments->size() <= arg_idx)
                return false;

            const auto * identifier = (*arguments)[arg_idx]->as<ASTIdentifier>();
            return identifier != nullptr;
        }

        /// Looks for a secret argument with a specified name. This function looks for arguments in format `key=value` where the key is specified.
        void findSecretNamedArgument(const std::string_view & key, size_t start = 0)
        {
            for (size_t i = start; i < arguments->size(); ++i)
            {
                const auto & argument = (*arguments)[i];
                const auto * equals_func = argument->as<ASTFunction>();
                if (!equals_func || (equals_func->name != "equals"))
                    continue;

                const auto * expr_list = equals_func->arguments->as<ASTExpressionList>();
                if (!expr_list)
                    continue;

                const auto & equal_args = expr_list->children;
                if (equal_args.size() != 2)
                    continue;

                String found_key;
                if (!tryGetStringFromArgument(*equal_args[0], &found_key))
                    continue;

                if (found_key == key)
                    markSecretArgument(i, /* argument_is_named= */ true);
            }
        }
    };
}


void ASTFunction::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// These functions contain some unexpected ASTs in arguments (e.g. SETTINGS or even a SELECT query)
    if (name == "view" || name == "viewIfPermitted" || name == "mysql" || name == "postgresql" || name == "mongodb" || name == "s3")
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Table function '{}' cannot be used as an expression", name);

    /// If function can be converted to literal it will be parsed as literal after formatting.
    /// In distributed query it may lead to mismathed column names.
    /// To avoid it we check whether we can convert function to literal.
    if (auto literal = toLiteral())
    {
        literal->appendColumnName(ostr);
        return;
    }

    writeString(name, ostr);

    if (parameters)
    {
        writeChar('(', ostr);
        for (auto * it = parameters->children.begin(); it != parameters->children.end(); ++it)
        {
            if (it != parameters->children.begin())
                writeCString(", ", ostr);

            (*it)->appendColumnName(ostr);
        }
        writeChar(')', ostr);
    }

    writeChar('(', ostr);
    if (arguments)
    {
        for (auto * it = arguments->children.begin(); it != arguments->children.end(); ++it)
        {
            if (it != arguments->children.begin())
                writeCString(", ", ostr);

            (*it)->appendColumnName(ostr);
        }
    }

    writeChar(')', ostr);

    if (is_window_function)
    {
        writeCString(" OVER ", ostr);
        if (!window_name.empty())
        {
            ostr << window_name;
        }
        else
        {
            FormatSettings format_settings{ostr, true /* one_line */};
            FormatState state;
            FormatStateStacked frame;
            writeCString("(", ostr);
            window_definition->formatImpl(format_settings, state, frame);
            writeCString(")", ostr);
        }
    }
}

void ASTFunction::finishFormatWithWindow(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!is_window_function)
        return;

    settings.ostr << " OVER ";
    if (!window_name.empty())
    {
        settings.ostr << backQuoteIfNeed(window_name);
    }
    else
    {
        settings.ostr << "(";
        window_definition->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }
}

/** Get the text that identifies this element. */
String ASTFunction::getID(char delim) const
{
    return "Function" + (delim + name);
}

ASTPtr ASTFunction::clone() const
{
    auto res = std::make_shared<ASTFunction>(*this);
    res->children.clear();

    if (arguments) { res->arguments = arguments->clone(); res->children.push_back(res->arguments); }
    if (parameters) { res->parameters = parameters->clone(); res->children.push_back(res->parameters); }

    if (window_definition)
    {
        res->window_definition = window_definition->clone();
        res->children.push_back(res->window_definition);
    }

    return res;
}


void ASTFunction::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    IAST::updateTreeHashImpl(hash_state);
}

template <typename Container>
static ASTPtr createLiteral(const ASTs & arguments)
{
    Container container;

    for (const auto & arg : arguments)
    {
        if (const auto * literal = arg->as<ASTLiteral>())
        {
            container.push_back(literal->value);
        }
        else if (auto * func = arg->as<ASTFunction>())
        {
            if (auto func_literal = func->toLiteral())
                container.push_back(func_literal->as<ASTLiteral>()->value);
            else
                return {};
        }
        else
            /// Some of the Array or Tuple arguments is not literal
            return {};
    }

    return std::make_shared<ASTLiteral>(container);
}

ASTPtr ASTFunction::toLiteral() const
{
    if (!arguments)
        return {};

    if (name == "array")
        return createLiteral<Array>(arguments->children);

    if (name == "tuple")
        return createLiteral<Tuple>(arguments->children);

    return {};
}


/** A special hack. If it's [I]LIKE or NOT [I]LIKE expression and the right hand side is a string literal,
  *  we will highlight unescaped metacharacters % and _ in string literal for convenience.
  * Motivation: most people are unaware that _ is a metacharacter and forgot to properly escape it with two backslashes.
  * With highlighting we make it clearly obvious.
  *
  * Another case is regexp match. Suppose the user types match(URL, 'www.clickhouse.com'). It often means that the user is unaware that . is a metacharacter.
  */
static bool highlightStringLiteralWithMetacharacters(const ASTPtr & node, const IAST::FormatSettings & settings, const char * metacharacters)
{
    if (const auto * literal = node->as<ASTLiteral>())
    {
        if (literal->value.getType() == Field::Types::String)
        {
            auto string = applyVisitor(FieldVisitorToString(), literal->value);

            unsigned escaping = 0;
            for (auto c : string)
            {
                if (c == '\\')
                {
                    settings.ostr << c;
                    if (escaping == 2)
                        escaping = 0;
                    ++escaping;
                }
                else if (nullptr != strchr(metacharacters, c))
                {
                    if (escaping == 2)      /// Properly escaped metacharacter
                        settings.ostr << c;
                    else                    /// Unescaped metacharacter
                        settings.ostr << "\033[1;35m" << c << "\033[0m";
                    escaping = 0;
                }
                else
                {
                    settings.ostr << c;
                    escaping = 0;
                }
            }

            return true;
        }
    }

    return false;
}


ASTSelectWithUnionQuery * ASTFunction::tryGetQueryArgument() const
{
    if (arguments && arguments->children.size() == 1)
    {
        return arguments->children[0]->as<ASTSelectWithUnionQuery>();
    }
    return nullptr;
}


void ASTFunction::formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.expression_list_prepend_whitespace = false;
    FormatStateStacked nested_need_parens = frame;
    FormatStateStacked nested_dont_need_parens = frame;
    nested_need_parens.need_parens = true;
    nested_dont_need_parens.need_parens = false;

    if (auto * query = tryGetQueryArgument())
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";
        std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
        settings.ostr << (settings.hilite ? hilite_function : "") << name << (settings.hilite ? hilite_none : "");
        settings.ostr << (settings.hilite ? hilite_function : "") << "(" << (settings.hilite ? hilite_none : "");
        settings.ostr << nl_or_nothing;
        FormatStateStacked frame_nested = frame;
        frame_nested.need_parens = false;
        ++frame_nested.indent;
        query->formatImpl(settings, state, frame_nested);
        settings.ostr << nl_or_nothing << indent_str;
        settings.ostr << (settings.hilite ? hilite_function : "") << ")" << (settings.hilite ? hilite_none : "");
        return;
    }

    /// Should this function to be written as operator?
    bool written = false;

    if (arguments && !parameters)
    {
        /// Unary prefix operators.
        if (arguments->children.size() == 1)
        {
            const char * operators[] =
            {
                "negate",      "-",
                "not",         "NOT ",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (strcasecmp(name.c_str(), func[0]) != 0)
                {
                    continue;
                }

                const auto * literal = arguments->children[0]->as<ASTLiteral>();
                const auto * function = arguments->children[0]->as<ASTFunction>();
                bool negate = name == "negate";
                bool is_tuple = literal && literal->value.getType() == Field::Types::Tuple;
                // do not add parentheses for tuple literal, otherwise extra parens will be added `-((3, 7, 3), 1)` -> `-(((3, 7, 3), 1))`
                bool literal_need_parens = literal && !is_tuple;
                // negate always requires parentheses, otherwise -(-1) will be printed as --1
                bool negate_need_parens = negate && (literal_need_parens || (function && function->name == "negate"));
                // We don't need parentheses around a single literal.
                bool need_parens = !literal && frame.need_parens && !negate_need_parens;

                // do not add extra parentheses for functions inside negate, i.e. -(-toUInt64(-(1)))
                if (negate_need_parens)
                    nested_need_parens.need_parens = false;

                if (need_parens)
                    settings.ostr << '(';

                settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");

                if (negate_need_parens)
                    settings.ostr << '(';

                arguments->formatImpl(settings, state, nested_need_parens);
                written = true;

                if (negate_need_parens)
                    settings.ostr << ')';

                if (need_parens)
                    settings.ostr << ')';

                break;
            }
        }

        /// Unary postfix operators.
        if (!written && arguments->children.size() == 1)
        {
            const char * operators[] =
            {
                "isNull",          " IS NULL",
                "isNotNull",       " IS NOT NULL",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (strcasecmp(name.c_str(), func[0]) != 0)
                {
                    continue;
                }

                if (frame.need_parens)
                    settings.ostr << '(';
                arguments->formatImpl(settings, state, nested_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
                if (frame.need_parens)
                    settings.ostr << ')';

                written = true;

                break;
            }
        }

        /** need_parens - do we need parentheses around the expression with the operator.
          * They are needed only if this expression is included in another expression with the operator.
          */

        if (!written && arguments->children.size() == 2)
        {
            const char * operators[] =
            {
                "multiply",        " * ",
                "divide",          " / ",
                "modulo",          " % ",
                "plus",            " + ",
                "minus",           " - ",
                "notEquals",       " != ",
                "lessOrEquals",    " <= ",
                "greaterOrEquals", " >= ",
                "less",            " < ",
                "greater",         " > ",
                "equals",          " = ",
                "like",            " LIKE ",
                "ilike",           " ILIKE ",
                "notLike",         " NOT LIKE ",
                "notILike",        " NOT ILIKE ",
                "in",              " IN ",
                "notIn",           " NOT IN ",
                "globalIn",        " GLOBAL IN ",
                "globalNotIn",     " GLOBAL NOT IN ",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (name == std::string_view(func[0]))
                {
                    if (frame.need_parens)
                        settings.ostr << '(';
                    arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                    settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");

                    bool special_hilite = settings.hilite
                        && (name == "like" || name == "notLike" || name == "ilike" || name == "notILike")
                        && highlightStringLiteralWithMetacharacters(arguments->children[1], settings, "%_");

                    /// Format x IN 1 as x IN (1): put parens around rhs even if there is a single element in set.
                    const auto * second_arg_func = arguments->children[1]->as<ASTFunction>();
                    const auto * second_arg_literal = arguments->children[1]->as<ASTLiteral>();
                    bool extra_parents_around_in_rhs = (name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn")
                        && !second_arg_func
                        && !(second_arg_literal
                             && (second_arg_literal->value.getType() == Field::Types::Tuple
                                || second_arg_literal->value.getType() == Field::Types::Array))
                        && !arguments->children[1]->as<ASTSubquery>();

                    if (extra_parents_around_in_rhs)
                    {
                        settings.ostr << '(';
                        arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                        settings.ostr << ')';
                    }

                    if (!special_hilite && !extra_parents_around_in_rhs)
                        arguments->children[1]->formatImpl(settings, state, nested_need_parens);

                    if (frame.need_parens)
                        settings.ostr << ')';
                    written = true;
                }
            }

            if (!written && name == "arrayElement"sv)
            {
                if (frame.need_parens)
                    settings.ostr << '(';

                arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
                arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
                written = true;

                if (frame.need_parens)
                    settings.ostr << ')';
            }

            if (!written && name == "tupleElement"sv)
            {
                // fuzzer sometimes may insert tupleElement() created from ASTLiteral:
                //
                //     Function_tupleElement, 0xx
                //     -ExpressionList_, 0xx
                //     --Literal_Int64_255, 0xx
                //     --Literal_Int64_100, 0xx
                //
                // And in this case it will be printed as "255.100", which
                // later will be parsed as float, and formatting will be
                // inconsistent.
                //
                // So instead of printing it as regular tuple,
                // let's print it as ExpressionList instead (i.e. with ", " delimiter).
                bool tuple_arguments_valid = true;
                const auto * lit_left = arguments->children[0]->as<ASTLiteral>();
                const auto * lit_right = arguments->children[1]->as<ASTLiteral>();

                if (lit_left)
                {
                    Field::Types::Which type = lit_left->value.getType();
                    if (type != Field::Types::Tuple && type != Field::Types::Array)
                    {
                        tuple_arguments_valid = false;
                    }
                }

                // It can be printed in a form of 'x.1' only if right hand side
                // is an unsigned integer lineral. We also allow nonnegative
                // signed integer literals, because the fuzzer sometimes inserts
                // them, and we want to have consistent formatting.
                if (tuple_arguments_valid && lit_right)
                {
                    if (isInt64OrUInt64FieldType(lit_right->value.getType())
                        && lit_right->value.get<Int64>() >= 0)
                    {
                        if (frame.need_parens)
                            settings.ostr << '(';

                        arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                        settings.ostr << (settings.hilite ? hilite_operator : "") << "." << (settings.hilite ? hilite_none : "");
                        arguments->children[1]->formatImpl(settings, state, nested_dont_need_parens);
                        written = true;

                        if (frame.need_parens)
                            settings.ostr << ')';
                    }
                }
            }

            if (!written && name == "lambda"sv)
            {
                /// Special case: zero elements tuple in lhs of lambda is printed as ().
                /// Special case: one-element tuple in lhs of lambda is printed as its element.

                if (frame.need_parens)
                    settings.ostr << '(';

                const auto * first_arg_func = arguments->children[0]->as<ASTFunction>();
                if (first_arg_func
                    && first_arg_func->name == "tuple"
                    && first_arg_func->arguments
                    && (first_arg_func->arguments->children.size() == 1 || first_arg_func->arguments->children.empty()))
                {
                    if (first_arg_func->arguments->children.size() == 1)
                        first_arg_func->arguments->children[0]->formatImpl(settings, state, nested_need_parens);
                    else
                        settings.ostr << "()";
                }
                else
                    arguments->children[0]->formatImpl(settings, state, nested_need_parens);

                settings.ostr << (settings.hilite ? hilite_operator : "") << " -> " << (settings.hilite ? hilite_none : "");
                arguments->children[1]->formatImpl(settings, state, nested_need_parens);
                if (frame.need_parens)
                    settings.ostr << ')';
                written = true;
            }

            if (!written && name == "viewIfPermitted"sv)
            {
                /// viewIfPermitted() needs special formatting: ELSE instead of comma between arguments, and better indents too.
                const auto * nl_or_nothing = settings.one_line ? "" : "\n";
                auto indent0 = settings.one_line ? "" : String(4u * frame.indent, ' ');
                auto indent1 = settings.one_line ? "" : String(4u * (frame.indent + 1), ' ');
                auto indent2 = settings.one_line ? "" : String(4u * (frame.indent + 2), ' ');
                settings.ostr << (settings.hilite ? hilite_function : "") << name << "(" << (settings.hilite ? hilite_none : "") << nl_or_nothing;
                FormatStateStacked frame_nested = frame;
                frame_nested.need_parens = false;
                frame_nested.indent += 2;
                arguments->children[0]->formatImpl(settings, state, frame_nested);
                settings.ostr << nl_or_nothing << indent1 << (settings.hilite ? hilite_keyword : "") << (settings.one_line ? " " : "")
                              << "ELSE " << (settings.hilite ? hilite_none : "") << nl_or_nothing << indent2;
                arguments->children[1]->formatImpl(settings, state, frame_nested);
                settings.ostr << nl_or_nothing << indent0 << ")";
                return;
            }
        }

        if (!written && arguments->children.size() >= 2)
        {
            const char * operators[] =
            {
                "and", " AND ",
                "or", " OR ",
                nullptr
            };

            for (const char ** func = operators; *func; func += 2)
            {
                if (name == std::string_view(func[0]))
                {
                    if (frame.need_parens)
                        settings.ostr << '(';
                    for (size_t i = 0; i < arguments->children.size(); ++i)
                    {
                        if (i != 0)
                            settings.ostr << (settings.hilite ? hilite_operator : "") << func[1] << (settings.hilite ? hilite_none : "");
                        if (arguments->children[i]->as<ASTSetQuery>())
                            settings.ostr << "SETTINGS ";
                        arguments->children[i]->formatImpl(settings, state, nested_need_parens);
                    }
                    if (frame.need_parens)
                        settings.ostr << ')';
                    written = true;
                }
            }
        }

        if (!written && name == "array"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << '[' << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ']' << (settings.hilite ? hilite_none : "");
            written = true;
        }

        if (!written && arguments->children.size() >= 2 && name == "tuple"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << ((frame.need_parens && !alias.empty()) ? "tuple" : "") << '('
                          << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ')' << (settings.hilite ? hilite_none : "");
            written = true;
        }

        if (!written && name == "map"sv)
        {
            settings.ostr << (settings.hilite ? hilite_operator : "") << "map(" << (settings.hilite ? hilite_none : "");
            for (size_t i = 0; i < arguments->children.size(); ++i)
            {
                if (i != 0)
                    settings.ostr << ", ";
                if (arguments->children[i]->as<ASTSetQuery>())
                    settings.ostr << "SETTINGS ";
                arguments->children[i]->formatImpl(settings, state, nested_dont_need_parens);
            }
            settings.ostr << (settings.hilite ? hilite_operator : "") << ')' << (settings.hilite ? hilite_none : "");
            written = true;
        }
    }

    if (written)
    {
        return finishFormatWithWindow(settings, state, frame);
    }

    settings.ostr << (settings.hilite ? hilite_function : "") << name;

    if (parameters)
    {
        settings.ostr << '(' << (settings.hilite ? hilite_none : "");
        parameters->formatImpl(settings, state, nested_dont_need_parens);
        settings.ostr << (settings.hilite ? hilite_function : "") << ')';
    }

    if ((arguments && !arguments->children.empty()) || !no_empty_args)
        settings.ostr << '(' << (settings.hilite ? hilite_none : "");

    if (arguments)
    {
        bool special_hilite_regexp = settings.hilite
            && (name == "match" || name == "extract" || name == "extractAll" || name == "replaceRegexpOne"
                || name == "replaceRegexpAll");

        FunctionSecretArgumentsFinder::Result secret_arguments;
        if (!settings.show_secrets)
            secret_arguments = FunctionSecretArgumentsFinder{*this}.getResult();

        for (size_t i = 0, size = arguments->children.size(); i < size; ++i)
        {
            if (i != 0)
                settings.ostr << ", ";

            const auto & argument = arguments->children[i];
            if (argument->as<ASTSetQuery>())
                settings.ostr << "SETTINGS ";

            if (!settings.show_secrets && (secret_arguments.start <= i) && (i < secret_arguments.start + secret_arguments.count))
            {
                if (secret_arguments.are_named)
                {
                   assert_cast<const ASTFunction *>(argument.get())->arguments->children[0]->formatImpl(settings, state, nested_dont_need_parens);
                   settings.ostr << (settings.hilite ? hilite_operator : "") << " = " << (settings.hilite ? hilite_none : "");
                }
                settings.ostr << "'[HIDDEN]'";
                if (size <= secret_arguments.start + secret_arguments.count && !secret_arguments.are_named)
                    break; /// All other arguments should also be hidden.
                continue;
            }

            if ((i == 1) && special_hilite_regexp
                && highlightStringLiteralWithMetacharacters(argument, settings, "|()^$.[]?*+{:-"))
            {
                continue;
            }

            argument->formatImpl(settings, state, nested_dont_need_parens);
        }
    }

    if ((arguments && !arguments->children.empty()) || !no_empty_args)
        settings.ostr << (settings.hilite ? hilite_function : "") << ')';

    settings.ostr << (settings.hilite ? hilite_none : "");

    return finishFormatWithWindow(settings, state, frame);
}

bool ASTFunction::hasSecretParts() const
{
    return (FunctionSecretArgumentsFinder{*this}.getResult().count > 0) || childrenHaveSecretParts();
}

String getFunctionName(const IAST * ast)
{
    String res;
    if (tryGetFunctionNameInto(ast, res))
        return res;
    if (ast)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "{} is not an function", queryToString(*ast));
    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "AST node is nullptr");
}

std::optional<String> tryGetFunctionName(const IAST * ast)
{
    String res;
    if (tryGetFunctionNameInto(ast, res))
        return res;
    return {};
}

bool tryGetFunctionNameInto(const IAST * ast, String & name)
{
    if (ast)
    {
        if (const auto * node = ast->as<ASTFunction>())
        {
            name = node->name;
            return true;
        }
    }
    return false;
}

}
