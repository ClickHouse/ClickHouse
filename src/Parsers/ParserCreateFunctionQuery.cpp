#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{

bool ParserCreateFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier function_name_p;
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_language(Keyword::LANGUAGE);
    ParserKeyword s_settings(Keyword::SETTINGS);

    ASTPtr function_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_function.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (s_as.ignore(pos, expected))
    {
        /// CREATE FUNCTION func AS x -> x + 1;
        ParserExpression lambda_p;

        ASTPtr function_expression_body;
        if (!lambda_p.parse(pos, function_expression_body, expected))
            return false;

        auto create_function_query = make_intrusive<ASTCreateSQLFunctionQuery>();

        create_function_query->function_name = function_name;
        create_function_query->children.push_back(function_name);

        create_function_query->function_core = function_expression_body;
        create_function_query->children.push_back(function_expression_body);

        create_function_query->or_replace = or_replace;
        create_function_query->if_not_exists = if_not_exists;
        create_function_query->cluster = std::move(cluster_str);

        node = create_function_query;
        return true;
    }
    else if (s_language.ignore(pos, expected))
    {
        /** CREATE func LANGUAGE WASM
          *     ARGUMENTS (a Int32, b UInt8)
          *     RETURNS UInt64
          *     FROM 'some_module' [:: 'internal_func_name']
          *     SHA256_HASH 'cafe42...'
          *     ABI ROW_DIRECT
          *     SETTINGS key1 = value1, key2 = value2
          */

        ASTPtr lang_literal;
        if (!ParserIdentifier{}.parse(pos, lang_literal, expected))
            return false;

        if (lang_literal->as<ASTIdentifier>()->name() != "WASM")
            return false;

        auto create_function_query = make_intrusive<ASTCreateWasmFunctionQuery>();
        create_function_query->or_replace = or_replace;
        create_function_query->if_not_exists = if_not_exists;
        create_function_query->cluster = std::move(cluster_str);

        create_function_query->setName(function_name);

        std::optional<ParserKeyword> s_arguments{Keyword::ARGUMENTS};
        std::optional<ParserKeyword> s_returns{Keyword::RETURNS};
        std::optional<ParserKeyword> s_from{Keyword::FROM};
        std::optional<ParserKeyword> s_hash{Keyword::SHA256_HASH};
        std::optional<ParserKeyword> s_abi{Keyword::ABI};

        /// Parse fields in any order, but only once each
        while (true)
        {
            if (s_abi && s_abi->ignore(pos, expected))
            {
                s_abi.reset();

                ASTPtr abi_literal;
                if (!ParserIdentifier{}.parse(pos, abi_literal, expected))
                    return false;

                create_function_query->setAbi(abi_literal);
            }
            else if (s_arguments && s_arguments->ignore(pos, expected))
            {
                s_arguments.reset();

                ParserToken s_lparen(TokenType::OpeningRoundBracket);
                ParserToken s_rparen(TokenType::ClosingRoundBracket);

                ASTPtr arguments_ast = nullptr;
                if (!s_lparen.ignore(pos, expected))
                    return false;

                if (s_rparen.ignore(pos, expected))
                {
                    /// Empty argument list
                    create_function_query->setArguments(make_intrusive<ASTExpressionList>());
                }
                else if (ParserNameTypePairList{}.parse(pos, arguments_ast, expected)
                     || ParserTypeList{}.parse(pos, arguments_ast, expected))
                {
                    /// Named arguments or only types
                    create_function_query->setArguments(arguments_ast);
                    if (!s_rparen.ignore(pos, expected))
                        return false;
                }
                else
                    return false;

            }
            else if (s_returns && s_returns->ignore(pos, expected))
            {
                s_returns.reset();

                ASTPtr return_type;
                if (!ParserDataType{}.parse(pos, return_type, expected))
                    return false;
                create_function_query->setReturnType(return_type);
            }
            else if (s_from && s_from->ignore(pos, expected))
            {
                s_from.reset();

                ASTPtr module_name_ast;
                if (!ParserStringLiteral{}.parse(pos, module_name_ast, expected))
                    return false;
                create_function_query->setModuleName(module_name_ast);

                if (ParserToken(TokenType::DoubleColon).ignore(pos, expected))
                {
                    ASTPtr source_function_name_ast;
                    if (!ParserStringLiteral{}.parse(pos, source_function_name_ast, expected))
                        return false;
                    create_function_query->setSourceFunctionName(source_function_name_ast);
                }
            }
            else if (s_hash && s_hash->ignore(pos, expected))
            {
                s_hash.reset();

                ASTPtr hash_ast;
                if (!ParserStringLiteral{}.parse(pos, hash_ast, expected))
                    return false;
                create_function_query->setModuleHash(hash_ast);
            }
            else
            {
                break;
            }
        }

        /// Check that mandatory fields were parsed
        if (s_arguments)
            return s_arguments->ignore(pos, expected);
        if (s_returns)
            return s_returns->ignore(pos, expected);
        if (s_from)
            return s_from->ignore(pos, expected);

        if (s_settings.ignore(pos, expected))
        {
            ParserToken s_comma(TokenType::Comma);

            SettingsChanges changes;
            while (true)
            {
                if (!changes.empty() && !s_comma.ignore(pos))
                    break;

                changes.push_back(SettingChange{});

                if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
                    return false;
            }
            create_function_query->setSettings(std::move(changes));
        }

        node = create_function_query;
        return true;
    }
    return false;
}

}
