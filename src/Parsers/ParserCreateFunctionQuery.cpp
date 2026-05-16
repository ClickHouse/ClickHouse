#include <Parsers/ParserCreateFunctionQuery.h>

#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTLiteral.h>

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
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier function_name_p;
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_language(Keyword::LANGUAGE);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_drv_arguments(Keyword::ARGUMENTS);
    ParserKeyword s_drv_returns(Keyword::RETURNS);
    ParserKeyword s_drv_engine(Keyword::ENGINE);

    ASTPtr function_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_attach = false;

    if (s_create.ignore(pos, expected))
        is_attach = false;
    else if (s_attach.ignore(pos, expected))
        is_attach = true;
    else
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

    /// Driver-based form:
    ///     CREATE FUNCTION name [ARGUMENTS (...)] [RETURNS T] ENGINE = DriverName(k=v, ...) AS '...'
    /// `ATTACH FUNCTION ...` form ends up here too.
    {
        IParser::Pos saved_pos = pos;

        ASTPtr arguments_ast;
        ASTPtr return_type_ast;
        bool consumed_anything = false;

        if (s_drv_arguments.ignore(pos, expected))
        {
            ParserToken s_lparen(TokenType::OpeningRoundBracket);
            ParserToken s_rparen(TokenType::ClosingRoundBracket);

            if (!s_lparen.ignore(pos, expected))
                return false;

            if (s_rparen.ignore(pos, expected))
            {
                arguments_ast = make_intrusive<ASTExpressionList>();
            }
            else if (ParserNameTypePairList{}.parse(pos, arguments_ast, expected)
                || ParserTypeList{}.parse(pos, arguments_ast, expected))
            {
                if (!s_rparen.ignore(pos, expected))
                    return false;
            }
            else
                return false;

            consumed_anything = true;
        }

        if (s_drv_returns.ignore(pos, expected))
        {
            if (!ParserDataType{}.parse(pos, return_type_ast, expected))
                return false;
            consumed_anything = true;
        }

        if (s_drv_engine.ignore(pos, expected))
        {
            ParserToken s_eq(TokenType::Equals);
            s_eq.ignore(pos, expected);

            ASTPtr engine_ident;
            if (!ParserIdentifier{}.parse(pos, engine_ident, expected))
                return false;
            String engine_name = engine_ident->as<ASTIdentifier>()->name();

            ParserToken s_lparen(TokenType::OpeningRoundBracket);
            ParserToken s_rparen(TokenType::ClosingRoundBracket);

            std::vector<std::pair<String, ASTPtr>> engine_arguments;
            if (s_lparen.ignore(pos, expected))
            {
                if (!s_rparen.ignore(pos, expected))
                {
                    ParserToken s_comma(TokenType::Comma);
                    ParserToken s_eq_inner(TokenType::Equals);

                    while (true)
                    {
                        ASTPtr key_ast;
                        if (!ParserIdentifier{}.parse(pos, key_ast, expected))
                            return false;
                        String key_name = key_ast->as<ASTIdentifier>()->name();

                        if (!s_eq_inner.ignore(pos, expected))
                            return false;

                        ASTPtr value_ast;
                        if (!ParserLiteral{}.parse(pos, value_ast, expected))
                            return false;

                        engine_arguments.emplace_back(key_name, value_ast);

                        if (!s_comma.ignore(pos, expected))
                            break;
                    }

                    if (!s_rparen.ignore(pos, expected))
                        return false;
                }
            }

            String source_code;
            if (s_as.ignore(pos, expected))
            {
                ASTPtr body_literal;
                if (!ParserStringLiteral{}.parse(pos, body_literal, expected))
                    return false;
                source_code = body_literal->as<ASTLiteral>()->value.safeGet<String>();
            }

            auto create_function_query = make_intrusive<ASTCreateFunctionWithDriverQuery>();
            create_function_query->or_replace = or_replace;
            create_function_query->if_not_exists = if_not_exists;
            create_function_query->is_attach = is_attach;
            create_function_query->cluster = std::move(cluster_str);
            create_function_query->engine_name = std::move(engine_name);
            create_function_query->engine_arguments = std::move(engine_arguments);
            create_function_query->source_code = std::move(source_code);

            create_function_query->function_name_ast = function_name;
            create_function_query->children.push_back(function_name);
            if (arguments_ast)
            {
                create_function_query->arguments_ast = arguments_ast;
                create_function_query->children.push_back(arguments_ast);
            }
            if (return_type_ast)
            {
                create_function_query->return_type_ast = return_type_ast;
                create_function_query->children.push_back(return_type_ast);
            }
            for (const auto & [_, value] : create_function_query->engine_arguments)
                create_function_query->children.push_back(value);

            node = create_function_query;
            return true;
        }

        if (consumed_anything)
        {
            /// We consumed ARGUMENTS/RETURNS but did not see ENGINE - this is not a valid query.
            return false;
        }

        pos = saved_pos;
    }

    if (is_attach)
    {
        /// ATTACH FUNCTION is only meaningful for the driver-based variant above.
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
        std::optional<ParserKeyword> s_deterministic{Keyword::DETERMINISTIC};

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
            else if (s_deterministic && s_deterministic->ignore(pos, expected))
            {
                s_deterministic.reset();
                create_function_query->is_deterministic = true;
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

        /// The parser accepts clauses in any order, so children were added
        /// in parse order. Normalize to the canonical order that formatImpl uses.
        create_function_query->normalizeChildrenOrder();

        node = create_function_query;
        return true;
    }
    return false;
}

}
