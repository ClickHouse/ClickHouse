#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserWithElement.h>


namespace DB
{
bool ParserWithElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_materialized(Keyword::MATERIALIZED);
    ParserSubquery s_subquery;
    ParserAliasesExpressionList exp_list_for_aliases;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    auto old_pos = pos;

    // `select` (case-insensitive) as a bareword is disallowed as a CTE name
    // because it creates ambiguity with the SELECT keyword that follows the WITH clause.
    // This check must happen before the CTE/expression parsing below, because if CTE parsing
    // rejects `select` and falls through to the expression path, `WITH select AS foo` would be
    // silently reinterpreted as an expression alias instead of producing an error.
    if (ASTPtr ident; s_ident.parse(pos, ident, expected))
    {
        String name;
        if (tryGetIdentifierNameInto(ident, name)
            && old_pos->type == TokenType::BareWord
            && strcasecmp(name.c_str(), "select") == 0)
            return false;
    }
    pos = old_pos;

    // Trying to parse structure: identifier [(alias1, alias2, ...)] AS (subquery)
    if (ASTPtr cte_name, aliases;
        s_ident.parse(pos, cte_name, expected) &&
        (
            [&]() -> bool {
                if (open_bracket.ignore(pos, expected))
                {
                    if (ASTPtr expression_list_for_aliases; exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
                    {
                        aliases = expression_list_for_aliases;
                        return close_bracket.ignore(pos, expected);
                    }
                    else
                    {
                        return false;
                    }
                }
                return true;
            }()
        ) &&
        s_as.ignore(pos, expected))
    {
        bool has_materialized_keyword = s_materialized.ignore(pos, expected);

        /// Optionally parse an engine clause for a materialized CTE:
        ///     WITH t AS MATERIALIZED ENGINE = <Engine>[(args)] [SETTINGS ...] (subquery)
        /// A missing ENGINE clause means the default Memory engine, so its absence is not an error.
        ///
        /// The engine function is parsed WITHOUT parametric parameters (`ParserFunction(false)`), and
        /// with an explicit bare-identifier fallback for argument-less engines. This is critical: the
        /// full parametric form would parse `ENGINE = Join(ANY, LEFT, k) (SELECT ...)` as the parametric
        /// function `Join(ANY, LEFT, k)(SELECT ...)`, swallowing the CTE subquery as an argument list.
        ASTPtr storage_ast;
        if (has_materialized_keyword)
        {
            ParserKeyword s_engine(Keyword::ENGINE);
            ParserKeyword s_settings(Keyword::SETTINGS);
            ParserToken s_eq(TokenType::Equals);
            ParserFunction engine_p(/*allow_function_parameters_=*/false, /*is_table_function_=*/false);
            ParserIdentifier engine_ident_p;
            ParserSetQuery settings_p(/*parse_only_internals_=*/true);

            if (s_engine.ignore(pos, expected))
            {
                s_eq.ignore(pos, expected);

                ASTPtr engine;
                if (!engine_p.parse(pos, engine, expected))
                {
                    /// Argument-less engine (e.g. Memory, Set): a bare identifier followed by the subquery.
                    ASTPtr engine_name;
                    if (!engine_ident_p.parse(pos, engine_name, expected))
                        return false;
                    auto engine_function = make_intrusive<ASTFunction>();
                    tryGetIdentifierNameInto(engine_name, engine_function->name);
                    engine_function->setNoEmptyArgs(true);
                    engine = engine_function;
                }
                engine->as<ASTFunction &>().setKind(ASTFunction::Kind::TABLE_ENGINE);

                ASTPtr settings;
                if (s_settings.ignore(pos, expected) && !settings_p.parse(pos, settings, expected))
                    return false;

                auto storage = make_intrusive<ASTStorage>();
                storage->set(storage->engine, engine);
                storage->set(storage->settings, settings);
                storage_ast = storage;
            }
        }

        if (ASTPtr subquery; s_subquery.parse(pos, subquery, expected))
        {
            auto with_element = make_intrusive<ASTWithElement>();

            tryGetIdentifierNameInto(cte_name, with_element->name);
            with_element->aliases = std::move(aliases);
            with_element->is_materialized = has_materialized_keyword;
            if (storage_ast)
            {
                with_element->storage = storage_ast;
                with_element->children.push_back(with_element->storage);
            }
            with_element->subquery = std::move(subquery);
            with_element->children.push_back(with_element->subquery);

            node = with_element;
            return true;
        }
    }

    /// CTE parsing failed, rollback and try to parse ordinary expression
    pos = old_pos;
    ParserExpressionWithOptionalAlias s_expr(false);
    if (!s_expr.parse(pos, node, expected))
        return false;

    return true;
}


}
