#include <IO/ReadHelpers.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDictionary.h>
#include <Parsers/ParserDictionaryAttributeDeclaration.h>
#include <Parsers/ParserProjectionSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserRefreshStrategy.h>
#include <Parsers/ParserViewTargets.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <Core/UUID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

namespace
{

ASTPtr parseComment(IParser::Pos & pos, Expected & expected)
{
    ParserKeyword s_comment(Keyword::COMMENT);
    ParserStringLiteral string_literal_parser;
    ASTPtr comment;

    auto begin = pos;
    if (s_comment.ignore(pos, expected))
    {
        if (!string_literal_parser.parse(pos, comment, expected))
            pos = begin;
    }
    return comment;
}

void rejectNilUUIDClause(bool attach, bool has_uuid_clause, const UUID & uuid)
{
    if (attach && has_uuid_clause && uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ATTACH queries cannot use a Nil UUID");
}

}

bool ParserSQLSecurity::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_eq(TokenType::Equals);
    ParserKeyword s_definer(Keyword::DEFINER);
    ParserKeyword s_current_user{Keyword::CURRENT_USER};
    ParserKeyword s_sql_security{Keyword::SQL_SECURITY};
    ParserKeyword s_invoker{Keyword::INVOKER};
    ParserKeyword s_none{Keyword::NONE};

    bool is_definer_current_user = false;
    ASTPtr definer;
    std::optional<SQLSecurityType> type;

    while (true)
    {
        if (!definer && !is_definer_current_user && s_definer.ignore(pos, expected))
        {
            s_eq.ignore(pos, expected);
            if (s_current_user.ignore(pos, expected))
                is_definer_current_user = true;
            else if (!ParserUserNameWithHost(/*allow_query_parameter=*/ false).parse(pos, definer, expected))
                return false;

            continue;
        }

        if (!type && s_sql_security.ignore(pos, expected))
        {
            if (s_definer.ignore(pos, expected))
                type = SQLSecurityType::DEFINER;
            else if (s_invoker.ignore(pos, expected))
                type = SQLSecurityType::INVOKER;
            else if (s_none.ignore(pos, expected))
                type = SQLSecurityType::NONE;
            else
                return false;

            continue;
        }

        break;
    }

    if (!type)
    {
        if (is_definer_current_user || definer)
            type = SQLSecurityType::DEFINER;
        else
            return false;
    }
    else if (type == SQLSecurityType::DEFINER && !definer)
        is_definer_current_user = true;

    auto result = make_intrusive<ASTSQLSecurity>();
    result->is_definer_current_user = is_definer_current_user;
    result->type = type;
    if (definer)
        result->definer = boost::static_pointer_cast<ASTUserNameWithHost>(definer);

    node = std::move(result);
    return true;
}


bool ParserIdentifierWithParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserFunction().parse(pos, node, expected);
}

bool ParserNameTypePairList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserNameTypePair>(), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserColumnDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserColumnDeclaration>(require_type, allow_null_modifiers, check_keywords_after_name), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserNameList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserCompoundIdentifier>(true, true), std::make_unique<ParserToken>(TokenType::Comma), false)
        .parse(pos, node, expected);
}

bool ParserIndexDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_granularity(Keyword::GRANULARITY);

    ParserIdentifier name_p;
    ParserExpressionWithOptionalArguments type_p;
    ParserExpression expression_p;
    ParserUnsignedInteger granularity_p;

    ASTPtr name;
    ASTPtr expr;
    ASTPtr type;
    ASTPtr granularity;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!expression_p.parse(pos, expr, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!type_p.parse(pos, type, expected))
        return false;

    if (s_granularity.ignore(pos, expected))
    {
        if (!granularity_p.parse(pos, granularity, expected))
            return false;
    }

    auto index = make_intrusive<ASTIndexDeclaration>(expr, type, name->as<ASTIdentifier &>().name());
    index->granularity = getSecondaryIndexGranularity(index->getType(), granularity);
    node = index;
    return true;
}

bool ParserStatisticsDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_type(Keyword::TYPE);

    ParserList columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserList types_p(std::make_unique<ParserExpressionWithOptionalArguments>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr columns;
    ASTPtr types;

    if (!columns_p.parse(pos, columns, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!types_p.parse(pos, types, expected))
        return false;

    auto stat = make_intrusive<ASTStatisticsDeclaration>();
    stat->set(stat->columns, columns);
    stat->set(stat->types, types);
    node = stat;

    return true;
}

bool ParserStatisticsDeclarationWithoutTypes::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{

    ParserList columns_p(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr columns;

    if (!columns_p.parse(pos, columns, expected))
        return false;

    auto stat = make_intrusive<ASTStatisticsDeclaration>();
    stat->set(stat->columns, columns);
    node = stat;

    return true;
}

bool ParserConstraintDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_check(Keyword::CHECK);
    ParserKeyword s_assume(Keyword::ASSUME);

    ParserIdentifier name_p;
    ParserExpression expression_p;

    ASTPtr name;
    ASTPtr expr;
    ASTConstraintDeclaration::Type type = ASTConstraintDeclaration::Type::CHECK;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_check.ignore(pos, expected))
    {
        if (s_assume.ignore(pos, expected))
            type = ASTConstraintDeclaration::Type::ASSUME;
        else
            return false;
    }

    if (!expression_p.parse(pos, expr, expected))
        return false;

    auto constraint = make_intrusive<ASTConstraintDeclaration>();
    constraint->name = name->as<ASTIdentifier &>().name();
    constraint->type = type;
    constraint->set(constraint->expr, expr);
    node = constraint;

    return true;
}


bool parseProjectionDeclarationBody(IParser::Pos & pos, Expected & expected, const String & name, ASTPtr & node)
{
    ParserProjectionSelectQuery query_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_type(Keyword::TYPE);
    ParserExpressionWithOptionalArguments type_p;
    ParserNotEmptyExpressionList expression_list_p(/* allow_alias_without_as_keyword */ false);
    ParserKeyword s_with_settings(Keyword::WITH_SETTINGS);
    ASTPtr query;
    ASTPtr index;
    ASTPtr type;
    ASTPtr with_settings;

    if (s_lparen.ignore(pos, expected))
    {
        if (!query_p.parse(pos, query, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }
    else if (s_index.ignore(pos, expected))
    {
        if (!expression_list_p.parse(pos, index, expected))
            return false;

        if (!s_type.ignore(pos, expected))
            return false;

        if (!type_p.parse(pos, type, expected))
            return false;
    }
    else
    {
        return false;
    }

    if (s_with_settings.ignore(pos, expected))
    {
        if (!s_lparen.ignore(pos, expected))
            return false;

        if (!settings_p.parse(pos, with_settings, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    auto projection = make_intrusive<ASTProjectionDeclaration>();
    projection->name = name;
    if (query)
        projection->set(projection->query, query);
    if (index)
        projection->set(projection->index, index);
    if (type)
        projection->set(projection->type, type);
    if (with_settings)
        projection->set(projection->with_settings, with_settings);
    node = projection;

    return true;
}

bool ParserProjectionDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ASTPtr name;
    if (!name_p.parse(pos, name, expected))
        return false;
    return parseProjectionDeclarationBody(pos, expected, name->as<ASTIdentifier &>().name(), node);
}

bool ParserForeignKeyDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_references(Keyword::REFERENCES);
    ParserCompoundIdentifier table_name_p(true, true);
    ParserExpression expression_p;

    ASTPtr name;
    ASTPtr expr;

    if (!expression_p.parse(pos, expr, expected))
        return false;

    if (!s_references.ignore(pos, expected))
        return false;

    if (!table_name_p.parse(pos, name, expected))
        return false;

    if (!expression_p.parse(pos, expr, expected))
        return false;

    ParserKeyword s_on(Keyword::ON);
    while (s_on.ignore(pos, expected))
    {
        ParserKeyword s_delete(Keyword::DELETE);
        ParserKeyword s_update(Keyword::UPDATE);

        if (!s_delete.ignore(pos, expected) && !s_update.ignore(pos, expected))
            return false;

        ParserKeyword s_restrict(Keyword::RESTRICT);
        ParserKeyword s_cascade(Keyword::CASCADE);
        ParserKeyword s_set_null(Keyword::SET_NULL);
        ParserKeyword s_no_action(Keyword::NO_ACTION);
        ParserKeyword s_set_default(Keyword::SET_DEFAULT);

        if (!s_restrict.ignore(pos, expected) && !s_cascade.ignore(pos, expected) &&
            !s_set_null.ignore(pos, expected) && !s_no_action.ignore(pos, expected) &&
            !s_set_default.ignore(pos, expected))
        {
            return false;
        }
    }

    auto foreign_key = make_intrusive<ASTForeignKeyDeclaration>();
    foreign_key->name = "Foreign Key";
    node = foreign_key;

    return true;
}

bool ParserTablePropertyDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_constraint(Keyword::CONSTRAINT);
    ParserKeyword s_projection(Keyword::PROJECTION);
    ParserKeyword s_foreign_key(Keyword::FOREIGN_KEY);
    ParserKeyword s_primary_key(Keyword::PRIMARY_KEY);

    ParserIndexDeclaration index_p;
    ParserConstraintDeclaration constraint_p;
    ParserProjectionDeclaration projection_p;
    ParserForeignKeyDeclaration foreign_key_p;
    ParserColumnDeclaration column_p{true, true};
    ParserExpression primary_key_p;

    ASTPtr new_node = nullptr;

    if (s_index.ignore(pos, expected))
    {
        if (!index_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_constraint.ignore(pos, expected))
    {
        if (!constraint_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_projection.ignore(pos, expected))
    {
        if (!projection_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_primary_key.ignore(pos, expected))
    {
        if (!primary_key_p.parse(pos, new_node, expected))
            return false;
    }
    else if (s_foreign_key.ignore(pos, expected))
    {
        if (!foreign_key_p.parse(pos, new_node, expected))
            return false;
    }
    else
    {
        if (!column_p.parse(pos, new_node, expected))
            return false;
    }

    node = new_node;
    return true;
}

bool ParserIndexDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserIndexDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserConstraintDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserConstraintDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserProjectionDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserProjectionDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, node, expected);
}

bool ParserTablePropertiesDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list;
    if (!ParserList(
            std::make_unique<ParserTablePropertyDeclaration>(),
                    std::make_unique<ParserToken>(TokenType::Comma), false)
            .parse(pos, list, expected))
        return false;

    ASTPtr columns = make_intrusive<ASTExpressionList>();
    ASTPtr indices = make_intrusive<ASTExpressionList>();
    ASTPtr constraints = make_intrusive<ASTExpressionList>();
    ASTPtr projections = make_intrusive<ASTExpressionList>();
    ASTPtr primary_key;
    ASTPtr primary_key_from_columns;

    for (const auto & elem : list->children)
    {
        if (auto * cd = elem->as<ASTColumnDeclaration>())
        {
            if (cd->primary_key_specifier)
            {
                if (!primary_key_from_columns)
                    primary_key_from_columns = makeASTOperator("tuple");
                auto column_identifier = make_intrusive<ASTIdentifier>(cd->name);
                primary_key_from_columns->children[0]->as<ASTExpressionList>()->children.push_back(column_identifier);
                /// The specifier's meaning has been transferred to `primary_key_from_columns`, which
                /// `ParserCreateQuery` normalizes into the storage definition. Clear it so the final
                /// AST is the same as for a query that spelled the primary key at the table level:
                /// formatting prints only the storage-level PRIMARY KEY, so a kept flag would not
                /// survive a format+parse round trip, and the tree hash would differ.
                cd->primary_key_specifier = false;
            }
            columns->children.push_back(elem);
        }
        else if (elem->as<ASTIndexDeclaration>())
            indices->children.push_back(elem);
        else if (elem->as<ASTConstraintDeclaration>())
            constraints->children.push_back(elem);
        else if (elem->as<ASTProjectionDeclaration>())
            projections->children.push_back(elem);
        else if (elem->as<ASTForeignKeyDeclaration>())
        {
            /// Ignore the foreign key node
            continue;
        }
        else if (elem->as<ASTIdentifier>() || elem->as<ASTFunction>())
        {
            if (primary_key)
            {
                /// Multiple primary keys are not allowed.
                return false;
            }
            primary_key = elem;
        }
        else
            return false;
    }

    auto res = make_intrusive<ASTColumns>();

    if (!columns->children.empty())
        res->set(res->columns, columns);
    if (!indices->children.empty())
        res->set(res->indices, indices);
    if (!constraints->children.empty())
        res->set(res->constraints, constraints);
    if (!projections->children.empty())
        res->set(res->projections, projections);
    if (primary_key)
        res->set(res->primary_key, primary_key);
    if (primary_key_from_columns)
        res->set(res->primary_key_from_columns, primary_key_from_columns);

    node = res;

    return true;
}

bool ParserStorageOrderByClause::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserStorageOrderByExpressionList order_list_p(allow_order);
    ParserStorageOrderByElement order_elem_p(allow_order);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ASTPtr order_by;

    /// Check possible ASC|DESC suffix for single key
    if (order_elem_p.parse(pos, order_by, expected))
    {
        /// This is needed because 'order by (x, y)' is parsed as tuple.
        /// We can remove ASTStorageOrderByElement if no ASC|DESC suffix was specified.
        if (const auto * elem = order_by->as<ASTStorageOrderByElement>(); elem && elem->direction > 0)
            order_by = elem->children.front();

        node = order_by;
        return true;
    }

    /// Check possible ASC|DESC suffix for a list of keys
    if (pos->type == TokenType::BareWord && std::string_view(pos->begin, pos->size()) == "tuple")
        ++pos;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!order_list_p.parse(pos, order_by, expected))
        order_by = make_intrusive<ASTExpressionList>();

    if (!s_rparen.ignore(pos, expected))
        return false;

    /// Remove ASTStorageOrderByElement wrappers when ALL elements have default (ASC) direction.
    /// We must unwrap all-or-nothing because KeyDescription expects either all children to be
    /// wrapped in ASTStorageOrderByElement, or none of them.
    bool all_default_direction = true;
    for (const auto & child : order_by->children)
    {
        if (const auto * elem = child->as<ASTStorageOrderByElement>(); !elem || elem->direction < 0)
        {
            all_default_direction = false;
            break;
        }
    }
    if (all_default_direction)
    {
        for (auto & child : order_by->children)
        {
            if (const auto * elem = child->as<ASTStorageOrderByElement>())
                child = elem->children.front();
        }
    }

    auto tuple_function = make_intrusive<ASTFunction>();
    tuple_function->name = "tuple";
    tuple_function->arguments = std::move(order_by);
    tuple_function->children.push_back(tuple_function->arguments);

    node = std::move(tuple_function);
    return true;
}

bool ParserStorage::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_engine(Keyword::ENGINE);
    ParserToken s_eq(TokenType::Equals);
    ParserKeyword s_partition_by(Keyword::PARTITION_BY);
    ParserKeyword s_primary_key(Keyword::PRIMARY_KEY);
    ParserKeyword s_order_by(Keyword::ORDER_BY);
    ParserKeyword s_sample_by(Keyword::SAMPLE_BY);
    ParserKeyword s_ttl(Keyword::TTL);
    ParserKeyword s_settings(Keyword::SETTINGS);
    ParserKeyword s_unique_key(Keyword::UNIQUE_KEY);

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p;
    ParserExpression expression_p;
    ParserStorageOrderByClause order_by_p(/*allow_order_*/ true);
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserTTLExpressionList parser_ttl_list;
    ParserStringLiteral string_literal_parser;

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr sample_by;
    ASTPtr ttl_table;
    ASTPtr unique_key;
    ASTPtr settings;

    bool storage_like = false;
    bool parsed_engine_keyword = s_engine.ignore(pos, expected);

    if (parsed_engine_keyword)
    {
        s_eq.ignore(pos, expected);

        if (!ident_with_optional_params_p.parse(pos, engine, expected))
            return false;
        storage_like = true;
    }

    while (true)
    {
        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (order_by_p.parse(pos, order_by, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        if (!unique_key && s_unique_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, unique_key, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
            {
                storage_like = true;
                continue;
            }
            return false;
        }

        /// For TABLE we only allow SETTINGS without ENGINE in order to support default_table_engine
        /// Special handling is provided in InterpreterSetQuery::applySettingsFromQuery to differentiate between engine and query settings
        /// For DATABASE we currently don't allow SETTINGS without ENGINE (it could be implemented in a similar fashion if necessary)
        if ((engine_kind == TABLE_ENGINE || parsed_engine_keyword) && s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
            storage_like = true;
        }

        break;
    }

    // If any part of storage definition is found create storage node
    if (!storage_like)
        return false;

    if (engine)
    {
        switch (engine_kind)
        {
            case EngineKind::TABLE_ENGINE:
                engine->as<ASTFunction &>().setKind(ASTFunction::Kind::TABLE_ENGINE);
                break;

            case EngineKind::DATABASE_ENGINE:
                engine->as<ASTFunction &>().setKind(ASTFunction::Kind::DATABASE_ENGINE);
                break;
        }
    }

    auto storage = make_intrusive<ASTStorage>();
    /// The order of `set()` calls below determines the order of `children`,
    /// because `set()` appends. It must match `ASTStorage::normalizeChildrenOrder`
    /// (and therefore `ASTStorage::formatImpl`), otherwise format-and-reparse
    /// produces a different `children` order, breaking the round-trip check
    /// in `executeQueryImpl` with `Inconsistent AST formatting`.
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->unique_key, unique_key);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);
    storage->set(storage->settings, settings);

    node = storage;
    return true;
}


bool ParserCreateTableQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_table(Keyword::TABLE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
    ParserKeyword s_from(Keyword::FROM);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_not(Keyword::NOT);
    ParserKeyword s_replicated(Keyword::REPLICATED);
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserSelectWithUnionQuery select_p;
    /// Parse the table function after AS in the table-function mode, so that a trailing
    /// SETTINGS clause is accepted: CREATE TABLE ... AS remote(..., SETTINGS skip_unavailable_shards = 1)
    ParserFunction table_function_p{/*allow_function_parameters_=*/ true, /*is_table_function_=*/ true};
    ParserNameList names_p;
    ParserSQLSecurity sql_security_p;

    ASTPtr table;
    ASTPtr to_inner_uuid;
    ASTPtr columns_list;
    boost::intrusive_ptr<ASTStorage> storage;
    bool is_time_series_table = false;
    ASTPtr targets;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr as_table_function;
    ASTPtr select;
    ASTPtr from_path;
    ASTPtr sql_security;

    String cluster_str;
    bool attach = false;
    bool replace = false;
    bool or_replace = false;
    bool if_not_exists = false;
    bool is_temporary = false;
    bool is_create_empty = false;
    bool is_clone_as = false;

    if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
            replace = or_replace = true;
    }
    else if (s_attach.ignore(pos, expected))
        attach = true;
    else if (s_replace.ignore(pos, expected))
        replace = true;
    else
        return false;

    if (s_temporary.ignore(pos, expected))
        is_temporary = true;
    if (!s_table.ignore(pos, expected))
        return false;

    if (!replace && !or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (ParserKeyword{Keyword::TO_INNER_UUID}.ignore(pos, expected))
    {
        ParserStringLiteral literal_p;
        if (!literal_p.parse(pos, to_inner_uuid, expected))
            return false;
    }

    std::optional<bool> attach_as_replicated = std::nullopt;
    if (attach)
    {
        if (s_from.ignore(pos, expected))
        {
            ParserStringLiteral from_path_p;
            if (!from_path_p.parse(pos, from_path, expected))
                return false;
        } else if (s_as.ignore(pos, expected))
        {
            if (s_not.ignore(pos, expected))
                attach_as_replicated = false;
            if (!s_replicated.ignore(pos, expected))
                return false;
            if (!attach_as_replicated.has_value())
                attach_as_replicated = true;
        }
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto * table_id = table->as<ASTTableIdentifier>();
    rejectNilUUIDClause(attach, table_id->has_uuid, table_id->uuid);

    /// A shortcut for ATTACH a previously detached table.
    bool short_attach = attach && !from_path;
    if (short_attach && (!pos.isValid() || pos.get().type == TokenType::Semicolon))
    {
        /// The short `ATTACH` form takes the whole table definition from the stored metadata, so it has
        /// nowhere to keep the parsed `TO INNER UUID` value: only the presence flag would survive, and
        /// `formatQueryImpl` prints the clause from `targets`, which this form never builds. The clause
        /// would therefore be silently dropped by formatting - and the query is rejected downstream
        /// anyway (`InterpreterCreateQuery` refuses to change the definition of a short `ATTACH`).
        /// Reject it here so that no `ASTCreateQuery` that cannot be formatted back is ever produced.
        if (to_inner_uuid)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ATTACH applies the table definition from stored metadata, so a 'TO INNER UUID' clause "
                "cannot be specified in the query itself");

        auto query = make_intrusive<ASTCreateQuery>();
        node = query;

        query->attach = attach;
        query->if_not_exists = if_not_exists;
        query->cluster = cluster_str;

        query->database = table_id->getDatabase();
        query->table = table_id->getTable();
        query->uuid = table_id->uuid;
        query->has_uuid = table_id->uuid != UUIDHelpers::Nil;
        query->has_uuid_clause = table_id->has_uuid;
        query->setIsTemporary(is_temporary);

        query->attach_as_replicated = attach_as_replicated;

        if (query->database)
            query->children.push_back(query->database);
        if (query->table)
            query->children.push_back(query->table);

        return true;
    }

    auto parse_storage = [&]
    {
        chassert(!storage);
        ASTPtr ast;
        if (!storage_p.parse(pos, ast, expected))
            return false;

        storage = boost::static_pointer_cast<ASTStorage>(ast);

        if (storage && storage->engine && (storage->engine->name == "TimeSeries"))
        {
            is_time_series_table = true;
            ParserViewTargets({ViewTarget::Samples, ViewTarget::RecentSamples, ViewTarget::Tags, ViewTarget::Metrics}).parse(pos, targets, expected);
        }

        return true;
    };

    /// Try to parse EMPTY or CLONE keywords (they can appear before or after COMMENT).
    auto try_parse_empty_or_clone = [&is_create_empty, &is_clone_as, &pos, &expected]()
    {
        if (is_create_empty || is_clone_as)
            return;
        if (ParserKeyword{Keyword::EMPTY}.ignore(pos, expected))
            is_create_empty = true;
        else if (ParserKeyword{Keyword::CLONE}.ignore(pos, expected))
            is_clone_as = true;
    };

    ASTPtr comment;

    /// List of columns.
    if (s_lparen.ignore(pos, expected))
    {
        /// Columns and all table properties (indices, constraints, projections, primary_key)
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        /// We allow a trailing comma in the columns list for user convenience.
        /// Although it diverges from the SQL standard slightly.
        s_comma.ignore(pos, expected);

        if (!s_rparen.ignore(pos, expected))
            return false;

        auto storage_parse_result = parse_storage();

        /// Accept both "EMPTY COMMENT ... AS" and "COMMENT ... EMPTY AS" orderings.
        try_parse_empty_or_clone();
        sql_security_p.parse(pos, sql_security, expected);
        comment = parseComment(pos, expected);
        try_parse_empty_or_clone();

        /// When EMPTY or CLONE was parsed, AS is required; otherwise AS is optional.
        bool has_as = false;
        if (is_create_empty || is_clone_as)
        {
            if (!ParserKeyword{Keyword::AS}.ignore(pos, expected))
                return false;
            has_as = true;
        }
        else
            has_as = ParserKeyword{Keyword::AS}.ignore(pos, expected);

        if ((storage_parse_result || is_temporary) && has_as)
        {
            if (!select_p.parse(pos, select, expected))
                return false;
        }

        if (!storage_parse_result && !is_temporary && has_as)
        {
            if (!table_function_p.parse(pos, as_table_function, expected))
                return false;
        }

        /// Will set default table engine if Storage clause was not parsed
    }
    /** Create queries without list of columns:
      *  - CREATE|ATTACH TABLE ... AS ...
      *  - CREATE|ATTACH TABLE ... ENGINE = engine
      */
    else
    {
        parse_storage();

        try_parse_empty_or_clone();
        sql_security_p.parse(pos, sql_security, expected);
        if (!comment)
            comment = parseComment(pos, expected);
        try_parse_empty_or_clone();

        /// When EMPTY or CLONE was parsed, AS is required; otherwise AS is optional.
        bool has_as = false;
        if (is_create_empty || is_clone_as)
        {
            if (!ParserKeyword{Keyword::AS}.ignore(pos, expected))
                return false;
            has_as = true;
        }
        else
            has_as = ParserKeyword{Keyword::AS}.ignore(pos, expected);

        /// CREATE|ATTACH TABLE ... AS ...
        if (has_as)
        {
            if (!select_p.parse(pos, select, expected)) /// AS SELECT ...
            {
                /// ENGINE can not be specified for table functions.
                if (storage || !table_function_p.parse(pos, as_table_function, expected))
                {
                    /// AS [db.]table
                    if (!name_p.parse(pos, as_table, expected))
                        return false;

                    if (s_dot.ignore(pos, expected))
                    {
                        as_database = as_table;
                        if (!name_p.parse(pos, as_table, expected))
                            return false;
                    }

                    /// Optional - ENGINE can be specified.
                    if (!storage)
                        parse_storage();
                }
            }
        }
    }

    if (select || as_table || as_table_function)
    {
        auto select_comment = parseComment(pos, expected);
        if (comment && select_comment)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Comment for a table cannot be specified both before and after AS; please use only one");
        if (!comment)
            comment = select_comment;
    }
    else if (!comment)
        comment = parseComment(pos, expected);

    /// `AS table` and `AS table_function` are formatted before the SQL SECURITY clause position,
    /// so allowing them together would produce text that does not parse back.
    if (sql_security && (as_table || as_table_function))
        return false;

    auto query = make_intrusive<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->attach_as_replicated = attach_as_replicated;
    query->replace_table = replace;
    query->create_or_replace = or_replace;
    query->if_not_exists = if_not_exists;
    query->setIsTemporary(is_temporary);
    query->is_time_series_table = is_time_series_table;

    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->has_uuid = table_id->uuid != UUIDHelpers::Nil;
    query->has_uuid_clause = table_id->has_uuid;
    query->has_inner_uuid_clause = to_inner_uuid != nullptr;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->set(query->columns_list, columns_list);
    query->set(query->storage, storage);
    query->set(query->as_table_function, as_table_function);

    /// A table created from a table function has no storage definition of its own, the same rule
    /// that rejects an explicit `ENGINE` above, so the one synthesized below is formatted after the
    /// table function, where the grammar has no production for it and metadata cannot be read back.
    if (query->as_table_function && query->columns_list
        && (query->columns_list->primary_key || query->columns_list->primary_key_from_columns))
        throw Exception(
            ErrorCodes::SYNTAX_ERROR, "PRIMARY KEY is not allowed in the column list of a table created from a table function");

    /// Normalize a PRIMARY KEY declared inside the column list into the storage definition
    /// before the comment child is appended: when there is no explicit ENGINE clause, the
    /// storage node is synthesized here, and it must land in `children` where a fresh parse
    /// of the formatted query would put it - before the comment - or the tree hash would
    /// not survive a format+parse round trip.
    if (query->columns_list && query->columns_list->primary_key)
    {
        /// If engine is not set will use default one
        if (!query->storage)
            query->set(query->storage, make_intrusive<ASTStorage>());
        else if (query->storage->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");

        query->storage->set(query->storage->primary_key, query->columns_list->primary_key->ptr());
        /// Remove from columns_list: ASTColumns::formatImpl does not output primary_key,
        /// so keeping it causes AST inconsistency after format+reparse.
        query->columns_list->reset(query->columns_list->primary_key);
        /// Normalize children order: `set()` always appends, but the canonical order
        /// (used by clone/format) expects primary_key before order_by.
        query->storage->normalizeChildrenOrder();
    }

    if (query->columns_list && (query->columns_list->primary_key_from_columns))
    {
        /// If engine is not set will use default one
        if (!query->storage)
            query->set(query->storage, make_intrusive<ASTStorage>());
        else if (query->storage->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");

        query->storage->set(query->storage->primary_key, query->columns_list->primary_key_from_columns->ptr());
        /// Remove from columns_list for the same reason as above.
        query->columns_list->reset(query->columns_list->primary_key_from_columns);
        query->storage->normalizeChildrenOrder();
    }

    if (comment)
        query->set(query->comment, comment);
    if (sql_security)
        query->set(query->sql_security, sql_security);

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);

    if (to_inner_uuid)
    {
        if (!storage || !storage->engine || (storage->engine->name != "SharedSet" && storage->engine->name != "SharedJoin"))
        {
            const String engine_name = (storage && storage->engine) ? storage->engine->name : "(no engine)";
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage engine {} does not support inner UUID", engine_name);
        }

        if (targets)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "targets are already defined {}", targets->formatForErrorMessage());

        const UUID inner_uuid = parseFromString<UUID>(to_inner_uuid->as<ASTLiteral>()->value.safeGet<String>());
        if (inner_uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "TO INNER UUID cannot use a Nil UUID");

        auto view_targets = make_intrusive<ASTViewTargets>();
        view_targets->setInnerUUID(ViewTarget::To, inner_uuid);

        targets = view_targets;
    }

    query->set(query->targets, targets);
    query->is_create_empty = is_create_empty;
    query->is_clone_as = is_clone_as;

    if (from_path)
    {
        query->attach_from_path = from_path->as<ASTLiteral &>().value.safeGet<String>();
        query->has_attach_from_path = true;
    }

    return true;
}

bool ParserTableOverrideDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_table_override(Keyword::TABLE_OVERRIDE);
    ParserIdentifier table_name_p;
    ParserToken lparen_p(TokenType::OpeningRoundBracket);
    ParserToken rparen_p(TokenType::ClosingRoundBracket);
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserExpression expression_p;
    ParserTTLExpressionList parser_ttl_list;
    ParserKeyword s_columns(Keyword::COLUMNS);
    ParserKeyword s_partition_by(Keyword::PARTITION_BY);
    ParserKeyword s_primary_key(Keyword::PRIMARY_KEY);
    ParserKeyword s_order_by(Keyword::ORDER_BY);
    ParserKeyword s_sample_by(Keyword::SAMPLE_BY);
    ParserKeyword s_ttl(Keyword::TTL);
    ASTPtr table_name;
    ASTPtr columns;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr sample_by;
    ASTPtr ttl_table;

    if (is_standalone)
    {
        if (!s_table_override.ignore(pos, expected))
            return false;
        if (!table_name_p.parse(pos, table_name, expected))
            return false;
        if (!lparen_p.ignore(pos, expected))
            return false;
    }

    while (true)
    {
        if (!columns && s_columns.ignore(pos, expected))
        {
            if (!lparen_p.ignore(pos, expected))
                return false;
            if (!table_properties_p.parse(pos, columns, expected))
                return false;
            if (!rparen_p.ignore(pos, expected))
                return false;
        }


        if (!partition_by && s_partition_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, partition_by, expected))
                continue;
            return false;
        }

        if (!primary_key && s_primary_key.ignore(pos, expected))
        {
            if (expression_p.parse(pos, primary_key, expected))
                continue;
            return false;
        }

        if (!order_by && s_order_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, order_by, expected))
                continue;
            return false;
        }

        if (!sample_by && s_sample_by.ignore(pos, expected))
        {
            if (expression_p.parse(pos, sample_by, expected))
                continue;
            return false;
        }

        if (!ttl_table && s_ttl.ignore(pos, expected))
        {
            if (parser_ttl_list.parse(pos, ttl_table, expected))
                continue;
            return false;
        }

        break;
    }

    if (is_standalone && !rparen_p.ignore(pos, expected))
        return false;

    auto storage = make_intrusive<ASTStorage>();
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    auto res = make_intrusive<ASTTableOverride>();
    if (table_name)
        res->table_name = table_name->as<ASTIdentifier>()->name();
    res->is_standalone = is_standalone;
    res->set(res->storage, storage);
    if (columns)
        res->set(res->columns, columns);

    node = res;

    return true;
}

bool ParserTableOverridesDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserTableOverrideDeclaration table_override_p;
    ParserToken s_comma(TokenType::Comma);
    auto res = make_intrusive<ASTTableOverrideList>();
    auto parse_element = [&]
    {
        ASTPtr element;
        if (!table_override_p.parse(pos, element, expected))
            return false;
        auto * table_override = element->as<ASTTableOverride>();
        if (!table_override)
            return false;
        res->setTableOverride(table_override->table_name, element);
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_element, s_comma, true))
        return false;

    if (!res->children.empty())
        node = res;

    return true;
}

bool ParserCreateDatabaseQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_database(Keyword::DATABASE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_uuid(Keyword::UUID);
    ParserStorage storage_p{ParserStorage::DATABASE_ENGINE};
    ParserIdentifier name_p(true);
    ParserTableOverridesDeclarationList table_overrides_p;

    ASTPtr database;
    ASTPtr storage;
    ASTPtr table_overrides;
    UUID uuid = UUIDHelpers::Nil;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (!s_database.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!name_p.parse(pos, database, expected))
        return false;

    bool has_uuid_clause = false;
    if (s_uuid.ignore(pos, expected))
    {
        ParserStringLiteral uuid_p;
        ASTPtr ast_uuid;
        if (!uuid_p.parse(pos, ast_uuid, expected))
            return false;
        uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.safeGet<String>());
        has_uuid_clause = true;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    storage_p.parse(pos, storage, expected);
    auto comment = parseComment(pos, expected);

    if (!table_overrides_p.parse(pos, table_overrides, expected))
        return false;

    auto query = make_intrusive<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;

    query->uuid = uuid;
    query->has_uuid = uuid != UUIDHelpers::Nil;
    query->has_uuid_clause = has_uuid_clause;
    rejectNilUUIDClause(attach, has_uuid_clause, uuid);
    query->cluster = cluster_str;
    query->database = database;

    if (database)
        query->children.push_back(database);

    query->set(query->storage, storage);
    if (comment)
        query->set(query->comment, comment);
    if (table_overrides && !table_overrides->children.empty())
        query->set(query->table_overrides, table_overrides);

    return true;
}


bool ParserCreateViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
    ParserCompoundIdentifier to_table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ false);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_materialized(Keyword::MATERIALIZED);
    ParserKeyword s_populate(Keyword::POPULATE);
    ParserKeyword s_empty(Keyword::EMPTY);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_to{Keyword::TO};
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserAliasesExpressionList expr_list_aliases;
    ParserSelectWithUnionQuery select_p;
    ParserNameList names_p;
    ParserSQLSecurity sql_security_p;

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr to_inner_uuid;
    ASTPtr columns_list;
    ASTPtr aliases_list;
    ASTPtr storage;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;
    ASTPtr sql_security;
    ASTPtr refresh_strategy;

    String cluster_str;
    bool attach = false;
    bool if_not_exists = false;
    bool is_ordinary_view = false;
    bool is_materialized_view = false;
    bool is_populate = false;
    bool is_create_empty = false;
    bool replace_view = false;
    bool is_temporary = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    /// VIEW or MATERIALIZED VIEW
    if (s_or_replace.ignore(pos, expected))
    {
        replace_view = true;
    }

    sql_security_p.parse(pos, sql_security, expected);

    if (s_materialized.ignore(pos, expected))
    {
        is_materialized_view = true;
    }
    else
        is_ordinary_view = true;

    if (!replace_view && !is_materialized_view && s_temporary.ignore(pos, expected))
    {
        is_temporary = true;
    }

    if (!s_view.ignore(pos, expected))
        return false;

    if (!replace_view && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (ParserKeyword{Keyword::REFRESH}.ignore(pos, expected))
    {
        // REFRESH only with materialized views
        if (!is_materialized_view)
            return false;
        if (!ParserRefreshStrategy{}.parse(pos, refresh_strategy, expected))
            return false;
    }

    if (is_materialized_view && ParserKeyword{Keyword::TO_INNER_UUID}.ignore(pos, expected))
    {
        ParserStringLiteral literal_p;
        if (!literal_p.parse(pos, to_inner_uuid, expected))
            return false;
    }
    else if (is_materialized_view && ParserKeyword{Keyword::TO}.ignore(pos, expected))
    {
        // TO [db.]table
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected)) // Parsing cases like CREATE VIEW (a Int64, b Int64) (a, b) SELECT ...
    {
        bool has_aliases = false;
        if (!table_properties_p.parse(pos, columns_list, expected))
        {
            if (!expr_list_aliases.parse(pos, aliases_list, expected))
                return false;
            else
                has_aliases = true;
        }
        else
        {
            if (!s_rparen.ignore(pos, expected))
                return false;
            if (s_lparen.ignore(pos, expected))
            {
                has_aliases = true;
                if (!expr_list_aliases.parse(pos, aliases_list, expected))
                    return false;
            }
        }

        if (has_aliases)
            if (!s_rparen.ignore(pos, expected))
                return false;
    }

    if (is_materialized_view)
    {
        if (!to_table)
        {
            /// Internal ENGINE for MATERIALIZED VIEW must be specified.
            /// Actually check it in Interpreter as default_table_engine can be set
            storage_p.parse(pos, storage, expected);

            if (s_populate.ignore(pos, expected))
                is_populate = true;
            else if (s_empty.ignore(pos, expected))
                is_create_empty = true;

            if (s_to.ignore(pos, expected))
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'ENGINE' and 'TO [db].[table]'");
        }
        else
        {
            if (storage_p.ignore(pos, expected))
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'TO [db].[table]' and 'ENGINE'");

            if (s_populate.ignore(pos, expected))
                is_populate = true;
            else if (s_empty.ignore(pos, expected))
            {
                if (!refresh_strategy)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'TO [db].[table]' and 'EMPTY'");

                is_create_empty = true;
            }
        }
    }

    if (!sql_security)
        sql_security_p.parse(pos, sql_security, expected);

    /// Accept both "POPULATE/EMPTY COMMENT" and "COMMENT POPULATE/EMPTY" orderings for materialized views.
    auto try_parse_populate_or_empty = [&]()
    {
        if (!is_materialized_view || is_populate || is_create_empty)
            return;
        if (!to_table)
        {
            if (s_populate.ignore(pos, expected))
                is_populate = true;
            else if (s_empty.ignore(pos, expected))
                is_create_empty = true;
        }
        else
        {
            if (s_populate.ignore(pos, expected))
                is_populate = true;
            else if (s_empty.ignore(pos, expected))
            {
                if (!refresh_strategy)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'TO [db].[table]' and 'EMPTY'");

                is_create_empty = true;
            }
        }
    };

    try_parse_populate_or_empty();
    auto comment = parseComment(pos, expected);
    try_parse_populate_or_empty();

    /// The first refresh of a refreshable materialized view already fills it with data, so 'POPULATE'
    /// would load the initial data twice (declare 'EMPTY' to skip the initial refresh instead).
    if (is_populate && refresh_strategy)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "When creating a refreshable materialized view you can't declare 'POPULATE': "
            "the first refresh fills the view (declare 'EMPTY' to skip it)");

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto select_comment = parseComment(pos, expected);
    if (comment && select_comment)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Comment for a view cannot be specified both before and after AS SELECT; please use only one");
    if (!comment)
        comment = select_comment;

    auto query = make_intrusive<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_ordinary_view = is_ordinary_view;
    query->is_materialized_view = is_materialized_view;
    query->is_populate = is_populate;
    query->is_create_empty = is_create_empty;
    query->replace_view = replace_view;
    query->setIsTemporary(is_temporary);

    auto * table_id = table->as<ASTTableIdentifier>();
    rejectNilUUIDClause(attach, table_id->has_uuid, table_id->uuid);
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->has_uuid = table_id->uuid != UUIDHelpers::Nil;
    query->has_uuid_clause = table_id->has_uuid;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->set(query->columns_list, columns_list);
    query->set(query->aliases_list, aliases_list);

    if (refresh_strategy)
        query->set(query->refresh_strategy, refresh_strategy);
    if (comment)
        query->set(query->comment, comment);
    if (sql_security)
        query->set(query->sql_security, sql_security);

    /// A PRIMARY KEY declared in the column list is normalized into the storage definition below.
    /// A plain view has no storage, and a materialized view with `TO [db].[table]` must not declare
    /// one - the same rule that rejects an explicit `ENGINE` above. Without this check the parser
    /// synthesizes a storage definition that formatting prints as a table-level `PRIMARY KEY`, and
    /// the formatted query no longer parses back - which also means such a view could not be loaded
    /// from its metadata after a restart.
    if (query->columns_list && (query->columns_list->primary_key || query->columns_list->primary_key_from_columns))
    {
        if (is_ordinary_view)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "PRIMARY KEY is not allowed in the column list of a view");
        if (to_table)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'TO [db].[table]' and 'PRIMARY KEY'");
    }

    if (query->columns_list && query->columns_list->primary_key)
    {
        /// If engine is not set will use default one
        if (!storage)
            storage = make_intrusive<ASTStorage>();
        auto & storage_ref = typeid_cast<ASTStorage &>(*storage);
        if (storage_ref.primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");
        storage_ref.set(storage_ref.primary_key, query->columns_list->primary_key->ptr());
        /// Remove from columns_list: ASTColumns::formatImpl does not output primary_key,
        /// so keeping it causes AST inconsistency after format+reparse.
        query->columns_list->reset(query->columns_list->primary_key);
        storage_ref.normalizeChildrenOrder();
    }

    if (query->columns_list && (query->columns_list->primary_key_from_columns))
    {
        /// If engine is not set will use default one
        if (!storage)
            storage = make_intrusive<ASTStorage>();
        auto & storage_ref = typeid_cast<ASTStorage &>(*storage);
        if (storage_ref.primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");
        storage_ref.set(storage_ref.primary_key, query->columns_list->primary_key_from_columns->ptr());
        /// Remove from columns_list for the same reason as above.
        query->columns_list->reset(query->columns_list->primary_key_from_columns);
        storage_ref.normalizeChildrenOrder();
    }

    boost::intrusive_ptr<ASTViewTargets> targets;
    if (to_table || to_inner_uuid || storage)
    {
        targets = make_intrusive<ASTViewTargets>();
        if (to_table)
        {
            if (!to_table->as<ASTTableIdentifier>()->isParam())
                targets->setTableID(ViewTarget::To, to_table->as<ASTTableIdentifier>()->getTableId());
            else
            {
                chassert(is_materialized_view);
                targets->setTableASTWithQueryParams(ViewTarget::To, to_table);
            }
        }
        if (to_inner_uuid)
        {
            const UUID inner_uuid = parseFromString<UUID>(to_inner_uuid->as<ASTLiteral>()->value.safeGet<String>());
            if (inner_uuid == UUIDHelpers::Nil)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "TO INNER UUID cannot use a Nil UUID");
            targets->setInnerUUID(ViewTarget::To, inner_uuid);
        }
        if (storage)
            targets->setInnerEngine(ViewTarget::To, storage);
    }

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);
    query->set(query->targets, targets);

    return true;
}

bool ParserCreateNamedCollectionQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_named_collection(Keyword::NAMED_COLLECTION);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_not_overridable(Keyword::NOT_OVERRIDABLE);
    ParserKeyword s_overridable(Keyword::OVERRIDABLE);
    ParserIdentifier name_p;
    ParserToken s_comma(TokenType::Comma);

    String cluster_str;
    bool if_not_exists = false;

    ASTPtr collection_name;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_named_collection.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!name_p.parse(pos, collection_name, expected))
        return false;


    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    SettingsChanges changes;
    std::unordered_map<String, bool> overridability;

    while (true)
    {
        if (!changes.empty() && !s_comma.ignore(pos))
            break;

        changes.push_back(SettingChange{});

        if (!ParserSetQuery::parseNameValuePair(changes.back(), pos, expected))
            return false;
        if (s_not_overridable.ignore(pos, expected))
            overridability.emplace(changes.back().name, false);
        else if (s_overridable.ignore(pos, expected))
            overridability.emplace(changes.back().name, true);
    }

    auto query = make_intrusive<ASTCreateNamedCollectionQuery>();

    tryGetIdentifierNameInto(collection_name, query->collection_name);
    query->if_not_exists = if_not_exists;
    query->changes = changes;
    query->cluster = std::move(cluster_str);
    query->overridability = overridability;

    node = query;
    return true;
}

bool ParserCreateDictionaryQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_replace(Keyword::REPLACE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_dictionary(Keyword::DICTIONARY);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserCompoundIdentifier dict_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
    ParserToken s_left_paren(TokenType::OpeningRoundBracket);
    ParserToken s_right_paren(TokenType::ClosingRoundBracket);
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_comma(TokenType::Comma);
    ParserDictionaryAttributeDeclarationList attributes_p;
    ParserDictionary dictionary_p;

    bool if_not_exists = false;
    bool replace = false;
    bool or_replace = false;

    ASTPtr name;
    ASTPtr attributes;
    ASTPtr dictionary;
    String cluster_str;

    bool attach = false;

    if (s_create.ignore(pos, expected))
    {
        if (s_or_replace.ignore(pos, expected))
        {
            replace = true;
            or_replace = true;
        }
    }
    else if (s_attach.ignore(pos, expected))
        attach = true;
    else if (s_replace.ignore(pos, expected))
        replace = true;
    else
        return false;

    if (!s_dictionary.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!dict_name_p.parse(pos, name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!attach)
    {
        if (!s_left_paren.ignore(pos, expected))
            return false;

        if (!attributes_p.parse(pos, attributes, expected))
            return false;

        /// We allow a trailing comma in the columns list for user convenience.
        /// Although it diverges from the SQL standard slightly.
        s_comma.ignore(pos, expected);

        if (!s_right_paren.ignore(pos, expected))
            return false;

        if (!dictionary_p.parse(pos, dictionary, expected))
            return false;
    }

    auto comment = parseComment(pos, expected);

    auto query = make_intrusive<ASTCreateQuery>();
    node = query;
    query->is_dictionary = true;
    query->attach = attach;
    query->create_or_replace = or_replace;
    query->replace_table = replace;

    auto * dict_id = name->as<ASTTableIdentifier>();
    rejectNilUUIDClause(attach, dict_id->has_uuid, dict_id->uuid);
    query->database = dict_id->getDatabase();
    query->table = dict_id->getTable();
    query->uuid = dict_id->uuid;
    query->has_uuid = dict_id->uuid != UUIDHelpers::Nil;
    query->has_uuid_clause = dict_id->has_uuid;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->if_not_exists = if_not_exists;
    query->set(query->dictionary_attributes_list, attributes);
    query->set(query->dictionary, dictionary);
    query->cluster = cluster_str;

    if (comment)
        query->set(query->comment, comment);

    return true;
}


bool ParserCreateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserCreateTableQuery table_p;
    ParserCreateDatabaseQuery database_p;
    ParserCreateViewQuery view_p;
    ParserCreateDictionaryQuery dictionary_p;

    return table_p.parse(pos, node, expected)
        || database_p.parse(pos, node, expected)
        || view_p.parse(pos, node, expected)
        || dictionary_p.parse(pos, node, expected);
}

}

namespace DB
{

void registerStatementCreate(StatementFactory & factory)
{
    factory.registerStatement("CREATE",
    {
        .description = R"DOCS_MD(
CREATE queries create (for example) new [databases](/reference/statements/create/database), [tables](/reference/statements/create/table) and [views](/reference/statements/create/view).
)DOCS_MD",
        .syntax = R"(
CREATE DATABASE ...
CREATE TABLE ...
CREATE VIEW ...
CREATE DICTIONARY ...
CREATE FUNCTION ...
CREATE NAMED COLLECTION ...
CREATE USER | ROLE | ROW POLICY | MASKING POLICY | QUOTA | SETTINGS PROFILE ...
)",
        .related = {"ATTACH", "DROP", "CREATE TABLE", "CREATE DATABASE", "CREATE VIEW", "CREATE DICTIONARY"},
    });

    factory.registerStatement("CREATE DATABASE",
    {
        .description = R"DOCS_MD(
Creates a new database.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

## Clauses {#clauses}

### IF NOT EXISTS {#if-not-exists}

If the `db_name` database already exists, then ClickHouse does not create a new database and:

- Doesn't throw an exception if clause is specified.
- Throws an exception if clause isn't specified.

### ON CLUSTER {#on-cluster}

ClickHouse creates the `db_name` database on all the servers of a specified cluster. More details in a [Distributed DDL](/reference/statements/distributed-ddl) article.

### ENGINE {#engine}

By default, ClickHouse uses its own [Atomic](/reference/engines/database-engines/atomic) database engine. There are also [MySQL](/reference/engines/database-engines/mysql), [PostgresSQL](/reference/engines/database-engines/postgresql), [MaterializedPostgreSQL](/reference/engines/database-engines/materialized-postgresql), [Replicated](/reference/engines/database-engines/replicated), [SQLite](/reference/engines/database-engines/sqlite).

### COMMENT {#comment}

You can add a comment to the database when you are creating it.

The comment is supported for all database engines.

**Syntax**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Example**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

### SETTINGS {#settings}

#### lazy_load_tables {#lazy-load-tables}

When enabled, tables are not fully loaded during database startup. Instead, a lightweight proxy is created for each table and the real table engine is materialized on first access. This reduces startup time and memory usage for databases with many tables where only a subset is actively queried.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

Applies to database engines that store table metadata on disk (e.g. `Atomic`, `Ordinary`). Views, materialized views, dictionaries, and tables backed by table functions are always loaded eagerly regardless of this setting.

**When to use:** This setting is useful for databases with a large number of tables (hundreds or thousands) where only a subset is actively queried. It reduces server startup time and memory usage by deferring the creation of table engine objects, scanning of data parts, and initialization of background threads until first access.

**Impact on `system.tables`:**

- Before a table is accessed, `system.tables` shows its engine as `TableProxy`. After first access, it shows the real engine name (e.g. `MergeTree`).
- Columns like `total_rows` and `total_bytes` return `NULL` for unloaded tables because the real storage has not been created yet.

**Interaction with DDL operations:**

- `SELECT`, `INSERT`, `ALTER`, `DROP` transparently trigger loading of the real table engine on first use.
- `RENAME TABLE` works without triggering a load.
- Once a table is loaded, it stays loaded for the lifetime of the server process.

**Limitations:**

- Monitoring tools that rely on `system.tables` metadata (e.g. `total_rows`, `engine`) may see incomplete information for unloaded tables.
- The first query to an unloaded table incurs a one-time loading cost (parsing the stored `CREATE TABLE` statement and initializing the engine).

Default value: `0` (disabled).
)DOCS_MD",
        .syntax = R"(
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
)",
        .parent = "CREATE",
        .related = {"CREATE", "CREATE TABLE", "DROP"},
    });

    factory.registerStatement("CREATE TABLE",
    {
        .description = R"DOCS_MD(
Creates a new table. By default, tables are created only on the current server.
Distributed DDL queries are implemented as `ON CLUSTER` clause, which is [described separately](/reference/statements/distributed-ddl).

## Syntax forms {#syntax-forms}

This query can have various syntax forms depending on the use case.

### Create a table with an explicit schema {#with-explicit-schema}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Creates a table named `table_name` in the `db` database or the current database if `db` is not set, with the structure specified in brackets and the `engine` engine.
The structure of the table is a list of column descriptions, secondary indexes, projections and constraints . If [primary key](#primary-key) is supported by the engine, it will be indicated as parameter for the table engine.

A column description is `name type` in the simplest case. Example: `RegionID UInt32`.

The modifiers that follow the type - `COMMENT`, `compression_codec`, `STATISTICS`, `TTL`, `COLLATE`, `PRIMARY KEY` and per-column `SETTINGS` - can be written in any order, and each of them at most once. For example, `RegionID UInt32 CODEC(ZSTD) COMMENT 'comment for column'` and `RegionID UInt32 COMMENT 'comment for column' CODEC(ZSTD)` are the same. Note that `SHOW CREATE TABLE` normalizes the column declaration: the modifiers that remain in it are always printed in the canonical order `COMMENT`, `CODEC`, `STATISTICS`, `TTL`, `COLLATE`, `SETTINGS`, while a per-column `PRIMARY KEY` is moved out of the column declaration into the table-level `PRIMARY KEY` clause.

Expressions can also be defined for default values (see below).

If necessary, primary key can be specified, with one or more key expressions.

Comments can be added for columns and for the table.

### Create a table with an existing tables schema {#with-a-schema-similar-to-other-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

ClickHouse supports the ability to copy the schema and data of an existing table.

For replicating the schema of an existing table:

This creates a table with the same structure as another table.

### Create a table with an existing tables schema and data {#with-a-schema-and-data-cloned-from-another-table}

For replicating the schema and data of an existing table:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

This creates a table with the same schema and data as an existing table.  After the new table is created, all partitions from `db.table` are attached to it. In other words, the data of `db.table` is cloned into `db2.table_clone` upon creation. This query is equivalent to the following:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

For both features, you can specify a different engine for the table. If the engine is not specified, the same engine will be used as for the original table (`db.table`).

### Create a table with a table function {#from-a-table-function}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Creates a table with the same result as that of the [table function](/reference/functions/table-functions/index) specified. The created table will also work in the same way as the corresponding table function that was specified.

### Create a table with a SELECT query {#from-select-query}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Creates a table with a structure like the result of the `SELECT` query, with the `engine` engine, and fills it with data from `SELECT`. Also you can explicitly specify columns description.

If the table already exists and `IF NOT EXISTS` is specified, the query won't do anything.

There can be other clauses after the `ENGINE` clause in the query. See detailed documentation on how to create tables in the descriptions of [table engines](/reference/engines/table-engines/index).

**Example**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

## Specify column default values {#default_values}

The column description can specify a default value expression in the form of `DEFAULT expr`, `MATERIALIZED expr`, or `ALIAS expr`. Example: `URLDomain String DEFAULT domain(URL)`.

The expression `expr` is optional. If it is omitted, the column type must be specified explicitly and the default value will be `0` for numeric columns, `''` (the empty string) for string columns, `[]` (the empty array) for array columns, `1970-01-01` for date columns, or `NULL` for nullable columns.

The column type of a default value column can be omitted in which case it is inferred from `expr`'s type. For example the type of column `EventDate DEFAULT toDate(EventTime)` will be date.

If both a data type and a default value expression are specified, an implicit type casting function inserted which converts the expression to the specified type. Example: `Hits UInt32 DEFAULT 0` is internally represented as `Hits UInt32 DEFAULT toUInt32(0)`.

A default value expression `expr` may reference arbitrary table columns and constants. ClickHouse checks that changes of the table structure do not introduce loops in the expression calculation. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

### DEFAULT {#default}

`DEFAULT expr`

Normal default value. If the value of such a column is not specified in an INSERT query, it is computed from `expr`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### MATERIALIZED {#materialized}

`MATERIALIZED expr`

Materialized expression. Values of such columns are automatically calculated according to the specified materialized expression when rows are inserted. Values cannot be explicitly specified during `INSERT`s.

Also, default value columns of this type are not included in the result of `SELECT *`. This is to preserve the invariant that the result of a `SELECT *` can always be inserted back into the table using `INSERT`. This behavior can be disabled with setting `asterisk_include_materialized_columns`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### EPHEMERAL {#ephemeral}

`EPHEMERAL [expr]`

Ephemeral column. Columns of this type are not stored in the table and it is not possible to SELECT from them. The only purpose of ephemeral columns is to build default value expressions of other columns from them.

An insert without explicitly specified columns will skip columns of this type. This is to preserve the invariant that the result of a `SELECT *` can always be inserted back into the table using `INSERT`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

### ALIAS {#alias}

`ALIAS expr`

Calculated columns (synonym). Column of this type are not stored in the table and it is not possible to INSERT values into them.

When SELECT queries explicitly reference columns of this type, the value is computed at query time from `expr`. By default, `SELECT *` excludes ALIAS columns. This behavior can be disabled with setting `asterisk_include_alias_columns`.

When using the ALTER query to add new columns, old data for these columns is not written. Instead, when reading old data that does not have values for the new columns, expressions are computed on the fly by default. However, if running the expressions requires different columns that are not indicated in the query, these columns will additionally be read, but only for the blocks of data that need it.

If you add a new column to a table but later change its default expression, the values used for old data will change (for data where values were not stored on the disk). Note that when running background merges, data for columns that are missing in one of the merging parts is written to the merged part.

It is not possible to set default values for elements in nested data structures.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

## Use NULL or NOT NULL modifiers {#null-or-not-null-modifiers}

`NULL` and `NOT NULL` modifiers after data type in column definition allow or do not allow it to be [Nullable](/reference/data-types/nullable).

If the type is not `Nullable` and if `NULL` is specified, it will be treated as `Nullable`; if `NOT NULL` is specified, then no. For example, `INT NULL` is the same as `Nullable(INT)`. If the type is `Nullable` and `NULL` or `NOT NULL` modifiers are specified, the exception will be thrown.

See also [data_type_default_nullable](/reference/settings/session-settings/other#data_type_default_nullable) setting.

## Primary key {#primary-key}

You can define a [primary key](/reference/engines/table-engines/mergetree-family/mergetree#primary-keys-and-indexes-in-queries) when creating a table. A primary key can be specified in two ways:

<Columns cols={2}>
<div>

**Inside the column list**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```
</div>
<div>

**Outside the column list**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```
</div>
</Columns>

<Tip>
You can't combine both ways in one query.
</Tip>

## Specify table constraints {#constraints}

Along with columns descriptions, constraints could be defined:

### CONSTRAINT {#constraint}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` could by any boolean expression. If constraints are defined for the table, each of them will be checked for every row in `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Adding large amount of constraints can negatively affect performance of big `INSERT` queries.

Existing constraints across all tables can be inspected via the [`system.constraints`](/reference/system-tables/constraints) table.

### ASSUME {#assume}

The `ASSUME` clause is used to define a `CONSTRAINT` on a table that is assumed to be true. This constraint can then be used by the optimizer to enhance the performance of SQL queries.

Take this example where `ASSUME CONSTRAINT` is used in the creation of the `users_a` table:

```sql
CREATE TABLE users_a (
    uid Int16,
    name String,
    age Int16,
    name_len UInt8 MATERIALIZED length(name),
    CONSTRAINT c1 ASSUME length(name) = name_len
)
ENGINE=MergeTree
ORDER BY (name_len, name);
```

Here, `ASSUME CONSTRAINT` is used to assert that the `length(name)` function always equals the value of the `name_len` column. This means that whenever `length(name)` is called in a query, ClickHouse can replace it with `name_len`, which should be faster because it avoids calling the `length()` function.

Then, when executing the query `SELECT name FROM users_a WHERE length(name) < 5;`, ClickHouse can optimize it to `SELECT name FROM users_a WHERE name_len < 5`; because of the `ASSUME CONSTRAINT`. This can make the query run faster because it avoids calculating the length of `name` for each row.

`ASSUME CONSTRAINT` **does not enforce the constraint**, it merely informs the optimizer that the constraint holds true. If the constraint is not actually true, the results of the queries may be incorrect. Therefore, you should only use `ASSUME CONSTRAINT` if you are sure that the constraint is true.

## Define storage time with TTL {#ttl-expression}

Defines storage time for values. Can be specified only for MergeTree-family tables. For the detailed description, see [TTL for columns and tables](/reference/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl).

## Select column compression codecs {#column_compression_codec}

{/* Landing anchors for links to sections that moved to /reference/statements/create/table/codec */}
<a id="general-purpose-codecs"></a>
<a id="none"></a>
<a id="lz4"></a>
<a id="lz4hc"></a>
<a id="zstd"></a>
<a id="zxc"></a>
<a id="zstd_qat"></a>
<a id="deflate_qpl"></a>
<a id="specialized-codecs"></a>
<a id="delta"></a>
<a id="doubledelta"></a>
<a id="gcd"></a>
<a id="gorilla"></a>
<a id="alp"></a>
<a id="fpc"></a>
<a id="sz3"></a>
<a id="t64"></a>
<a id="quantized"></a>
<a id="encryption-codecs"></a>
<a id="aes_128_gcm_siv"></a>
<a id="aes-256-gcm-siv"></a>
<a id="adaptive-codec-selection"></a>

By default, ClickHouse applies `lz4` compression in the self-managed version, and `zstd` in ClickHouse Cloud. You can also define the compression method for each individual column in the `CREATE TABLE` query:

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

For the available general purpose, specialized and encryption codecs, see [Column compression codecs](/reference/statements/create/table/codec).

## Create temporary tables {#temporary-tables}

ClickHouse supports temporary tables, which disappear when the session ends. For details, see [CREATE TEMPORARY TABLE](/reference/statements/create/table/temporary-table).

## Update a table atomically with REPLACE TABLE {#replace-table}

{/* Landing anchors for links to sections that moved to /reference/statements/create/table/replace-table */}
<a id="syntax"></a>
<a id="examples"></a>

The `REPLACE` statement allows you to update a table [atomically](/concepts/core-concepts/glossary#atomicity). For details, see [REPLACE TABLE](/reference/statements/create/table/replace-table).

## Add a table comment {#comment-clause}

You can add a comment to the table when creating it.

**Syntax**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

<Note>
The `COMMENT` clause must be specified **after** any storage-specific clauses such as `PARTITION BY`, `ORDER BY`, and storage-specific `SETTINGS`.

After the `COMMENT` clause, only query-specific `SETTINGS` (like `max_threads`, etc.) will be parsed, not storage-related settings.

This means the correct clause order is:
- `ENGINE`
- storage clauses
- `COMMENT`
- query settings (if any)
</Note>

**Example**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

## Related content {#related-content}

- Blog: [Optimizing ClickHouse with Schemas and Codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
)DOCS_MD",
        .syntax = R"(
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] ...
) ENGINE = engine
    [COMMENT 'comment for table']

CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], ...)] ENGINE = engine AS SELECT ...
)",
        .parent = "CREATE",
        .related = {"CREATE", "CREATE TEMPORARY TABLE", "REPLACE TABLE", "CODEC", "ALTER", "DROP"},
    });

    factory.registerStatement("CREATE TEMPORARY TABLE",
    {
        .description = R"DOCS_MD(
## Temporary table support {#temporary-table-support}

<Note>
Please note that temporary tables are not replicated. As a result, there is no guarantee that data inserted into a temporary table will be available in other replicas. The primary use case where temporary tables can be useful is for querying or joining small external datasets during a single session.
</Note>

ClickHouse supports temporary tables which have the following characteristics:

- Temporary tables disappear when the session ends, including if the connection is lost.
- A temporary table uses the Memory table engine when engine is not specified and it may use any table engine except Replicated and `KeeperMap` engines.
- The DB can't be specified for a temporary table. It is created outside of databases.
- Impossible to create a temporary table with distributed DDL query on all cluster servers (by using `ON CLUSTER`): this table exists only in the current session.
- If a temporary table has the same name as another one and a query specifies the table name without specifying the DB, the temporary table will be used.
- For distributed query processing, temporary tables with Memory engine used in a query are passed to remote servers.

## Syntax {#syntax}

To create a temporary table, use the following syntax:

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

In most cases, temporary tables are not created manually, but when using external data for a query, or for distributed `(GLOBAL) IN`. For more information, see the appropriate sections

It's possible to use tables with [ENGINE = Memory](/reference/engines/table-engines/special/memory) instead of temporary tables.
)DOCS_MD",
        .syntax = R"(
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] ...
) [ENGINE = engine]
)",
        .parent = "CREATE TABLE",
        .related = {"CREATE TABLE", "DROP"},
    });

    factory.registerStatement("REPLACE TABLE",
    {
        .description = R"DOCS_MD(
## Overview {#overview}

The `REPLACE` statement allows you to update a table [atomically](/concepts/core-concepts/glossary#atomicity).

<Note>
This statement is supported for the [`Atomic`](/reference/engines/database-engines/atomic) and [`Replicated`](/reference/engines/database-engines/replicated) database engines,
which are the default database engines for ClickHouse and ClickHouse Cloud respectively.
</Note>

Ordinarily, if you need to delete some data from a table,
you can create a new table and fill it with a `SELECT` statement that does not retrieve unwanted data,
then drop the old table and rename the new one.
This approach is demonstrated in the example below:

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

Instead of the approach above, it is also possible to use `REPLACE` (given you are using the default database engines) to achieve the same result:

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

## Syntax {#syntax}

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

<Note>
All syntax forms for the [`CREATE`](/reference/statements/create/table) statement also work for this statement. Invoking `REPLACE` for a non-existent table will cause an error.
</Note>

## Examples {#examples}

<Tabs>
<Tab title="Local">

Consider the following table:

```sql
CREATE DATABASE base
ENGINE = Atomic;

CREATE OR REPLACE TABLE base.t1
(
    n UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (1, 'test');

SELECT * FROM base.t1;

┌─n─┬─s────┐
│ 1 │ test │
└───┴──────┘
```

We can use the `REPLACE` statement to clear all the data:

```sql
CREATE OR REPLACE TABLE base.t1
(
    n UInt64,
    s Nullable(String)
)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (2, null);

SELECT * FROM base.t1;

┌─n─┬─s──┐
│ 2 │ \N │
└───┴────┘
```

Or we can use the `REPLACE` statement to change the table structure:

```sql
REPLACE TABLE base.t1 (n UInt64)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (3);

SELECT * FROM base.t1;

┌─n─┐
│ 3 │
└───┘
```
</Tab>
<Tab title="Cloud">

Consider the following table on ClickHouse Cloud:

```sql
CREATE DATABASE base;

CREATE OR REPLACE TABLE base.t1
(
    n UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (1, 'test');

SELECT * FROM base.t1;

1    test
```

We can use the `REPLACE` statement to clear all the data:

```sql
CREATE OR REPLACE TABLE base.t1
(
    n UInt64,
    s Nullable(String)
)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (2, null);

SELECT * FROM base.t1;

2
```

Or we can use the `REPLACE` statement to change the table structure:

```sql
REPLACE TABLE base.t1 (n UInt64)
ENGINE = MergeTree
ORDER BY n;

INSERT INTO base.t1 VALUES (3);

SELECT * FROM base.t1;

3
```
</Tab>
</Tabs>
)DOCS_MD",
        .syntax = R"(
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
)",
        .parent = "CREATE TABLE",
        .related = {"CREATE TABLE", "EXCHANGE", "RENAME"},
    });

    factory.registerStatement("CODEC",
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

By default, ClickHouse applies `lz4` compression in the self-managed version, and `zstd` in ClickHouse Cloud.

For `MergeTree`-engine family you can change the default compression method in the [compression](/reference/settings/server-settings/settings/other#compression) section of a server configuration.

You can also define the compression method for each individual column in the [`CREATE TABLE`](/reference/statements/create/table) query.

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

The `Default` codec can be specified to reference default compression which may depend on different settings (and properties of data) in runtime.
Example: `value UInt64 CODEC(Default)` — the same as lack of codec specification.
See also [Adaptive Codec Selection](#adaptive-codec-selection).

Also you can remove current CODEC from the column and use default compression from config.xml:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Codecs can be combined in a pipeline, for example, `CODEC(Delta, Default)`.

<Tip>
You can't decompress ClickHouse database files with external utilities like `lz4`. Instead, use the special [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) utility.
</Tip>

Compression is supported for the following table engines:

- [MergeTree](/reference/engines/table-engines/mergetree-family/mergetree) family. Supports column compression codecs and selecting the default compression method by [compression](/reference/settings/server-settings/settings/other#compression) settings.
- [Log](/reference/engines/table-engines/log-family/index) family. Uses the `lz4` compression method by default and supports column compression codecs.
- [Set](/reference/engines/table-engines/special/set). Only supported the default compression.
- [Join](/reference/engines/table-engines/special/join). Only supported the default compression.

ClickHouse supports general purpose codecs and specialized codecs.

## General Purpose Codecs {#general-purpose-codecs}

### NONE {#none}

`NONE` — No compression.

### LZ4 {#lz4}

`LZ4` — Lossless [data compression algorithm](https://github.com/lz4/lz4) used by default. Applies LZ4 fast compression.

### LZ4HC {#lz4hc}

`LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` applies the default level. Possible levels: \[1, 12\]. Recommended level range: \[4, 9\].

### ZSTD {#zstd}

`ZSTD[(level)]` — [ZSTD compression algorithm](https://en.wikipedia.org/wiki/Zstandard) with configurable `level`. Possible levels: \[1, 22\]. Default level: 1.

High compression levels are useful for asymmetric scenarios, like compress once, decompress repeatedly. Higher levels mean better compression and higher CPU usage.

### ZXC {#zxc}

<ExperimentalBadge/>

`ZXC[(level)]` — asymmetric [`zxc` compression algorithm](https://github.com/hellobertrand/zxc) with configurable `level`. Possible levels: \[1, 7\]. Default level: 3.

`ZXC` trades slow compression for very fast decompression, at a compression ratio between `LZ4` and `ZSTD`. It is a good fit for the compress-once, decompress-many pattern, and decompresses fastest on modern ARM cores. Higher levels mean better compression and slower compression, while decompression stays fast.

<Note>
This codec is experimental and requires `SET enable_zxc_codec = 1` to use.
</Note>

### Obsolete: ZSTD_QAT {#zstd_qat}

<CloudNotSupportedBadge/>

### Obsolete: DEFLATE_QPL {#deflate_qpl}

<CloudNotSupportedBadge/>

## Specialized Codecs {#specialized-codecs}

These codecs are designed to make compression more effective by exploiting specific features of the data. Some of these codecs do not compress data themselves, they instead preprocess the data such that a second compression stage using a general-purpose codec can achieve a higher data compression rate.

### Delta {#delta}

`Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. `delta_bytes` is the maximum size of raw values, the default value is `sizeof(type)`. Specifying `delta_bytes` as an argument is deprecated and support will be removed in a future release. Delta is a data preparation codec, i.e. it cannot be used stand-alone.

### DoubleDelta {#doubledelta}

`DoubleDelta(bytes_size)` — Calculates delta of deltas and writes it in compact binary form. The `bytes_size` has a similar meaning than `delta_bytes` in [Delta](#delta) codec. Specifying `bytes_size` as an argument is deprecated and support will be removed in a future release. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any numeric type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-bit deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta is a data preparation codec, i.e. it cannot be used stand-alone.

### GCD {#gcd}

`GCD()` - - Calculates the greatest common denominator (GCD) of the values in the column, then divides each value by the GCD. Can be used with integer, decimal and date/time columns. The codec is well suited for columns with values that change (increase or decrease) in multiples of the GCD, e.g. 24, 28, 16, 24, 8, 24 (GCD = 4). GCD is a data preparation codec, i.e. it cannot be used stand-alone.

### Gorilla {#gorilla}

`Gorilla(bytes_size)` — Calculates XOR between current and previous floating point value and writes it in compact binary form. The smaller the difference between consecutive values is, i.e. the slower the values of the series changes, the better the compression rate. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Possible `bytes_size` values: 1, 2, 4, 8, the default value is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it's 1. For additional information, see section 4.1 in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

### ALP {#alp}

<BetaBadge/>

`ALP(variant)` — Adaptive lossless compression for floating-point data. Supports `Float32` and `Float64`. For details, see [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334).

The codec accepts an optional variant argument:

- `ALP()` or `ALP(AUTO)` (default) — Uses STD and falls back to RD based on the estimated compressed size.
- `ALP(STD)` — Standard ALP variant. Represents each value as an exact scaled integer using decimal powers, then compresses the resulting integers with Frame-of-Reference and bit-packing. Non-representable values are stored as raw exceptions. Works best for numbers originating from decimals (e.g., measurements, prices).
- `ALP(RD)` — Real Doubles variant. Reinterprets each value's bit pattern and splits it into a high part (sign + exponent + top mantissa bits) and a low part. High parts are dictionary-encoded (up to 8 entries), low parts are bit-packed. Works best when many values share the same high bits.

<Note>
This codec is in beta and requires `SET enable_alp_codec = 1` to use.
</Note>

### FPC {#fpc}

`FPC(level, float_size)` - Repeatedly predicts the next floating point value in the sequence using the better of two predictors, then XORs the actual with the predicted value, and leading-zero compresses the result. Similar to Gorilla, this is efficient when storing a series of floating point values that change slowly. For 64-bit values (double), FPC is faster than Gorilla, for 32-bit values your mileage may vary. Possible `level` values: 1-28, the default value is 12.  Possible `float_size` values: 4, 8, the default value is `sizeof(type)` if type is Float. In all other cases, it's 4. For a detailed description of the algorithm see [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

### SZ3 {#sz3}

<ExperimentalBadge/>

`SZ3` or `SZ3(algorithm, error_bound_mode, error_bound)` - A lossy but error-bound codec ([SZ3 Lossy Compressor](https://szcompressor.org/)) for columns of type Float32, Float64, Array(Float32), or Array(Float64). For array columns, compression is most effective when all arrays have the same length (they are then compressed as fixed-width vectors); arrays of different lengths are still supported and are compressed as a flat sequence of values. The codec is not applicable to Map columns, because its keys would be corrupted by lossy compression. Supported values for 'algorithm' are `ALGO_LORENZO_REG`, `ALGO_INTERP_LORENZO` and `ALGO_INTERP`. Supported values for 'error_bound_mode' are `ABS`, `REL`, `PSNR` and `ABS_AND_REL`. Argument 'error_bound' is the maximum error and of type Float64.

<Note>
This codec is experimental and requires `SET enable_sz3_codec = 1` to use.
</Note>

### T64 {#t64}

`T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` and `DateTime`). At each step of its algorithm, codec takes a block of 64 values, puts them into 64x64 bit matrix, transposes it, crops the unused bits of values and returns the rest as a sequence. Unused bits are the bits, that do not differ between maximum and minimum values in the whole data part for which the compression is used.

`DoubleDelta` and `Gorilla` codecs are used in Gorilla TSDB as the components of its compressing algorithm. Gorilla approach is effective in scenarios when there is a sequence of slowly changing values with their timestamps. Timestamps are effectively compressed by the `DoubleDelta` codec, and values are effectively compressed by the `Gorilla` codec. For example, to get an effectively stored table, you can create it in the following configuration:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

### Quantized {#quantized}

<ExperimentalBadge/>

`Quantized(method, dimensions[, ...])` — A specialized codec to support approximate vector search on columns of type `Array(Float32)`, `Array(Float64)` or `Array(BFloat16)`.
It stores the original, full-precision vectors, as well as a compact *quantized code* per vector alongside.
On `MergeTree`-family tables, vector search queries with setting [`vector_search_use_quantized_codes`](/reference/settings/session-settings/vector-search#vector_search_use_quantized_codes) will scan the quantized codes to build a shortlist and subsequently rescore the results against the full-precision vectors.
This two-stage search reads fewer bytes than a normal full-precision scan at the cost of lower recall.
`dimensions` is the vector length; supported `method` values are `rabitq`, `turboquant`, `int8`, `prefix` and `product`, each a different size / accuracy / distance-function trade-off.

The codec can only be set in `CREATE TABLE`, it cannot be added, removed, or changed through `ALTER TABLE`, including with `ADD COLUMN ... CODEC(Quantized(...))`.
It cannot be chained with any other codec (not even an encryption codec such as `AES_128_GCM_SIV`).
For more details, see [Vector search with quantized codecs](/reference/engines/table-engines/mergetree-family/annindexes#vector-search-with-quantized-codecs).

<Note>
This codec is experimental and requires `SET enable_quantized_codec = 1` to use.
</Note>

```sql
SET enable_quantized_codec = 1;

CREATE TABLE vectors
(
    id UInt32,
    vec Array(BFloat16) CODEC(Quantized('rabitq', 1536))
)
ENGINE = MergeTree ORDER BY id;
```

## Encryption Codecs {#encryption-codecs}

These codecs don't actually compress data, but instead encrypt data on disk. These are only available when an encryption key is specified by [encryption](/reference/settings/server-settings/settings/other#encryption) settings. Note that encryption only makes sense at the end of codec pipelines, because encrypted data usually can't be compressed in any meaningful way.

Encryption codecs:

### AES_128_GCM_SIV {#aes_128_gcm_siv}

`CODEC('AES-128-GCM-SIV')` — Encrypts data with AES-128 in [RFC 8452](https://tools.ietf.org/html/rfc8452) GCM-SIV mode.

### AES-256-GCM-SIV {#aes-256-gcm-siv}

`CODEC('AES-256-GCM-SIV')` — Encrypts data with AES-256 in GCM-SIV mode.

These codecs use a fixed nonce and encryption is therefore deterministic. This makes it compatible with deduplicating engines such as [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) but has a weakness: when the same data block is encrypted twice, the resulting ciphertext will be exactly the same so an adversary who can read the disk can see this equivalence (although only the equivalence, without getting its content).

<Note>
Most engines including the "\*MergeTree" family create index files on disk without applying codecs. This means plaintext will appear on disk if an encrypted column is indexed.
</Note>

<Note>
If you perform a SELECT query mentioning a specific value in an encrypted column (such as in its WHERE clause), the value may appear in [system.query_log](/reference/system-tables/query_log). You may want to disable the logging.
</Note>

**Example**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<Note>
If compression needs to be applied, it must be explicitly specified. Otherwise, only encryption will be applied to data.
</Note>

**Example**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

## Adaptive Codec Selection {#adaptive-codec-selection}

<ExperimentalBadge/>

The specialized codecs above can shrink the right data dramatically, but choosing them takes expertise, and no single choice fits a column whose data changes over time. With the MergeTree setting [`enable_adaptive_codec_selection`](/reference/settings/merge-tree-settings) enabled, ClickHouse chooses for you. For columns that use the default codec (`CODEC(Default)` or no `CODEC` at all), each block is written with whichever codec would compress it smallest, chosen among the table's default codec, `NONE`, and specialized codecs suited to the column type.

<Note>
Specialized codecs are currently chosen for integers up to 64 bits, enums, dates and times, `Decimal32`/`Decimal64`, `IPv4`, and `Float32`/`Float64`. Other columns select between the default codec and `NONE` for their values.
</Note>

A block is never larger than the default codec would make it, and incompressible data is stored raw (compressing it would produce a slightly larger file that is slower to read). The work happens in the background, on merges and mutations, where the data is recompressed anyway. Insert speed is unaffected. Queries often get faster: less data is fetched from disk, every block a query reads must be decompressed first, and specialized codecs decompress faster than the default `LZ4`. Each block records the codec it was written with, so reading requires no setting, and the feature can be switched off at any time with all data remaining readable.

```sql
CREATE TABLE adaptive
(
    time DateTime,
    user_id UInt64
)
ENGINE = MergeTree
ORDER BY time
SETTINGS enable_adaptive_codec_selection = 1;

INSERT INTO adaptive SELECT toDateTime('2026-01-01') + number, cityHash64(number) FROM numbers(1000000);
OPTIMIZE TABLE adaptive FINAL;
```

You can observe how it works with the [`mergeTreeCodecBlockCounts`](/reference/functions/table-functions/mergeTreeCodecBlockCounts) table function. Here `time` grows steadily, so `T64`, which stores only the bits that vary within a block, beat the default codec on every block. `user_id` holds hashes that no codec can shrink, so its blocks were stored raw:

```sql
SELECT column, codec_block_counts FROM mergeTreeCodecBlockCounts(currentDatabase(), 'adaptive');
```

```text
   ┌─column──┬─codec_block_counts─┐
1. │ time    │ {'T64':62}         │
2. │ user_id │ {'NONE':123}       │
   └─────────┴────────────────────┘
```

## Related content {#related-content}

- Blog: [Optimizing ClickHouse with Schemas and Codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
)DOCS_MD",
        .syntax = R"(
column_name type CODEC(codec1[(arguments)][, codec2[(arguments)], ...])
)",
        .parent = "CREATE TABLE",
        .related = {"CREATE TABLE", "ALTER TABLE ... COLUMN"},
    });

    factory.registerStatement("CREATE VIEW",
    {
        .description = R"DOCS_MD(
import { DeprecatedBadge } from "/snippets/components/DeprecatedBadge/DeprecatedBadge.jsx";

Creates a new view. Views can be [normal](#normal-view), [materialized](#materialized-view), and [refreshable materialized](#refreshable-materialized-view).

## Normal View {#normal-view}

Syntax:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

Normal views do not store any data. They just perform a read from another table on each access. In other words, a normal view is nothing more than a saved query. When reading from a view, this saved query is used as a subquery in the [FROM](/reference/statements/select/from) clause.

As an example, assume you've created a view:

```sql
CREATE VIEW view AS SELECT ...
```

and written a query:

```sql
SELECT a, b, c FROM view
```

This query is fully equivalent to using the subquery:

```sql
SELECT a, b, c FROM (SELECT ...)
```

## Parameterized View {#parameterized-view}

Parameterized views are similar to normal views, but can be created with parameters which are not resolved immediately.
These views can be used with table functions, which specify the name of the view as function name and the parameter values as its arguments.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```
The above creates a view for table which can be used as table function by substituting parameters as shown below.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

Since the parameterized view depends on the parameter values, it doesn't have a schema when parameters are not provided.
That means there's no information about parameterized views in the `system.columns` table.
Also, `DESCRIBE` queries would work only if parameters are provided.

```sql
DESCRIBE view(column1=value1, column2=value2 ...)
```

## Materialized View {#materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` and `IF NOT EXISTS` are mutually exclusive: combining them is a syntax error.

### CREATE OR REPLACE MATERIALIZED VIEW {#create-or-replace-materialized-view}

`CREATE OR REPLACE MATERIALIZED VIEW` atomically replaces an existing materialized view and its inner storage table (if any). The operation requires an `Atomic` or `Replicated` database engine.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

Key behaviors:

- **Without `TO` clause**: the old inner table is dropped and a new one is created. Existing data in the inner table is lost unless `POPULATE` is specified.
- **With `TO` clause**: only the view definition is replaced; the target table and its data are unaffected.
- Compatible with `REFRESH`, `ON CLUSTER`, and all engine options. `POPULATE` is supported on `Atomic` databases only — it is rejected on `Replicated` databases (see the `POPULATE` note below).
- Requires `CREATE VIEW` and `DROP VIEW` privileges.

<Note>
`CREATE OR REPLACE MATERIALIZED VIEW` is only supported with `Atomic` or `Replicated` database engines. It is not supported with the `Ordinary` database engine.
</Note>

**Examples:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

<Tip>
Here is a step-by-step guide on using [Materialized views](/concepts/features/materialized-views/cascading-materialized-views).
</Tip>

Materialized views store data transformed by the corresponding [SELECT](/reference/statements/select/index) query.

When creating a materialized view without `TO [db].[table]`, you must specify `ENGINE` – the table engine for storing data.

When creating a materialized view with `TO [db].[table]`, you can also use `POPULATE` to backfill the target table from the existing source data (the target table may already contain data, in which case the backfilled rows are appended). `POPULATE` cannot be combined with `REFRESH`: a [refreshable materialized view](#refreshable-materialized-view) is filled by its first refresh, so `POPULATE` would load the initial data twice (use `EMPTY` to skip the first refresh instead).

A materialized view is implemented as follows: when inserting data to the table specified in `SELECT`, part of the inserted data is converted by this `SELECT` query, and the result is inserted in the view.

<Note>
Materialized views in ClickHouse use **column names** instead of column order during insertion into destination table. If some column names are not present in the `SELECT` query result, ClickHouse uses a default value, even if the column is not [Nullable](/reference/data-types/nullable). A safe practice would be to add aliases for every column when using Materialized views.

Materialized views in ClickHouse are implemented more like insert triggers. If there's some aggregation in the view query, it's applied only to the batch of freshly inserted data. Any changes to existing data of source table (like update, delete, drop partition, etc.) does not change the materialized view.

Materialized views in ClickHouse do not have deterministic behaviour in case of errors. This means that blocks that had been already written will be preserved in the destination table, but all blocks after error will not.

By default, if pushing to one of the views throws, the `INSERT` query fails. Whether the block has already reached the source table by that point is not guaranteed — it depends on insert pipeline timing, not on the view error. Retry the failed `INSERT` with insert deduplication (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`) to get exactly-once delivery to the source table and all dependent views.

Setting `materialized_views_ignore_errors=true` on the `INSERT` query only changes error reporting: each view error is logged as a warning and the `INSERT` query succeeds. Delivery to the failing view's destination is partial — blocks processed before the exception are kept, and the failing block plus any subsequent blocks are dropped from that view. Views downstream of that destination see only the blocks that did arrive, so their delivery is partial too. Sibling views (and their downstream chains) that did not throw are written to in full, and the source table is written to as usual. Because the `INSERT` reports success, the client gets no failure signal and no automatic retry is triggered; use this setting only when source-table writes must not be blocked by view-side problems (for example, `system.*_log` tables).

`materialized_views_ignore_errors` is `true` by default for `system.*_log` tables.
</Note>

If you specify `POPULATE`, the existing source table data is inserted into the view when creating it. Otherwise, the view contains only the data inserted into the source table after the view is created.

For a plain `CREATE MATERIALIZED VIEW`, `POPULATE` is **atomic** by default (setting `materialized_views_populate_atomically = 1`): the view is subscribed to new inserts of the source table and a snapshot of the existing data is taken together, under a brief exclusive lock on the source table, so that every row inserted concurrently with the population is delivered to the view **exactly once** — neither missed nor duplicated. The (possibly long-running) population then reads the pinned snapshot without holding any lock.

This is local insert-path atomicity: the exclusive lock only serializes with inserts that acquire this source table's storage lock **on the same server**, so the exactly-once guarantee covers inserts arriving through this server. It is not a cluster-wide guarantee — rows inserted on another replica of a `ReplicatedMergeTree` source, or through a distributed write path (for example, into a `Distributed` table or via `ON CLUSTER`), concurrently with the population are outside this cut and can still be missed or duplicated.

If the population fails — for example, the exclusive lock on a busy source table cannot be acquired within `lock_acquire_timeout`, or the view's `SELECT` throws while running — the just-created view is dropped and the `CREATE` query fails, leaving behind nothing of what it created, so it can simply be retried. For the `TO [db].[table]` form this rollback drops only the view, never the pre-existing target table — but rows the failed population already inserted into the target stay there, exactly as after a failed `INSERT ... SELECT` into that table, so retrying the `CREATE` inserts them again. If the backfill must be exact, retry into a truncated or fresh target table, or use a deduplicating engine such as `ReplacingMergeTree`.

<Note>
Atomicity requires the source table to support reading a pinned point-in-time snapshot — the `MergeTree` family and `Memory`. For any other source (a view, `Distributed`, `Merge`, `Buffer`, the `Log` family, or a table not in an `Atomic` database), the population falls back to the legacy, non-atomic behavior (recorded in the server log): the existing data is read with a separate, uncoordinated snapshot, so rows inserted during the population can be missed or duplicated. In that case create the view and run a separate `INSERT ... SELECT` if you need exact data. Setting `materialized_views_populate_atomically = 0` forces this legacy behavior for all sources.

Atomic population applies to plain `CREATE MATERIALIZED VIEW` only. `CREATE OR REPLACE` / `REPLACE MATERIALIZED VIEW ... POPULATE` always use the legacy, non-atomic population.

`POPULATE` is not supported with `Replicated` databases (use `database_replicated_allow_heavy_create` to override) and is not supported in ClickHouse Cloud. When it is enabled through that override, the population is always the legacy, non-atomic one — a failed population could not be rolled back consistently on all replicas.
</Note>

A `SELECT` query can contain `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` is set, data is aggregated during insertion, but only within a single packet of inserted data. The data won't be further aggregated. The exception is when using an `ENGINE` that independently performs data aggregation, such as `SummingMergeTree`.

If the materialized view uses the construction `TO [db.]name`, you can `DETACH` the view, run `ALTER` for the target table, and then `ATTACH` the previously detached (`DETACH`) view.

Views look the same as normal tables. For example, they are listed in the result of the `SHOW TABLES` query.

To delete a view, use [DROP VIEW](/reference/statements/drop#drop-view). Although `DROP TABLE` works for VIEWs as well.

## SQL security {#sql_security}

`DEFINER` and `SQL SECURITY` allow you to specify which ClickHouse user to use when executing the view's underlying query.
`SQL SECURITY` has three legal values: `DEFINER`, `INVOKER`, or `NONE`. You can specify any existing user or `CURRENT_USER` in the `DEFINER` clause.

The following table will explain which rights are required for which user in order to select from view.
Note that regardless of the SQL security option, in every case it is still required to have `GRANT SELECT ON <view>` in order to read from it.

| SQL security option | View                                                            | Materialized View                                                                                                 |
|---------------------|-----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `DEFINER alice`     | `alice` must have a `SELECT` grant for the view's source table. | `alice` must have a `SELECT` grant for the view's source table and an `INSERT` grant for the view's target table. |
| `INVOKER`           | User must have a `SELECT` grant for the view's source table.    | `SQL SECURITY INVOKER` can't be specified for materialized views.                                                 |
| `NONE`              | -                                                               | -                                                                                                                 |

<Note>
`SQL SECURITY NONE` is a deprecated option. Any user with the rights to create views with `SQL SECURITY NONE` will be able to execute any arbitrary query.
Thus, it is required to have `GRANT ALLOW SQL SECURITY NONE TO <user>` in order to create a view with this option.
</Note>

If `DEFINER`/`SQL SECURITY` aren't specified, the result depends on the [`ignore_empty_sql_security_in_create_view_query`](/reference/settings/server-settings/settings/other#ignore_empty_sql_security_in_create_view_query) server setting.

With its default value of `true`, the query is stored as written and the view gets an empty SQL security type. A normal view then runs with the permissions of the invoker, and for a materialized view with an explicitly specified target table, the access checks on that target table are skipped: inserting into the source table does not require the `INSERT` privilege on the target table, and reading from the view does not require the `SELECT` privilege on it.

With `false`, the following defaults are written into the view definition at creation time:
- `SQL SECURITY`: `INVOKER` for normal views (configurable by [`default_normal_view_sql_security`](/reference/settings/session-settings/default#default_normal_view_sql_security)) and `DEFINER` for materialized views (configurable by [`default_materialized_view_sql_security`](/reference/settings/session-settings/default#default_materialized_view_sql_security))
- `DEFINER`: `CURRENT_USER` (configurable by [`default_view_definer`](/reference/settings/session-settings/default#default_view_definer))

Refreshable materialized views always receive these defaults, regardless of the setting.

A view keeps the SQL security type from its stored definition when it is attached or reloaded at server startup, so a view stored without `DEFINER`/`SQL SECURITY` keeps the empty SQL security type.

To change SQL security for an existing view, use
```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

### Examples {#examples}
```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

## Live View {#live-view}

<DeprecatedBadge/>

This feature is deprecated and will be removed in the future.

For your convenience, the old documentation is located [here](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

## Refreshable Materialized View {#refreshable-materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```
where `interval` is a sequence of simple intervals:
```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

The `REFRESH` clause must specify at least one of `EVERY`, `AFTER`, or `DEPENDS ON`. Bare `REFRESH` (with none of these) is rejected. `REFRESH DEPENDS ON ...` without `EVERY`/`AFTER` is shorthand for `REFRESH AFTER 0 SECOND DEPENDS ON ...`; see [Refresh Dependencies](#refresh-dependencies) below.

Periodically runs the corresponding query and stores its result into a table.
* If `APPEND` is specified, each refresh inserts rows into the table without deleting existing rows. The insert is not atomic, just like a regular `INSERT INTO ... SELECT` query.
* Otherwise, each refresh atomically replaces the table's previous contents.

Differences from regular non-refreshable materialized views:
* No insert trigger. When new data is inserted into the table specified in `SELECT`, it's *not* automatically pushed to the refreshable materialized view. Instead, data insertion only takes place during the periodic or manual refresh runs.
* No restrictions on the `SELECT` query. Table functions (e.g. `url()`), views, UNION, JOIN, are all allowed.

<Note>
The settings in the `REFRESH ... SETTINGS` part of the query are refresh settings (e.g. `refresh_retries`), distinct from regular settings (e.g. `max_threads`). Regular settings can be specified using `SETTINGS` at the end of the query.
</Note>

### Refresh Schedule {#refresh-schedule}

Example refresh schedules:
```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` randomly adjusts the time of each refresh, e.g.:
```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

At most one refresh may be running at a time, for a given view. E.g. if a view with `REFRESH EVERY 1 MINUTE` takes 2 minutes to refresh, it'll just be refreshing every 2 minutes. If it then becomes faster and starts refreshing in 10 seconds, it'll go back to refreshing every minute. (In particular, it won't refresh every 10 seconds to catch up with a backlog of missed refreshes - there's no such backlog.)

Typically the first refresh is started immediately after the materialized view is created: time since last refresh is infinity, so any schedule says it's time to refresh now. If `EMPTY` is specified, this initial refresh is skipped, and the first refresh happens at the next scheduled time; e.g. for `EVERY 1 HOUR` the first refresh will happen at the end of current hour.

### In Replicated DB {#in-replicated-db}

If the refreshable materialized view is in a [Replicated database](/reference/engines/database-engines/replicated), the replicas coordinate with each other such that only one replica performs the refresh at each scheduled time. [ReplicatedMergeTree](/reference/engines/table-engines/mergetree-family/replication) table engine is required, so that all replicas see the data produced by the refresh.

In `APPEND` mode, coordination can be disabled using `SETTINGS all_replicas = 1`. This makes replicas do refreshes independently of each other. In this case ReplicatedMergeTree is not required.

In non-`APPEND` mode, only coordinated refreshing is supported. For uncoordinated, use `Atomic` database and `CREATE ... ON CLUSTER` query to create refreshable materialized views on all replicas.

The coordination is done through Keeper. The znode path is determined by [default_replica_path](/reference/settings/server-settings/settings/default-replica#default_replica_path) server setting.

### Refresh Dependencies {#refresh-dependencies}

`DEPENDS ON` synchronizes refreshes of different tables:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```
Dependent view's refresh will start only after all dependency views' refreshes complete.

To refresh immediately after another view's refresh:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```
Or equivalently:
```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

<Note>
`DEPENDS ON` only works between refreshable materialized views. In particular, if the dependency view uses `TO <table>`, make sure to use the name of the view rather than the table. If the `DEPENDS ON` list contains a regular table or non-refreshable view or has a typo, the view will never refresh and will show state `MissingDependencies` in `system.view_refreshes`. Dependencies can be changed or removed using `ALTER`, see [Changing Refresh Parameters](#changing-refresh-parameters).
</Note>

#### Using DEPENDS ON for consistent propagation latency {#using-depends-on-for-consistent-propagation-latency}

If both views use `REFRESH EVERY` with the same period, the dependency applies in each timeslot.

E.g. suppose views X and Y both use `REFRESH EVERY 1 HOUR`, and Y reads from X's output table. Without dependencies, Y would usually see X's data from previous hour's refresh. With `DEPENDS ON X`, Y's 11:00 refresh will start only after the X's 11:00 refresh completes.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

Both dependency and dependent may independently skip timeslots if refreshes run for longer than the refresh period. There's no guarantee that the dependent refreshes exactly once for each dependency refresh.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

#### Using DEPENDS ON for batched stream processing {#using-depends-on-for-batched-stream-processing}

If `REFRESH EVERY` is not used, the dependent view X refreshes if all its dependencies refreshed at least once since X's last refresh. `REFRESH AFTER T` adds a delay: the dependent will start refresh T time after the dependency completes a refresh.

Circular dependencies are allowed and useful. Consider this graph of refreshable materialized views:
 1. X takes a batch of rows from some stream and puts them in a table.
 2. Then Y and Z both read from that table, do different aggregation, and append results to other tables.
 3. After the batch is fully processed, X takes the next batch, and the cycle repeats.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

Complete example:
```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

Longer chains work as well.

This only works well when refresh coordination is enabled, i.e. the views are in Replicated or Shared database. Without coordination, server restart breaks the cycle, requiring a manual `SYSTEM REFRESH VIEW` after each restart rather than once after creating the views.

### Refresh Settings {#refresh-settings}

Available refresh settings:
* `refresh_retries` - How many times to retry if refresh query fails with an exception. If all retries fail, skip to the next scheduled refresh time. 0 means no retries, -1 means infinite retries. Default: 2.
* `refresh_retry_initial_backoff_ms` - Delay before the first retry, if `refresh_retries` is not zero. Each subsequent retry doubles the delay, up to `refresh_retry_max_backoff_ms`. Default: 100 ms.
* `refresh_retry_max_backoff_ms` - Limit on the exponential growth of delay between refresh attempts. Default: 60000 ms (1 minute).
* `all_replicas` - In a [Replicated database](/reference/engines/database-engines/replicated) with `APPEND`, controls whether all replicas refresh independently or only one replica refreshes at each scheduled time. Cannot be changed after the view is created. Default: `false`.

### Changing Refresh Parameters {#changing-refresh-parameters}

Refresh parameters of an existing refreshable materialized view are changed with [`ALTER TABLE ... MODIFY REFRESH`](/reference/statements/alter/view#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

The schedule (`EVERY` or `AFTER`) is mandatory: the statement always replaces *all* refresh parameters — schedule, `RANDOMIZE FOR`, `DEPENDS ON`, and refresh settings — with what is specified. Anything omitted is reset to its default (settings) or removed (dependencies, randomization).

<Note>
- To change only refresh settings (e.g. `refresh_retries`), repeat the existing schedule:

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

- `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` is not supported on materialized views; you must go through `MODIFY REFRESH`.

- Adding or removing `APPEND` is not supported.

- The `all_replicas` setting cannot be changed after creation.
</Note>

Examples:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

### Other operations {#other-operations}

The status of all refreshable materialized views is available in table [`system.view_refreshes`](/reference/system-tables/view_refreshes). In particular, it contains refresh progress (if running), last and next refresh time, exception message if a refresh failed.

To manually stop, start, trigger, or cancel refreshes, use [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](/reference/statements/system#managing-refreshable-materialized-views).

To wait for a refresh to complete, use [`SYSTEM WAIT VIEW`](/reference/statements/system#wait-view). In particular, useful for waiting for initial refresh after creating a view.

<Note>
Fun fact: the refresh query is allowed to read from the view that's being refreshed, seeing pre-refresh version of the data. This means you can implement Conway's game of life: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
</Note>

## Related Content {#related-content}

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

## Temporary Views {#temporary-views}

ClickHouse supports **temporary views** with the following characteristics (matching temporary tables where applicable):

* **Session-lifetime**
  A temporary view exists only for the duration of the current session. It is dropped automatically when the session ends.

* **No database**
  You **cannot** qualify a temporary view with a database name. It lives outside databases (session namespace).

* **Not replicated / no ON CLUSTER**
  Temporary objects are local to the session and **cannot** be created with `ON CLUSTER`.

* **Name resolution**
  If a temporary object (table or view) has the same name as a persistent object and a query references the name **without** a database, the **temporary** object is used.

* **Logical object (no storage)**
  A temporary view stores only its `SELECT` text (uses the `View` storage internally). It does not persist data and cannot accept `INSERT`.

* **Engine clause**
  You do **not** need to specify `ENGINE`; if provided as `ENGINE = View`, it’s ignored/treated as the same logical view.

* **Security / privileges**
  Creating a temporary view requires the privilege `CREATE TEMPORARY VIEW` which is implicitly granted by `CREATE VIEW`.

* **SHOW CREATE**
  Use `SHOW CREATE TEMPORARY VIEW view_name;` to print the DDL of a temporary view.

### Syntax {#temporary-views-syntax}

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` is **not** supported for temporary views (to match temporary tables). If you need to “replace” a temporary view, drop it and create it again.

### Examples {#temporary-views-examples}

Create a temporary source table and a temporary view on top:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

Show its DDL:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

Drop it:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

### Disallowed / limitations {#temporary-views-limitations}

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **not allowed** (use `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` → **not allowed**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **not allowed** (no database qualifier).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **not allowed** (temporary objects are session-local).
* `POPULATE`, `REFRESH`, `TO [db.table]`, inner engines, and all MV-specific clauses → **not applicable** to temporary views.

### Notes on distributed queries {#temporary-views-distributed-notes}

A temporary **view** is just a definition; there’s no data to pass around. If your temporary view references temporary **tables** (e.g., `Memory`), their data can be shipped to remote servers during distributed query execution the same way temporary tables work.

#### Example {#temporary-views-distributed-example}

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```
)DOCS_MD",
        .syntax = R"(
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']

CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO [db.]name [(columns)]]
[ENGINE = engine] [POPULATE] [REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
)",
        .parent = "CREATE",
        .related = {"CREATE", "CREATE TABLE", "ALTER TABLE ... MODIFY QUERY", "DROP"},
    });

    factory.registerStatement("CREATE DICTIONARY",
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";
import { CloudSupportedBadge } from "/snippets/components/CloudSupportedBadge/CloudSupportedBadge.jsx";

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.
ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

Dictionaries can be created in two ways:
- [With a DDL query](#creating-a-dictionary-with-a-ddl-query) (recommended)
- [With a configuration file](#creating-a-dictionary-with-a-configuration-file)

## Creating a dictionary with a DDL query {#creating-a-dictionary-with-a-ddl-query}

<CloudSupportedBadge/>

Dictionaries can be created with DDL queries.
This is the recommended method because with DDL created dictionaries:
- No additional records are added to server configuration files.
- Dictionaries can be used like first-class entities such as tables or views.
- Data can be read directly, using familiar `SELECT` syntax rather than dictionary table functions. Note that when accessing a dictionary directly via a `SELECT` statement, cached dictionary will return only cached data, while for a non-cached dictionary it will return all the data that it stores.
- Dictionaries can be easily renamed.

### Syntax {#syntax}

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Clause | Description |
|---|---|
| [Attributes](/reference/statements/create/dictionary/attributes) | Dictionary attributes are specified similarly to table columns. The only required property is the type, all others may have default values. |
| PRIMARY KEY | Defines the key column(s) for dictionary lookups. Depending on the layout, one or more attributes can be specified as keys. |
| [`SOURCE`](/reference/statements/create/dictionary/sources/overview) | Defines the data source for the dictionary (e.g. ClickHouse table, HTTP, PostgreSQL). |
| [`LAYOUT`](/reference/statements/create/dictionary/layouts/overview) | Controls how the dictionary is stored in memory (e.g. `FLAT`, `HASHED`, `CACHE`). |
| [`LIFETIME`](/reference/statements/create/dictionary/lifetime) | Sets the refresh interval for the dictionary. |
| [`ON CLUSTER`](/reference/statements/distributed-ddl) | Creates the dictionary on a cluster. Optional. |
| `SETTINGS` | Additional dictionary settings. Optional. |
| `COMMENT` | Adds a text comment to the dictionary. Optional. |

## Creating a dictionary with a configuration file {#creating-a-dictionary-with-a-configuration-file}

<CloudNotSupportedBadge/>

<Note>
Creating a dictionary with a configuration file is not applicable to ClickHouse Cloud. Please use DDL (see above), and create your dictionary as the `default` user.
</Note>

The dictionary configuration file has the following format:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/clickhouse-server/substitutions.xml</include_from>

    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

You can configure any number of dictionaries in the same file.

## Related content {#related-content}

- [Layouts](/reference/statements/create/dictionary/layouts/overview) — How dictionaries are stored in memory
- [Sources](/reference/statements/create/dictionary/sources/overview) — Connecting to data sources
- [Lifetime](/reference/statements/create/dictionary/lifetime) — Automatic refresh configuration
- [Attributes](/reference/statements/create/dictionary/attributes) — Key and attribute configuration
- [Embedded Dictionaries](/reference/statements/create/dictionary/embedded) — Built-in geobase dictionaries
- [system.dictionaries](/reference/system-tables/dictionaries) — System table with dictionary information
)DOCS_MD",
        .syntax = R"(
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    attr1 type2  [DEFAULT | EXPRESSION expr2] [HIERARCHICAL|INJECTIVE],
    ...
)
PRIMARY KEY key1[, key2]
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, ...)
COMMENT 'Comment'
)",
        .parent = "CREATE",
        .related = {"CREATE", "DROP", "SYSTEM"},
    });

    factory.registerStatement("CREATE NAMED COLLECTION",
    {
        .description = R"DOCS_MD(
Creates a new named collection.

<Note>
DDL-created named collections can be enabled on select ClickHouse Cloud services. Contact Support to confirm availability.
</Note>

**Syntax**

```sql
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster] AS
key_name1 = 'some value' [[NOT] OVERRIDABLE],
key_name2 = 'some value' [[NOT] OVERRIDABLE],
key_name3 = 'some value' [[NOT] OVERRIDABLE],
...
```

**Example**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2' OVERRIDABLE;
```

**Related statements**

- [CREATE NAMED COLLECTION](/reference/statements/alter/named-collection)
- [DROP NAMED COLLECTION](/reference/statements/drop#drop-function)

**See Also**

- [Named collections guide](/concepts/features/configuration/server-config/named-collections)
)DOCS_MD",
        .syntax = R"(
CREATE NAMED COLLECTION [IF NOT EXISTS] name [ON CLUSTER cluster]
AS key_name1 = 'some value' [[NOT] OVERRIDABLE], key_name2 = 'some value' [[NOT] OVERRIDABLE], ...
)",
        .parent = "CREATE",
        .related = {"CREATE", "ALTER NAMED COLLECTION", "DROP"},
    });

    factory.registerStatement("ATTACH",
    {
        .description = R"DOCS_MD(
Attaches a table or a dictionary, for example, when moving a database to another server.

**Syntax**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

The query does not create data on disk, but assumes that data is already in the appropriate places, and just adds information about the specified table, dictionary or database to the server. After executing the `ATTACH` query, the server will know about the existence of the table, dictionary or database.

If a table was previously detached ([DETACH](/reference/statements/detach) query), meaning that its structure is known, you can use shorthand without defining the structure.

## Attach Existing Table {#attach-existing-table}

**Syntax**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of some system tables, which are explicitly created on the server).

If the table was detached permanently, it won't be reattached at the server start, so you need to use `ATTACH` query explicitly.

## Create New Table And Attach Data {#create-new-table-and-attach-data}

### With Specified Path to Table Data {#with-specified-path-to-table-data}

The query creates a new table with provided structure and attaches table data from the provided directory in `user_files`.

**Syntax**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Example**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

### With Specified Table UUID {#with-specified-table-uuid}

This query creates a new table with provided structure and attaches data from the table with the specified UUID.
It is supported by the [Atomic](/reference/engines/database-engines/atomic) database engine.

**Syntax**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

## Attach MergeTree table as ReplicatedMergeTree {#attach-mergetree-table-as-replicatedmergetree}

Allows to attach non-replicated MergeTree table as ReplicatedMergeTree. ReplicatedMergeTree table will be created with values of `default_replica_path` and `default_replica_name` settings. It is also possible to attach a replicated table as a regular MergeTree.

Note that table's data in ZooKeeper is not affected in this query. This means you have to add metadata in ZooKeeper using `SYSTEM RESTORE REPLICA` or clear it with `SYSTEM DROP REPLICA ... FROM ZKPATH ...` after attach.

If you are trying to add a replica to an existing ReplicatedMergeTree table, keep in mind that all the local data in converted MergeTree table will be detached.

**Syntax**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**Convert table to replicated**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**Convert table to not replicated**

Get ZooKeeper path and replica name for table:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```
```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```
Attach table as not replicated and delete replica's data from ZooKeeper:
```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

## Attach Existing Dictionary {#attach-existing-dictionary}

Attaches a previously detached dictionary.

**Syntax**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

## Attach Existing Database {#attach-existing-database}

Attaches a previously detached database.

**Syntax**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```
)DOCS_MD",
        .syntax = R"(
ATTACH TABLE|VIEW|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
ATTACH TABLE [db.]name AS [NOT] REPLICATED
)",
        .related = {"DETACH", "CREATE", "DROP"},
    });
}

}
