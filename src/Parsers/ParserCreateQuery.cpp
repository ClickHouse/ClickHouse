#include <IO/ReadHelpers.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTForeignKeyDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTDataType.h>
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

    s_comment.ignore(pos, expected) && string_literal_parser.parse(pos, comment, expected);

    return comment;
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
            else if (!ParserUserNameWithHost{}.parse(pos, definer, expected))
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

    auto result = std::make_shared<ASTSQLSecurity>();
    result->is_definer_current_user = is_definer_current_user;
    result->type = type;
    if (definer)
        result->definer = typeid_cast<std::shared_ptr<ASTUserNameWithHost>>(definer);

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

    auto index = std::make_shared<ASTIndexDeclaration>(expr, type, name->as<ASTIdentifier &>().name());

    if (granularity)
        index->granularity = granularity->as<ASTLiteral &>().value.safeGet<UInt64>();
    else
    {
        auto index_type = index->getType();
        if (index_type->name == "vector_similarity")
            index->granularity = ASTIndexDeclaration::DEFAULT_VECTOR_SIMILARITY_INDEX_GRANULARITY;
        else
            index->granularity = ASTIndexDeclaration::DEFAULT_INDEX_GRANULARITY;
    }

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

    auto stat = std::make_shared<ASTStatisticsDeclaration>();
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

    auto stat = std::make_shared<ASTStatisticsDeclaration>();
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

    auto constraint = std::make_shared<ASTConstraintDeclaration>();
    constraint->name = name->as<ASTIdentifier &>().name();
    constraint->type = type;
    constraint->set(constraint->expr, expr);
    node = constraint;

    return true;
}


bool ParserProjectionDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier name_p;
    ParserProjectionSelectQuery query_p;
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ASTPtr name;
    ASTPtr query;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!query_p.parse(pos, query, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    auto projection = std::make_shared<ASTProjectionDeclaration>();
    projection->name = name->as<ASTIdentifier &>().name();
    projection->set(projection->query, query);
    node = projection;

    return true;
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

    auto foreign_key = std::make_shared<ASTForeignKeyDeclaration>();
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

    ASTPtr columns = std::make_shared<ASTExpressionList>();
    ASTPtr indices = std::make_shared<ASTExpressionList>();
    ASTPtr constraints = std::make_shared<ASTExpressionList>();
    ASTPtr projections = std::make_shared<ASTExpressionList>();
    ASTPtr primary_key;
    ASTPtr primary_key_from_columns;

    for (const auto & elem : list->children)
    {
        if (auto * cd = elem->as<ASTColumnDeclaration>())
        {
            if (cd->primary_key_specifier)
            {
                if (!primary_key_from_columns)
                    primary_key_from_columns = makeASTFunction("tuple");
                auto column_identifier = std::make_shared<ASTIdentifier>(cd->name);
                primary_key_from_columns->children[0]->as<ASTExpressionList>()->children.push_back(column_identifier);
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

    auto res = std::make_shared<ASTColumns>();

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

    ParserIdentifierWithOptionalParameters ident_with_optional_params_p;
    ParserExpression expression_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ParserTTLExpressionList parser_ttl_list;
    ParserStringLiteral string_literal_parser;

    ASTPtr engine;
    ASTPtr partition_by;
    ASTPtr primary_key;
    ASTPtr order_by;
    ASTPtr sample_by;
    ASTPtr ttl_table;
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
            if (expression_p.parse(pos, order_by, expected))
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

        /// Do not allow SETTINGS clause without ENGINE,
        /// because we cannot distinguish engine settings from query settings in this case.
        /// And because settings for each engine are different.
        if (parsed_engine_keyword && s_settings.ignore(pos, expected))
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
                engine->as<ASTFunction &>().kind = ASTFunction::Kind::TABLE_ENGINE;
                break;

            case EngineKind::DATABASE_ENGINE:
                engine->as<ASTFunction &>().kind = ASTFunction::Kind::DATABASE_ENGINE;
                break;
        }
    }

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
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
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserSelectWithUnionQuery select_p;
    ParserFunction table_function_p;
    ParserNameList names_p;

    ASTPtr table;
    ASTPtr columns_list;
    std::shared_ptr<ASTStorage> storage;
    bool is_time_series_table = false;
    ASTPtr targets;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr as_table_function;
    ASTPtr select;
    ASTPtr from_path;

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


    if (!replace && !or_replace && s_temporary.ignore(pos, expected))
    {
        is_temporary = true;
    }
    if (!s_table.ignore(pos, expected))
        return false;

    if (!replace && !or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (attach && s_from.ignore(pos, expected))
    {
        ParserStringLiteral from_path_p;
        if (!from_path_p.parse(pos, from_path, expected))
            return false;
    }

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto * table_id = table->as<ASTTableIdentifier>();

    /// A shortcut for ATTACH a previously detached table.
    bool short_attach = attach && !from_path;
    if (short_attach && (!pos.isValid() || pos.get().type == TokenType::Semicolon))
    {
        auto query = std::make_shared<ASTCreateQuery>();
        node = query;

        query->attach = attach;
        query->if_not_exists = if_not_exists;
        query->cluster = cluster_str;

        query->database = table_id->getDatabase();
        query->table = table_id->getTable();
        query->uuid = table_id->uuid;
        query->has_uuid = table_id->uuid != UUIDHelpers::Nil;

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

        storage = typeid_cast<std::shared_ptr<ASTStorage>>(ast);

        if (storage && storage->engine && (storage->engine->name == "TimeSeries"))
        {
            is_time_series_table = true;
            ParserViewTargets({ViewTarget::Data, ViewTarget::Tags, ViewTarget::Metrics}).parse(pos, targets, expected);
        }

        return true;
    };

    auto need_parse_as_select = [&is_create_empty, &is_clone_as, &pos, &expected]()
    {
        if (ParserKeyword{Keyword::EMPTY_AS}.ignore(pos, expected))
        {
            is_create_empty = true;
            return true;
        }
        if (ParserKeyword{Keyword::CLONE_AS}.ignore(pos, expected))
        {
            is_clone_as = true;
            return true;
        }

        return ParserKeyword{Keyword::AS}.ignore(pos, expected);
    };

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

        if ((storage_parse_result || is_temporary) && need_parse_as_select())
        {
            if (!select_p.parse(pos, select, expected))
                return false;
        }

        if (!storage_parse_result && !is_temporary)
        {
            if (need_parse_as_select() && !table_function_p.parse(pos, as_table_function, expected))
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

        /// CREATE|ATTACH TABLE ... AS ...
        if (need_parse_as_select())
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

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->replace_table = replace;
    query->create_or_replace = or_replace;
    query->if_not_exists = if_not_exists;
    query->temporary = is_temporary;
    query->is_time_series_table = is_time_series_table;

    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->has_uuid = table_id->uuid != UUIDHelpers::Nil;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->set(query->columns_list, columns_list);
    query->set(query->storage, storage);
    query->set(query->as_table_function, as_table_function);

    if (comment)
        query->set(query->comment, comment);

    if (query->columns_list && query->columns_list->primary_key)
    {
        /// If engine is not set will use default one
        if (!query->storage)
            query->set(query->storage, std::make_shared<ASTStorage>());
        else if (query->storage->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");

        query->storage->primary_key = query->columns_list->primary_key;

    }

    if (query->columns_list && (query->columns_list->primary_key_from_columns))
    {
        /// If engine is not set will use default one
        if (!query->storage)
            query->set(query->storage, std::make_shared<ASTStorage>());
        else if (query->storage->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");

        query->storage->primary_key = query->columns_list->primary_key_from_columns;
    }

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);
    query->set(query->targets, targets);
    query->is_create_empty = is_create_empty;
    query->is_clone_as = is_clone_as;

    if (from_path)
        query->attach_from_path = from_path->as<ASTLiteral &>().value.safeGet<String>();

    return true;
}

bool ParserCreateLiveViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_live(Keyword::LIVE);
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserStorage storage_inner{ParserStorage::TABLE_ENGINE};
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserSelectWithUnionQuery select_p;
    ParserSQLSecurity sql_security_p;

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr columns_list;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;
    ASTPtr sql_security;

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

    sql_security_p.parse(pos, sql_security, expected);

    if (!s_live.ignore(pos, expected))
        return false;

    if (!s_view.ignore(pos, expected))
       return false;

    if (s_if_not_exists.ignore(pos, expected))
       if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (ParserKeyword{Keyword::ON}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    // TO [db.]table
    if (ParserKeyword{Keyword::TO}.ignore(pos, expected))
    {
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
    }

    std::shared_ptr<ASTViewTargets> targets;
    if (to_table)
    {
        targets = std::make_shared<ASTViewTargets>();
        targets->setTableID(ViewTarget::To, to_table->as<ASTTableIdentifier>()->getTableId());
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    if (!sql_security && !sql_security_p.parse(pos, sql_security, expected))
        sql_security = std::make_shared<ASTSQLSecurity>();

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_live_view = true;

    auto * table_id = table->as<ASTTableIdentifier>();
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->set(query->columns_list, columns_list);

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);
    query->set(query->targets, targets);

    if (comment)
        query->set(query->comment, comment);

    if (sql_security)
        query->sql_security = typeid_cast<std::shared_ptr<ASTSQLSecurity>>(sql_security);

    return true;
}

bool ParserCreateWindowViewQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_temporary(Keyword::TEMPORARY);
    ParserKeyword s_attach(Keyword::ATTACH);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserCompoundIdentifier table_name_p(/*table_name_with_optional_uuid*/ true, /*allow_query_parameter*/ true);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_view(Keyword::VIEW);
    ParserKeyword s_window(Keyword::WINDOW);
    ParserKeyword s_populate(Keyword::POPULATE);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_inner(Keyword::INNER);
    ParserKeyword s_watermark(Keyword::WATERMARK);
    ParserKeyword s_allowed_lateness(Keyword::ALLOWED_LATENESS);
    ParserKeyword s_empty(Keyword::EMPTY);
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_eq(TokenType::Equals);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserStorage storage_inner{ParserStorage::TABLE_ENGINE};
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserExpression watermark_p;
    ParserExpression lateness_p;
    ParserSelectWithUnionQuery select_p;

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr columns_list;
    ASTPtr storage;
    ASTPtr inner_storage;
    ASTPtr watermark;
    ASTPtr lateness;
    ASTPtr as_database;
    ASTPtr as_table;
    ASTPtr select;

    String cluster_str;
    bool attach = false;
    bool is_watermark_strictly_ascending = false;
    bool is_watermark_ascending = false;
    bool is_watermark_bounded = false;
    bool allowed_lateness = false;
    bool if_not_exists = false;
    bool is_populate = false;
    bool is_create_empty = false;

    if (!s_create.ignore(pos, expected))
    {
        if (s_attach.ignore(pos, expected))
            attach = true;
        else
            return false;
    }

    if (!s_window.ignore(pos, expected))
        return false;

    if (!s_view.ignore(pos, expected))
       return false;

    if (s_if_not_exists.ignore(pos, expected))
       if_not_exists = true;

    if (!table_name_p.parse(pos, table, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    // TO [db.]table
    if (s_to.ignore(pos, expected))
    {
        if (!table_name_p.parse(pos, to_table, expected))
            return false;
    }

    /// Optional - a list of columns can be specified. It must fully comply with SELECT.
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    if (s_inner.ignore(pos, expected))
    {
        /// Inner table ENGINE for WINDOW VIEW
        storage_inner.parse(pos, inner_storage, expected);
    }

    if (!to_table)
    {
        /// Target table ENGINE for WINDOW VIEW
        storage_p.parse(pos, storage, expected);
    }

    std::shared_ptr<ASTViewTargets> targets;
    if (to_table || storage || inner_storage)
    {
        targets = std::make_shared<ASTViewTargets>();
        if (to_table)
            targets->setTableID(ViewTarget::To, to_table->as<ASTTableIdentifier>()->getTableId());
        if (storage)
            targets->setInnerEngine(ViewTarget::To, storage);
        if (inner_storage)
            targets->setInnerEngine(ViewTarget::Inner, inner_storage);
    }

    // WATERMARK
    if (s_watermark.ignore(pos, expected))
    {
        s_eq.ignore(pos, expected);

        if (ParserKeyword(Keyword::STRICTLY_ASCENDING).ignore(pos,expected))
            is_watermark_strictly_ascending = true;
        else if (ParserKeyword(Keyword::ASCENDING).ignore(pos,expected))
            is_watermark_ascending = true;
        else if (watermark_p.parse(pos, watermark, expected))
            is_watermark_bounded = true;
        else
            return false;
    }

    // ALLOWED LATENESS
    if (s_allowed_lateness.ignore(pos, expected))
    {
        s_eq.ignore(pos, expected);
        allowed_lateness = true;

        if (!lateness_p.parse(pos, lateness, expected))
            return false;
    }

    if (s_populate.ignore(pos, expected))
        is_populate = true;
    else if (s_empty.ignore(pos, expected))
        is_create_empty = true;

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_window_view = true;

    auto * table_id = table->as<ASTTableIdentifier>();
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);
    if (comment)
        query->set(query->comment, comment);

    query->set(query->columns_list, columns_list);

    query->is_watermark_strictly_ascending = is_watermark_strictly_ascending;
    query->is_watermark_ascending = is_watermark_ascending;
    query->is_watermark_bounded = is_watermark_bounded;
    query->watermark_function = watermark;
    query->allowed_lateness = allowed_lateness;
    query->lateness_function = lateness;
    query->is_populate = is_populate;
    query->is_create_empty = is_create_empty;

    tryGetIdentifierNameInto(as_database, query->as_database);
    tryGetIdentifierNameInto(as_table, query->as_table);
    query->set(query->select, select);
    query->set(query->targets, targets);

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

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->partition_by, partition_by);
    storage->set(storage->primary_key, primary_key);
    storage->set(storage->order_by, order_by);
    storage->set(storage->sample_by, sample_by);
    storage->set(storage->ttl_table, ttl_table);

    auto res = std::make_shared<ASTTableOverride>();
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
    auto res = std::make_shared<ASTTableOverrideList>();
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

    if (s_uuid.ignore(pos, expected))
    {
        ParserStringLiteral uuid_p;
        ASTPtr ast_uuid;
        if (!uuid_p.parse(pos, ast_uuid, expected))
            return false;
        uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.safeGet<String>());
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

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;

    query->uuid = uuid;
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
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
    ParserIdentifier name_p;
    ParserTablePropertiesDeclarationList table_properties_p;
    ParserSelectWithUnionQuery select_p;
    ParserNameList names_p;
    ParserSQLSecurity sql_security_p;

    ASTPtr table;
    ASTPtr to_table;
    ASTPtr to_inner_uuid;
    ASTPtr columns_list;
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

    if (!replace_view && s_materialized.ignore(pos, expected))
    {
        is_materialized_view = true;
    }
    else
        is_ordinary_view = true;

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
    if (s_lparen.ignore(pos, expected))
    {
        if (!table_properties_p.parse(pos, columns_list, expected))
            return false;

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
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR, "When creating a materialized view you can't declare both 'TO [db].[table]' and 'POPULATE'");

            if (s_empty.ignore(pos, expected))
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

    /// AS SELECT ...
    if (!s_as.ignore(pos, expected))
        return false;

    if (!select_p.parse(pos, select, expected))
        return false;

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;

    query->attach = attach;
    query->if_not_exists = if_not_exists;
    query->is_ordinary_view = is_ordinary_view;
    query->is_materialized_view = is_materialized_view;
    query->is_populate = is_populate;
    query->is_create_empty = is_create_empty;
    query->replace_view = replace_view;

    auto * table_id = table->as<ASTTableIdentifier>();
    query->database = table_id->getDatabase();
    query->table = table_id->getTable();
    query->uuid = table_id->uuid;
    query->cluster = cluster_str;

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    query->set(query->columns_list, columns_list);

    if (refresh_strategy)
        query->set(query->refresh_strategy, refresh_strategy);
    if (comment)
        query->set(query->comment, comment);
    if (sql_security)
        query->sql_security = typeid_cast<std::shared_ptr<ASTSQLSecurity>>(sql_security);

    if (query->columns_list && query->columns_list->primary_key)
    {
        /// If engine is not set will use default one
        if (!storage)
            storage = std::make_shared<ASTStorage>();
        auto & storage_ref = typeid_cast<ASTStorage &>(*storage);
        if (storage_ref.primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");
        storage_ref.primary_key = query->columns_list->primary_key;
    }

    if (query->columns_list && (query->columns_list->primary_key_from_columns))
    {
        /// If engine is not set will use default one
        if (!storage)
            storage = std::make_shared<ASTStorage>();
        auto & storage_ref = typeid_cast<ASTStorage &>(*storage);
        if (storage_ref.primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple primary keys are not allowed.");
        storage_ref.primary_key = query->columns_list->primary_key_from_columns;
    }

    std::shared_ptr<ASTViewTargets> targets;
    if (to_table || to_inner_uuid || storage)
    {
        targets = std::make_shared<ASTViewTargets>();
        if (to_table)
            targets->setTableID(ViewTarget::To, to_table->as<ASTTableIdentifier>()->getTableId());
        if (to_inner_uuid)
            targets->setInnerUUID(ViewTarget::To, parseFromString<UUID>(to_inner_uuid->as<ASTLiteral>()->value.safeGet<String>()));
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

    auto query = std::make_shared<ASTCreateNamedCollectionQuery>();

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

        if (!s_right_paren.ignore(pos, expected))
            return false;

        if (!dictionary_p.parse(pos, dictionary, expected))
            return false;
    }

    auto comment = parseComment(pos, expected);

    auto query = std::make_shared<ASTCreateQuery>();
    node = query;
    query->is_dictionary = true;
    query->attach = attach;
    query->create_or_replace = or_replace;
    query->replace_table = replace;

    auto * dict_id = name->as<ASTTableIdentifier>();
    query->database = dict_id->getDatabase();
    query->table = dict_id->getTable();
    query->uuid = dict_id->uuid;

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
    ParserCreateLiveViewQuery live_view_p;
    ParserCreateWindowViewQuery window_view_p;

    return table_p.parse(pos, node, expected)
        || database_p.parse(pos, node, expected)
        || view_p.parse(pos, node, expected)
        || dictionary_p.parse(pos, node, expected)
        || live_view_p.parse(pos, node, expected)
        || window_view_p.parse(pos, node, expected);
}

}
