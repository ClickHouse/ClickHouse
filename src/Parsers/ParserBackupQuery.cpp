#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/stripQuerySettings.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    bool parsePartitions(IParser::Pos & pos, Expected & expected, std::optional<ASTs> & partitions)
    {
        if (!ParserKeyword(Keyword::PARTITION).ignore(pos, expected) && !ParserKeyword(Keyword::PARTITIONS).ignore(pos, expected))
            return false;

        ASTs result;
        auto parse_list_element = [&]
        {
            ASTPtr ast;
            if (!ParserPartition{}.parse(pos, ast, expected))
                return false;
            result.push_back(ast);
            return true;
        };
        if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
            return false;

        partitions = std::move(result);
        return true;
    }

    bool parseExceptDatabases(IParser::Pos & pos, Expected & expected, std::set<String> & except_databases)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword(Keyword::EXCEPT_DATABASE).ignore(pos, expected) && !ParserKeyword(Keyword::EXCEPT_DATABASES).ignore(pos, expected))
                return false;

            std::set<String> result;
            auto parse_list_element = [&]
            {
                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                result.insert(getIdentifierName(ast));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_databases = std::move(result);
            return true;
        });
    }

    bool parseExceptTables(IParser::Pos & pos, Expected & expected, const std::optional<String> & database_name, std::set<DatabaseAndTableName> & except_tables)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword(Keyword::EXCEPT_TABLE).ignore(pos, expected) && !ParserKeyword(Keyword::EXCEPT_TABLES).ignore(pos, expected))
                return false;

            std::set<DatabaseAndTableName> result;
            auto parse_list_element = [&]
            {
                DatabaseAndTableName table_name;

                if (!parseDatabaseAndTableName(pos, expected, table_name.first, table_name.second))
                    return false;

                if (database_name && table_name.first.empty())
                    table_name.first = *database_name;

                if (database_name && table_name.first != *database_name)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "Database name in EXCEPT TABLES clause doesn't match the database name in DATABASE clause: {} != {}",
                        table_name.first,
                        *database_name
                    );

                result.emplace(std::move(table_name));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_tables = std::move(result);
            return true;
        });
    }

    /// Whether `pos` is at the start of a BACKUP element (`TABLE t`, `DATABASE db`, `ALL`, ...).
    ///
    /// A comma in `EXCEPT DATA FROM TABLES a, b` separates two table names, while the same comma in
    /// `EXCEPT DATA FROM TABLE a, TABLE b` separates the clause from the next element of the query.
    /// Only what follows the comma tells the two apart, which is what this looks at.
    bool isAtElementStart(IParser::Pos & pos, Expected & expected)
    {
        return ParserKeyword(Keyword::TABLE).checkWithoutMoving(pos, expected)
            || ParserKeyword(Keyword::DICTIONARY).checkWithoutMoving(pos, expected)
            || ParserKeyword(Keyword::VIEW).checkWithoutMoving(pos, expected)
            || ParserKeyword(Keyword::TEMPORARY_TABLE).checkWithoutMoving(pos, expected)
            || ParserKeyword(Keyword::DATABASE).checkWithoutMoving(pos, expected)
            || ParserKeyword(Keyword::ALL).checkWithoutMoving(pos, expected);
    }

    bool parseExceptDataTables(IParser::Pos & pos, Expected & expected, const std::optional<String> & database_name, std::set<DatabaseAndTableName> & except_data_tables)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLE).ignore(pos, expected) && !ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLES).ignore(pos, expected))
                return false;

            std::set<DatabaseAndTableName> result;
            bool at_first_name = true;
            auto parse_list_element = [&]
            {
                /// A BACKUP element keyword after a comma means the comma ends this clause and belongs to
                /// the query's element list. Returning false makes `ParserList::parseUtil` rewind to before
                /// the comma and finish the list, so `parseElements` sees the element.
                ///
                /// The first name is exempt because no comma precedes it, which keeps a table genuinely
                /// called `all` or `view` nameable; after a comma such a name has to be quoted.
                if (!at_first_name && isAtElementStart(pos, expected))
                    return false;
                at_first_name = false;

                DatabaseAndTableName table_name;

                if (!parseDatabaseAndTableName(pos, expected, table_name.first, table_name.second))
                    return false;

                if (database_name && table_name.first.empty())
                    table_name.first = *database_name;

                if (database_name && table_name.first != *database_name)
                    throw Exception(
                        ErrorCodes::SYNTAX_ERROR,
                        "Database name in EXCEPT DATA FROM TABLE/TABLES clause doesn't match the database name of the "
                        "backup element it is written on: {} != {}",
                        table_name.first,
                        *database_name
                    );

                result.emplace(std::move(table_name));
                return true;
            };
            if (!ParserList::parseUtil(pos, expected, parse_list_element, false))
                return false;

            except_data_tables = std::move(result);
            return true;
        });
    }

    /// Parses `EXCEPT DATA FROM {TABLE|TABLES}` written on a single-object element (TABLE, DICTIONARY, VIEW,
    /// TEMPORARY TABLE) and reduces it to the `except_data` flag on that element.
    ///
    /// Such an element selects exactly one object, so its own object is the only one the clause can refer to.
    /// Any other name is out of the element's scope: it cannot be excluded here, because a data exclusion is
    /// element-scoped and must not reach the tables selected by the other elements of the same query. Reject
    /// it instead of accepting a clause which would do nothing (or, worse, something the user didn't write).
    ///
    /// The element's table name is always known here, so a foreign table name is rejected right away. Its
    /// database name may not be: an unqualified element (`BACKUP TABLE t ...`) takes its database from the
    /// current database, which the parser does not know. A database name the clause states explicitly in that
    /// case is kept in `except_data_database_name`, for `Element::setCurrentDatabase` to compare once the
    /// element is resolved - so `BACKUP TABLE t EXCEPT DATA FROM TABLE test.t` is accepted when the current
    /// database is `test` and rejected when it is not, instead of being rejected outright.
    ///
    /// `element_has_database` is false for a TEMPORARY TABLE element, which has no database and never gets
    /// one, so for it a stated database name is already known to be wrong and there is nothing to defer.
    bool parseExceptDataFromThisTable(IParser::Pos & pos, Expected & expected, Element & element, bool element_has_database)
    {
        /// Parse with no expected database name: the comparison against the element's own database is done
        /// below, because it may have to be deferred - which `parseExceptDataTables` (shared with the DATABASE
        /// and ALL elements, whose database is always known at parse time) cannot do.
        std::set<DatabaseAndTableName> except_data_tables;
        if (!parseExceptDataTables(pos, expected, /*database_name=*/ {}, except_data_tables))
            return false;

        auto reject = [&](const DatabaseAndTableName & specified)
        {
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "EXCEPT DATA FROM TABLE clause of a single-object BACKUP element can only name that element's own "
                "object {}, but {} was specified. To exclude the data of another table, back it up as its own element "
                "with its own clause (BACKUP TABLE t1, TABLE t2 EXCEPT DATA FROM TABLE t2), or exclude it at the "
                "database level (BACKUP DATABASE db EXCEPT DATA FROM TABLE db.t2)",
                element.database_name.empty()
                    ? backQuoteIfNeed(element.table_name)
                    : backQuoteIfNeed(element.database_name) + "." + backQuoteIfNeed(element.table_name),
                specified.first.empty()
                    ? backQuoteIfNeed(specified.second)
                    : backQuoteIfNeed(specified.first) + "." + backQuoteIfNeed(specified.second));
        };

        for (const auto & except_data_table : except_data_tables)
        {
            const String & clause_database_name = except_data_table.first;
            const String & clause_table_name = except_data_table.second;

            if (clause_table_name != element.table_name)
                reject(except_data_table);

            /// An omitted database name means "this element's database", whatever it resolves to.
            if (clause_database_name.empty())
                continue;

            /// A temporary table has no database, so no database name can be the right one.
            if (!element_has_database)
                reject(except_data_table);

            if (!element.database_name.empty())
            {
                if (clause_database_name != element.database_name)
                    reject(except_data_table);
                continue;
            }

            /// The element is unqualified, so defer the comparison to `Element::setCurrentDatabase`. Two
            /// different database names in one clause cannot both be the element's own one, whatever it
            /// resolves to, so that much is still decided here.
            if (!element.except_data_database_name.empty() && (element.except_data_database_name != clause_database_name))
                reject(except_data_table);
            element.except_data_database_name = clause_database_name;
        }

        element.except_data = !except_data_tables.empty();
        return true;
    }

    bool parseElement(IParser::Pos & pos, Expected & expected, Element & element, Kind kind)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (ParserKeyword(Keyword::TABLE).ignore(pos, expected) || ParserKeyword(Keyword::DICTIONARY).ignore(pos, expected) ||
                ParserKeyword(Keyword::VIEW).ignore(pos, expected))
            {
                element.type = ElementType::TABLE;
                if (!parseDatabaseAndTableName(pos, expected, element.database_name, element.table_name))
                    return false;

                element.new_database_name = element.database_name;
                element.new_table_name = element.table_name;
                if (ParserKeyword(Keyword::AS).ignore(pos, expected))
                {
                    if (!parseDatabaseAndTableName(pos, expected, element.new_database_name, element.new_table_name))
                        return false;
                }

                parsePartitions(pos, expected, element.partitions);

                if (kind == Kind::BACKUP)
                {
                    parseExceptDataFromThisTable(pos, expected, element, /*element_has_database=*/ true);
                }
                else if (ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLE).checkWithoutMoving(pos, expected) ||
                         ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLES).checkWithoutMoving(pos, expected))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXCEPT DATA FROM TABLE/TABLES clause is only valid for BACKUP queries, not RESTORE");
                }
                return true;
            }

            if (ParserKeyword(Keyword::TEMPORARY_TABLE).ignore(pos, expected))
            {
                element.type = ElementType::TEMPORARY_TABLE;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                element.table_name = getIdentifierName(ast);
                element.new_table_name = element.table_name;

                if (ParserKeyword(Keyword::AS).ignore(pos, expected))
                {
                    ast = nullptr;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    element.new_table_name = getIdentifierName(ast);
                }

                if (kind == Kind::BACKUP)
                {
                    /// A temporary table has no database, so no database name may be given in the clause either.
                    parseExceptDataFromThisTable(pos, expected, element, /*element_has_database=*/ false);
                }
                else if (ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLE).checkWithoutMoving(pos, expected) ||
                         ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLES).checkWithoutMoving(pos, expected))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXCEPT DATA FROM TABLE/TABLES clause is only valid for BACKUP queries, not RESTORE");
                }
                return true;
            }

            if (ParserKeyword(Keyword::DATABASE).ignore(pos, expected))
            {
                element.type = ElementType::DATABASE;

                ASTPtr ast;
                if (!ParserIdentifier{}.parse(pos, ast, expected))
                    return false;
                element.database_name = getIdentifierName(ast);
                element.new_database_name = element.database_name;

                if (ParserKeyword(Keyword::AS).ignore(pos, expected))
                {
                    ast = nullptr;
                    if (!ParserIdentifier{}.parse(pos, ast, expected))
                        return false;
                    element.new_database_name = getIdentifierName(ast);
                }

                parseExceptTables(pos, expected, element.database_name, element.except_tables);

                if (kind == Kind::BACKUP)
                {
                    parseExceptDataTables(pos, expected, element.database_name, element.except_data_tables);
                }
                else if (ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLE).checkWithoutMoving(pos, expected) ||
                         ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLES).checkWithoutMoving(pos, expected))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXCEPT DATA FROM TABLE/TABLES clause is only valid for BACKUP queries, not RESTORE");
                }
                return true;
            }

            if (ParserKeyword(Keyword::ALL).ignore(pos, expected))
            {
                element.type = ElementType::ALL;
                parseExceptDatabases(pos, expected, element.except_databases);
                parseExceptTables(pos, expected, {}, element.except_tables);

                if (kind == Kind::BACKUP)
                {
                    parseExceptDataTables(pos, expected, {}, element.except_data_tables);
                }
                else if (ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLE).checkWithoutMoving(pos, expected) ||
                         ParserKeyword(Keyword::EXCEPT_DATA_FROM_TABLES).checkWithoutMoving(pos, expected))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "EXCEPT DATA FROM TABLE/TABLES clause is only valid for BACKUP queries, not RESTORE");
                }
                return true;
            }

            return false;
        });
    }

    bool parseElements(IParser::Pos & pos, Expected & expected, std::vector<Element> & elements, Kind kind)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<Element> result;

            auto parse_element = [&]
            {
                Element element;
                if (parseElement(pos, expected, element, kind))
                {
                    result.emplace_back(std::move(element));
                    return true;
                }
                return false;
            };

            if (!ParserList::parseUtil(pos, expected, parse_element, false))
                return false;

            elements = std::move(result);
            return true;
        });
    }

    bool parseBackupName(IParser::Pos & pos, Expected & expected, ASTPtr & backup_name)
    {
        if (!ParserIdentifierWithOptionalParameters{}.parse(pos, backup_name, expected))
            return false;

        backup_name->as<ASTFunction &>().setKind(ASTFunction::Kind::BACKUP_NAME);
        return true;
    }

    bool parseBaseBackupSetting(IParser::Pos & pos, Expected & expected, ASTPtr & base_backup_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::BASE_BACKUP}.ignore(pos, expected)
                && ParserToken(TokenType::Equals).ignore(pos, expected)
                && parseBackupName(pos, expected, base_backup_name);
        });
    }

    /// Whether the next item is `<name> = DEFAULT`, matched as the pair parser matches it. `pos` is taken by
    /// value, so this only looks ahead.
    bool isDefaultedSetting(IParser::Pos pos, Expected & expected)
    {
        ASTPtr name;
        return ParserCompoundIdentifier{}.parse(pos, name, expected) && ParserToken(TokenType::Equals).ignore(pos, expected)
            && ParserKeyword(Keyword::DEFAULT).ignore(pos, expected);
    }

    bool parseClusterHostIDs(IParser::Pos & pos, Expected & expected, ASTPtr & cluster_host_ids)
    {
        /// Accept both [...] and array(...) syntax for formatting roundtrip consistency.
        if (ParserArray{}.parse(pos, cluster_host_ids, expected))
            return true;

        ASTPtr tmp;
        if (!ParserFunction{}.parse(pos, tmp, expected))
            return false;

        auto * func = tmp->as<ASTFunction>();
        if (!func || func->name != "array")
            return false;

        cluster_host_ids = std::move(tmp);
        return true;
    }

    bool parseClusterHostIDsSetting(IParser::Pos & pos, Expected & expected, ASTPtr & cluster_host_ids)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::CLUSTER_HOST_IDS}.ignore(pos, expected)
                && ParserToken(TokenType::Equals).ignore(pos, expected)
                && parseClusterHostIDs(pos, expected, cluster_host_ids);
        });
    }

    /// One parsed SETTINGS clause of a BACKUP/RESTORE query.
    struct ParsedSettings
    {
        SettingsChanges changes;
        std::vector<String> default_settings;
        /// A `param_x = value` item. The clause is never accepted with one (see parseSettings).
        bool has_parameter = false;
        ASTPtr base_backup_name;
        ASTPtr cluster_host_ids;
    };

    /// True if a change has a `{name:Type}` substitution as its value (wrapped into a Field like `disk(...)`).
    bool hasQueryParameterValue(const SettingsChanges & changes)
    {
        for (const auto & change : changes)
        {
            CustomType custom;
            if (!change.value.tryGet<CustomType>(custom) || std::string_view(custom.getTypeName()) != FieldFromASTImpl::name)
                continue;

            if (dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast->as<ASTQueryParameter>())
                return true;
        }
        return false;
    }

    /// A comma here means ParserList stopped mid-list: nothing that may follow a BACKUP/RESTORE SETTINGS
    /// clause starts with one.
    bool isAtListSeparator(IParser::Pos pos)
    {
        return pos->type == TokenType::Comma;
    }

    /// `default_aware` selects the pair parser that also accepts `name = DEFAULT` and `param_x = value`.
    bool parseSettingsList(IParser::Pos & pos, Expected & expected, ParsedSettings & res, bool default_aware)
    {
        auto parse_setting = [&]
        {
            /// A backup name may be the bare identifier `DEFAULT`; here such an item is a reset, which
            /// `resolveDefaultedSubSettings` applies.
            const bool defaulted = default_aware && isDefaultedSetting(pos, expected);

            if (!defaulted && !res.base_backup_name && parseBaseBackupSetting(pos, expected, res.base_backup_name))
                return true;

            if (!defaulted && !res.cluster_host_ids && parseClusterHostIDsSetting(pos, expected, res.cluster_host_ids))
                return true;

            SettingChange setting;

            if (!default_aware)
            {
                if (!ParserSetQuery::parseNameValuePair(setting, pos, expected))
                    return false;

                res.changes.push_back(std::move(setting));
                return true;
            }

            String name_of_default_setting;
            ParserSetQuery::Parameter parameter;
            /// No shorthand (`SETTINGS name` for `name = true`): the BACKUP grammar never accepted it.
            if (!ParserSetQuery::parseNameValuePairWithParameterOrDefault(
                    setting, name_of_default_setting, parameter, pos, expected, /* enable_shorthand_syntax= */ false))
                return false;

            if (!parameter.first.empty())
                res.has_parameter = true;
            else if (!name_of_default_setting.empty())
                res.default_settings.push_back(std::move(name_of_default_setting));
            else
                res.changes.push_back(std::move(setting));

            return true;
        };

        return ParserList::parseUtil(pos, expected, parse_setting, false);
    }

    /// `base_backup` and `cluster_host_ids` are AST fields, not `changes`, so their reset is resolved here:
    /// clear the field, drop the name.
    void resolveDefaultedSubSettings(ParsedSettings & res)
    {
        /// Both fields are set through case-insensitive keywords, so their resets are matched the same way.
        auto is_base_backup = [](const String & name) { return equalsCaseInsensitive(name, "base_backup"); };
        auto is_cluster_host_ids = [](const String & name) { return equalsCaseInsensitive(name, "cluster_host_ids"); };

        for (const auto & name : res.default_settings)
        {
            if (is_base_backup(name))
                res.base_backup_name = nullptr;
            else if (is_cluster_host_ids(name))
                res.cluster_host_ids = nullptr;
        }

        std::erase_if(res.default_settings, [&](const String & name) { return is_base_backup(name) || is_cluster_host_ids(name); });
    }

    void moveParsedSettingsOut(ParsedSettings & res, ASTPtr & settings, ASTPtr & base_backup_name, ASTPtr & cluster_host_ids)
    {
        ASTPtr res_settings;
        if (!res.changes.empty() || !res.default_settings.empty())
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->changes = std::move(res.changes);
            settings_ast->default_settings = std::move(res.default_settings);
            settings_ast->is_standalone = false;
            res_settings = settings_ast;
        }

        settings = std::move(res_settings);
        base_backup_name = std::move(res.base_backup_name);
        cluster_host_ids = std::move(res.cluster_host_ids);
    }

    bool parseSettings(IParser::Pos & pos, Expected & expected, ASTPtr & settings, ASTPtr & base_backup_name, ASTPtr & cluster_host_ids)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword(Keyword::SETTINGS).ignore(pos, expected))
                return false;

            const auto list_begin = pos;

            /// Run both grammars rather than predict which items the plain one rejects; the plain one wins if
            /// it reads the whole clause.
            ParsedSettings plain;
            const bool plain_ok = parseSettingsList(pos, expected, plain, /* default_aware= */ false);
            const auto plain_end = pos;

            if (!plain_ok || isAtListSeparator(plain_end))
            {
                pos = list_begin;
                ParsedSettings with_default;
                const bool with_default_ok = parseSettingsList(pos, expected, with_default, /* default_aware= */ true);

                /// Take the DEFAULT-aware reading only if it reads the whole clause and reinterprets no
                /// `param_x` or substitution value.
                if (with_default_ok && !isAtListSeparator(pos) && !with_default.has_parameter
                    && !hasQueryParameterValue(with_default.changes))
                {
                    resolveDefaultedSubSettings(with_default);
                    moveParsedSettingsOut(with_default, settings, base_backup_name, cluster_host_ids);
                    return true;
                }

                pos = plain_end;
            }

            if (!plain_ok)
                return false;

            moveParsedSettingsOut(plain, settings, base_backup_name, cluster_host_ids);
            return true;
        });
    }

    bool parseSyncOrAsync(IParser::Pos & pos, Expected & expected, ASTPtr & settings)
    {
        bool async = false;
        if (ParserKeyword(Keyword::ASYNC).ignore(pos, expected))
            async = true;
        else if (ParserKeyword(Keyword::SYNC).ignore(pos, expected))
            async = false;
        else
            return false;

        auto new_settings = make_intrusive<ASTSetQuery>();
        if (settings)
        {
            const auto & parsed = *assert_cast<ASTSetQuery *>(settings.get());
            new_settings->changes = parsed.changes;
            new_settings->default_settings = parsed.default_settings;
        }

        /// The explicit ASYNC/SYNC keyword wins over an `async` item in the clause, in either carrier.
        static constexpr std::string_view names_to_strip[] = {"async"};
        stripNamesFromSetQuery(*new_settings, names_to_strip);
        new_settings->changes.emplace_back("async", async);

        new_settings->is_standalone = false;
        settings = new_settings;
        return true;
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword(Keyword::ON).ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserBackupQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Kind kind = {};
    if (ParserKeyword(Keyword::BACKUP).ignore(pos, expected))
        kind = Kind::BACKUP;
    else if (ParserKeyword(Keyword::RESTORE).ignore(pos, expected))
        kind = Kind::RESTORE;
    else
        return false;

    ASTPtr base_snapshot_name = nullptr;
    std::vector<Element> elements;
    if (kind == Kind::BACKUP && ParserKeyword(Keyword::FROM_SNAPSHOT).ignore(pos, expected))
    {
        if (!parseBackupName(pos, expected, base_snapshot_name))
            return false;
    }
    else if (!parseElements(pos, expected, elements, kind))
        return false;

    String cluster;
    parseOnCluster(pos, expected, cluster);

    if (!ParserKeyword((kind == Kind::BACKUP) ? Keyword::TO : Keyword::FROM).ignore(pos, expected))
        return false;

    ASTPtr backup_name;
    if (!parseBackupName(pos, expected, backup_name))
        return false;

    ASTPtr settings;
    ASTPtr base_backup_name;
    ASTPtr cluster_host_ids;
    parseSettings(pos, expected, settings, base_backup_name, cluster_host_ids);
    parseSyncOrAsync(pos, expected, settings);

    auto query = make_intrusive<ASTBackupQuery>();
    node = query;

    query->kind = kind;
    query->elements = std::move(elements);
    query->cluster = std::move(cluster);

    if (backup_name)
        query->set(query->backup_name, backup_name);

    query->settings = std::move(settings);
    query->cluster_host_ids = std::move(cluster_host_ids);

    if (base_backup_name)
        query->set(query->base_backup_name, base_backup_name);

    if (base_snapshot_name)
        query->set(query->base_snapshot_name, base_snapshot_name);

    return true;
}

std::map<String, Documentation> ParserBackupQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["BACKUP"] =
    {
        .description = R"DOCS_MD(
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import { CloudOnlyBadge } from "/snippets/components/CloudOnlyBadge/CloudOnlyBadge.jsx";

Introduced in: v21.10.0

Creates a backup of tables, views, dictionaries, databases or all databases, and writes it to a destination. [`RESTORE`](/reference/statements/restore) restores objects from such a backup. For backup strategies and walkthroughs per destination, see [Backup and restore](/concepts/features/backup-restore/overview).

## Syntax {#syntax}

```sql
BACKUP element [, ...]
    [ON CLUSTER cluster]
    TO destination
    [SETTINGS setting = value [, ...]]
    [ASYNC | SYNC]
```

## Elements {#elements}

| Element | Backs up |
|---|---|
| `TABLE [db.]name [AS [db.]name_in_backup] [PARTITION[S] partition_expr [, ...]]` | One table, view or dictionary. With `PARTITIONS`, only the listed partitions. `AS` stores it in the backup under another name. |
| `VIEW ...`, `DICTIONARY ...` | Same as `TABLE`: the keyword does not restrict the kind of object. |
| `TEMPORARY TABLE name [AS name_in_backup]` | A temporary table of the current session. |
| `DATABASE name [AS name_in_backup] [EXCEPT TABLE[S] table [, ...]]` | A database and all its tables, views and dictionaries, except the listed ones, which must belong to it. |
| `ALL [EXCEPT DATABASE[S] db [, ...]] [EXCEPT TABLE[S] db.table [, ...]]` | Every database, including `system`, and the temporary tables of the current session, except the listed ones. Introduced in v23.4.0. |

Each element can end with `EXCEPT DATA FROM TABLE[S] [db.]table [, ...]` (introduced in v26.10.0): the listed tables are stored without their data and restore as empty tables. On a `TABLE`, `VIEW`, `DICTIONARY` or `TEMPORARY TABLE` element the clause can only name the element's own object. See [Backing up a table without its data](/concepts/features/backup-restore/overview#backup-without-data).

## Destinations {#destinations}

| Destination | Stores the backup | Introduced in |
|---|---|---|
| `File('path')` | In a local directory below the server setting [`backups.allowed_path`](/reference/settings/server-settings/settings/backups#backups). A relative path is relative to it. | v21.12.0 |
| `Disk('disk', 'path')` | On a disk listed in the server setting [`backups.allowed_disk`](/reference/settings/server-settings/settings/backups#backups). | v21.12.0 |
| `S3('url' [, 'access_key_id', 'secret_access_key'])`, `S3(named_collection [, 'path'])` | In S3 or S3-compatible storage. See [S3 endpoint](/concepts/features/backup-restore/s3-endpoint). | v22.10.0 |
| `AzureBlobStorage('connection_string_or_url', 'container', 'path' [, 'account_name', 'account_key'])`, `AzureBlobStorage(named_collection [, 'path'])` | In Azure Blob Storage. See [AzureBlobStorage](/concepts/features/backup-restore/azure-blob-storage). | v24.2.0 |
| `Memory('name')` | In the memory of the current session, until the session ends. | v25.1.0 |
| `Null` | Nowhere: the backup is written and discarded, and cannot be restored. For testing. | v25.1.0 |

A path ending with `.zip`, `.zipx`, `.tar`, `.tar.gz`, `.tgz`, `.tar.zst`, `.tzst`, `.tar.xz`, `.tar.bz2` or `.tar.lzma` stores the backup as one archive of that format instead of a directory. `S3` and `AzureBlobStorage` do not support ZIP archives.

## ON CLUSTER {#on-cluster}

Introduced in: v22.5.0

Runs the backup on every host of `cluster`, coordinated through ClickHouse Keeper. See [Distributed DDL](/reference/statements/distributed-ddl). The settings `shard_num` and `replica_num` restrict it to one shard or one replica.

## ASYNC and SYNC {#async}

Introduced in: v22.5.0

By default (`SYNC`) the statement returns when the backup is complete. With `ASYNC`, or `SETTINGS async = 1`, it returns at once and the backup continues in the background: follow it by its `id` in [`system.backups`](/reference/system-tables/backups) and [`system.backup_log`](/reference/system-tables/backup_log).

## Result {#result}

One row with the columns `id` (`String`: the `id` setting, or a generated UUID) and `status` (`Enum8`): `BACKUP_CREATED` when the backup is complete, `CREATING_BACKUP` with `ASYNC`. A synchronous backup that fails throws an exception, for example `BACKUP_ALREADY_EXISTS` when the destination exists.

## Settings {#settings}

| Setting | Default | Description |
|---|---|---|
| `id` | a generated UUID | ID of the operation in the result and in `system.backups`. It must differ from the ID of every operation `system.backups` lists. |
| `base_backup` | unset | Destination of a base backup. The new backup is incremental: it stores only what the base backup does not contain. |
| `compression_method` | `''` | Compression method of a ZIP archive, for example `'lzma'`; `''` is the default method. Not supported for TAR archives. |
| `compression_level` | `-1` | Compression level of a ZIP archive; `-1` is the default level. |
| `password` | `''` | Encrypts a ZIP archive with this password; a directory or TAR backup rejects it. |
| `s3_storage_class` | `''` | Storage class of the files written to S3, for example `'STANDARD'`. Alias: `s3_storage_class_name`. |
| `structure_only` | `0` | Stores only the definitions of the objects, without the data of tables. |
| `async` | `0` | Same as `ASYNC`. |
| `decrypt_files_from_encrypted_disks` | `0` | Decrypts the files of tables stored on encrypted disks. |
| `deduplicate_files` | `1` | Stores identical files once within the backup. |
| `fsync_backup_files` | `1` | Syncs the files of `File` and local `Disk` backups to storage before the backup is reported as created. |
| `allow_s3_native_copy` | `1` | Allows server-side copies in S3. |
| `allow_azure_native_copy` | `1` | Allows server-side copies in Azure Blob Storage. |
| `use_same_s3_credentials_for_base_backup` | `0` | Opens an S3 base backup with the credentials of this backup's destination. |
| `use_same_password_for_base_backup` | `0` | Opens the base backup archive with the `password` of this backup. |
| `azure_attempt_to_create_container` | `1` | Creates the Azure container if it does not exist, which requires permissions at the storage account level. |
| `read_from_filesystem_cache` | `1` | Uses the existing entries of the filesystem cache without adding new ones. |
| `shard_num` | `0` | With `ON CLUSTER`, the 1-based index of the only shard to back up; `0` means all shards. |
| `replica_num` | `0` | With `ON CLUSTER`, the 1-based index of the only replica to back up; `0` means all replicas. |
| `check_parts` | `1` | Checks the checksums of data parts before writing them to the backup. |
| `check_projection_parts` | `1` | Checks the checksums of projection parts before writing them to the backup. |
| `allow_backup_broken_projections` | `0` | Allows backing up a table with broken projections. |
| `write_access_entities_dependents` | `1` | Stores, with the access entities, what depends on them, for example the grant of a backed up role to a user that is not backed up. |
| `allow_checksums_from_remote_paths` | `1` | Allows computing the checksums of backup entries from the paths of their blobs in object storage. |
| `data_file_name_generator` | `''` | `''` keeps the original names of the data files, `'checksum'` names them by checksum. The server setting `backups.data_file_name_generator` changes the default. |
| `data_file_name_prefix_length` | `3` | With `data_file_name_generator = 'checksum'`, the length of the checksum prefix used as a directory name. |
| `backup_data_from_refreshable_materialized_view_targets` | `0` | Also stores the data of the target tables of refreshable materialized views without `APPEND`; the data of other targets is always stored. |
| `backup_named_collections` | unset | Whether to store the named collections created with SQL. See [Named collections](/concepts/features/backup-restore/named-collections). |
| `experimental_lightweight_snapshot` | `0` | Experimental, ClickHouse Cloud only: creates a snapshot backup. See [Snapshot backup](/concepts/features/backup-restore/snapshot). |

Any other setting in the clause applies to the statement as a query-level setting, for example the [`backup_restore_*` settings](/reference/settings/session-settings/backup-restore); an unknown setting is an error. `setting = DEFAULT` resets a setting to its default value.

## Privileges {#privileges}

`BACKUP` requires the `BACKUP` privilege on every object it backs up (on `*.*` for `ALL`) and the `WRITE` [source privilege](/reference/statements/grant#sources) of the destination: `FILE`, `DISK`, `S3` or `AZURE`. `Memory` and `Null` require no source privilege.

## BACKUP FROM SNAPSHOT {#backup-from-snapshot}

Introduced in: v26.3.0

<ExperimentalBadge/>
<CloudOnlyBadge/>

```sql
BACKUP FROM SNAPSHOT snapshot_destination [ON CLUSTER cluster] TO destination [SETTINGS ...] [ASYNC | SYNC]
```

Creates a backup at `destination` from the snapshot backup at `snapshot_destination`, an `S3` or `AzureBlobStorage` backup created with the setting `experimental_lightweight_snapshot`. The experimental setting [`resumable_backup_from_snapshot`](/reference/settings/session-settings/other#resumable_backup_from_snapshot) makes a failed attempt resumable.

## Examples {#examples}

A full backup of a table to a ZIP archive:

```sql title="Query"
CREATE TABLE test.events (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test.events SELECT number, toString(number) FROM numbers(1000);
BACKUP TABLE test.events TO Disk('backups', 'events_full.zip') SETTINGS id = 'events_full';
```

```text title="Response"
┌─id──────────┬─status─────────┐
│ events_full │ BACKUP_CREATED │
└─────────────┴────────────────┘
```

An incremental backup on top of it, which stores fewer entries:

```sql title="Query"
INSERT INTO test.events SELECT number, toString(number) FROM numbers(1000, 10);
BACKUP TABLE test.events TO Disk('backups', 'events_incremental.zip')
SETTINGS id = 'events_incremental', base_backup = Disk('backups', 'events_full.zip');
SELECT id, base_backup_name, num_entries FROM system.backups WHERE id LIKE 'events_%' ORDER BY start_time;
```

```text title="Response"
┌─id─────────────────┬─status─────────┐
│ events_incremental │ BACKUP_CREATED │
└────────────────────┴────────────────┘
┌─id─────────────────┬─base_backup_name───────────────────┬─num_entries─┐
│ events_full        │                                    │          12 │
│ events_incremental │ Disk('backups', 'events_full.zip') │           7 │
└────────────────────┴────────────────────────────────────┴─────────────┘
```

An asynchronous backup of a database, followed in `system.backups`:

```sql title="Query"
BACKUP DATABASE test TO File('test_db') SETTINGS id = 'test_db' ASYNC;
```

```text title="Response"
┌─id──────┬─status──────────┐
│ test_db │ CREATING_BACKUP │
└─────────┴─────────────────┘
```

```sql title="Query"
SELECT id, name, status, num_files FROM system.backups WHERE id = 'test_db';
```

```text title="Response"
┌─id──────┬─name────────────┬─status─────────┬─num_files─┐
│ test_db │ File('test_db') │ BACKUP_CREATED │        24 │
└─────────┴─────────────────┴────────────────┴───────────┘
```

## Related content {#related-content}

- [Backup and restore](/concepts/features/backup-restore/overview)
- [`RESTORE`](/reference/statements/restore)
- [`system.backups`](/reference/system-tables/backups)
)DOCS_MD",
        .syntax = R"(
BACKUP element [, ...] [ON CLUSTER cluster] TO destination [SETTINGS setting = value [, ...]] [ASYNC | SYNC]
BACKUP FROM SNAPSHOT snapshot_destination [ON CLUSTER cluster] TO destination [SETTINGS setting = value [, ...]] [ASYNC | SYNC]
)",
        .related = {"RESTORE", "ALTER TABLE ... UNLOCK SNAPSHOT", "SYSTEM", "GRANT"},
    };

    documentation["RESTORE"] =
    {
        .description = R"DOCS_MD(
Introduced in: v21.10.0

Restores tables, views, dictionaries, databases or all databases from a backup created by [`BACKUP`](/reference/statements/backup).

## Syntax {#syntax}

```sql
RESTORE element [, ...]
    [ON CLUSTER cluster]
    FROM destination
    [SETTINGS setting = value [, ...]]
    [ASYNC | SYNC]
```

The elements are those of [`BACKUP`](/reference/statements/backup#elements). They name the objects as they are stored in the backup, and `AS` restores an object under another name. `PARTITIONS` restores only the listed partitions. `EXCEPT DATA FROM TABLE[S]` is rejected. The destinations are those of [`BACKUP`](/reference/statements/backup#destinations); a `Null` backup cannot be restored. An incremental backup reads its base backup by itself.

`ON CLUSTER`, `ASYNC` and `SYNC` work as for [`BACKUP`](/reference/statements/backup#on-cluster). The result has the same columns, with the `status` `RESTORED`, or `RESTORING` with `ASYNC`.

## Existing objects {#existing-objects}

By default a missing table is created, and an existing table is restored into if it is empty and its definition matches the one in the backup. A different definition fails unless `allow_different_table_def = 1`, and a non-empty table fails unless `allow_non_empty_tables = 1`. Databases are handled the same way, with `create_database` and `allow_different_database_def`.

`RESTORE` does not roll back on failure: see [Restore atomicity](/reference/system-tables/backups#restore-atomicity).

## Settings {#settings}

| Setting | Default | Description |
|---|---|---|
| `id` | a generated UUID | ID of the operation in the result and in `system.backups`. It must differ from the ID of every operation `system.backups` lists. |
| `base_backup` | unset | Location of the base backup of an incremental backup, overriding the one the backup records. |
| `password` | `''` | Password of a ZIP archive created with `password`. |
| `structure_only` | `0` | Restores only the definitions of the objects, without data. It changes the default of the next three settings. |
| `restore_table_data` | `NOT structure_only` | Restores the data of tables. |
| `restore_access_entities` | `NOT structure_only` | Restores users, roles, settings profiles, row policies and quotas. |
| `restore_functions` | `NOT structure_only` | Restores user-defined functions. |
| `restore_named_collections` | `1` | Restores the named collections created with SQL, also with `structure_only`. See [Named collections](/concepts/features/backup-restore/named-collections). |
| `create_table` | `'if-not-exists'` | `'create'` or `1`: fails if the table exists. `'must-exist'` or `0`: fails if the table does not exist. `'if-not-exists'`: creates the table only if it does not exist. |
| `create_database` | `'if-not-exists'` | The same for databases. |
| `allow_different_table_def` | `0` | Allows restoring into an existing table whose definition differs from the one in the backup. |
| `allow_different_database_def` | `0` | Allows restoring into an existing database whose definition differs from the one in the backup. |
| `allow_non_empty_tables` | `0` | Allows restoring into a non-empty table. This can duplicate data. |
| `async` | `0` | Same as `ASYNC`. |
| `shard_num` | `0` | With `ON CLUSTER`, the 1-based index of the only shard to restore to; `0` means all shards. |
| `replica_num` | `0` | With `ON CLUSTER`, the 1-based index of the only replica to restore to; `0` means all replicas. |
| `shard_num_in_backup` | `0` | 1-based index of the shard in the backup to read the data from. `0`: the only shard of the backup, otherwise the same shard as the restoring host. |
| `replica_num_in_backup` | `0` | 1-based index of the replica in the backup to read the data from. `0`: the only replica of the shard in the backup, otherwise the same replica as the restoring host. |
| `create_access` | `'if-not-exists'` | When a restored user, role, settings profile, row policy or quota exists: `'create'` or `1` fails, `'if-not-exists'` keeps the existing one, `'replace'` replaces it. |
| `create_function` | `'if-not-exists'` | The same for user-defined functions. |
| `create_named_collection` | `'if-not-exists'` | The same for named collections. |
| `create_workloads_and_resources` | `'if-not-exists'` | The same for workloads and resources, which are restored together. |
| `skip_unresolved_access_dependencies` | `0` | Restores an access entity without a dependency that cannot be resolved, for example a settings profile of a user that is neither restored nor exists, instead of failing. |
| `restore_access_entities_with_current_grants` | `0` | Limits the grants of restored access entities to what the current user can grant, as `GRANT CURRENT GRANTS` does, instead of failing. |
| `update_access_entities_dependents` | `1` | Updates existing access entities that depend on a restored one, for example grants a restored role to an existing user again. |
| `allow_s3_native_copy` | `1` | Allows server-side copies in S3. |
| `allow_azure_native_copy` | `1` | Allows server-side copies in Azure Blob Storage. |
| `use_same_s3_credentials_for_base_backup` | `0` | Opens an S3 base backup with the credentials of this backup's destination. |
| `use_same_password_for_base_backup` | `0` | Opens the base backup archive with the `password` of this backup. |
| `restore_broken_parts_as_detached` | `0` | Restores broken parts as detached parts with names starting with `broken-from-backup` instead of failing. |
| `storage_policy` | unset | Storage policy of the restored tables. |

Any other setting in the clause applies to the statement as a query-level setting, as for [`BACKUP`](/reference/statements/backup#settings).

## Privileges {#privileges}

`RESTORE` requires the `READ` [source privilege](/reference/statements/grant#sources) of the destination and the privileges to create and fill what it restores, for example `CREATE TABLE` and `INSERT` on a restored table.

## Examples {#examples}

Restore a table under another name from the incremental backup of the [`BACKUP` examples](/reference/statements/backup#examples):

```sql title="Query"
RESTORE TABLE test.events AS test.events_restored FROM Disk('backups', 'events_incremental.zip') SETTINGS id = 'events_restored';
SELECT count() FROM test.events_restored;
```

```text title="Response"
┌─id──────────────┬─status───┐
│ events_restored │ RESTORED │
└─────────────────┴──────────┘
┌─count()─┐
│    1010 │
└─────────┘
```

Restoring into the existing, non-empty table fails:

```sql title="Query"
RESTORE TABLE test.events FROM Disk('backups', 'events_full.zip');
```

```text title="Response"
Code: 608. DB::Exception: Cannot restore the table test.events because it already contains some data. You can set structure_only=true, restore_table_data=false or allow_non_empty_tables=true to overcome that in the way you want: While restoring data of table test.events. (CANNOT_RESTORE_TABLE)
```

## Related content {#related-content}

- [Backup and restore](/concepts/features/backup-restore/overview)
- [Restore settings](/concepts/features/backup-restore/restore-settings)
- [`BACKUP`](/reference/statements/backup)
)DOCS_MD",
        .syntax = R"(
RESTORE element [, ...] [ON CLUSTER cluster] FROM destination [SETTINGS setting = value [, ...]] [ASYNC | SYNC]
)",
        .related = {"BACKUP", "SYSTEM", "GRANT"},
    };

    return documentation;
}

}
