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

    /// True if a change carries a `{name:Type}` substitution as its value, which ParserSubstitution wraps
    /// into a Field the same way `disk(...)` is wrapped.
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

    /// A comma at `pos` means ParserList stopped in the middle of the list (it rewinds to before a
    /// separator that is not followed by a parsable item). Nothing that may follow a BACKUP/RESTORE
    /// SETTINGS clause starts with a comma, so this distinguishes "read the whole clause" from
    /// "gave up part way through it".
    bool isAtListSeparator(IParser::Pos pos)
    {
        return pos->type == TokenType::Comma;
    }

    /// `default_aware` selects the pair parser: the plain one, which rejects `name = DEFAULT`, or the one
    /// that also understands `name = DEFAULT` and `param_x = value`.
    bool parseSettingsList(IParser::Pos & pos, Expected & expected, ParsedSettings & res, bool default_aware)
    {
        auto parse_setting = [&]
        {
            /// A backup name may be a bare identifier, `DEFAULT` included. In this grammar such an item is a
            /// reset, so the sub-setting parsers stand aside and `resolveDefaultedSubSettings` clears the field.
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
            /// Shorthand (`SETTINGS name` standing for `name = true`) stays with the plain grammar: the
            /// BACKUP grammar never accepted it, and today's fallback handles it.
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

    /// `base_backup` and `cluster_host_ids` are not stored in `changes` but in their own AST fields, so
    /// their `= DEFAULT` form is resolved here rather than by the Backups layer: the default of both is
    /// "absent", so clear the field and drop the name.
    void resolveDefaultedSubSettings(ParsedSettings & res)
    {
        /// Both names reach the field through a keyword, which is case-insensitive, so the reset that clears
        /// it has to be matched the same way or a locator survives the reset naming it.
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

            /// Which grammar reads the clause is decided by running both, never by predicting which items
            /// the plain one rejects: `param_x = 1` is an ordinary change to it, a `{name:Type}` value is
            /// not, and either can appear in any position. The plain grammar wins whenever it reads the
            /// clause to the end, so every form accepted today keeps its exact current meaning.
            ParsedSettings plain;
            const bool plain_ok = parseSettingsList(pos, expected, plain, /* default_aware= */ false);
            const auto plain_end = pos;

            if (!plain_ok || isAtListSeparator(plain_end))
            {
                pos = list_begin;
                ParsedSettings with_default;
                const bool with_default_ok = parseSettingsList(pos, expected, with_default, /* default_aware= */ true);

                /// Accept the DEFAULT-aware reading only if it is strictly better: it must read the clause
                /// to the end, and it must not have reinterpreted an item the plain grammar reads
                /// differently (a `param_x` query parameter, or a substitution as a value). Otherwise leave
                /// the clause to the caller exactly as before, so a later unparsable item is still a syntax
                /// error and a first unparsable item still falls through to ParserQueryWithOutput.
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

}
