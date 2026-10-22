#include <Poco/Util/AbstractConfiguration.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Interpreters/misc.h>
#include <Common/FieldVisitorToString.h>
#include <Common/isLocalAddress.h>
#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Net/NetException.h>
#include <base/find_symbols.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <set>


namespace DB
{
namespace ErrorCodes
{
    extern const int AMBIGUOUS_IDENTIFIER;
    extern const int NOT_IMPLEMENTED;
    extern const int WRONG_DDL_RENAMING_SETTINGS;
}

namespace
{
    /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW or CREATE TEMPORARY TABLE or CREATE DATABASE query.
    void visitCreateQuery(ASTCreateQuery & create, const DDLRenamingVisitor::Data & data)
    {
        if (create.isTemporary())
        {
            /// CREATE TEMPORARY TABLE
            String table_name = create.getTable();
            QualifiedTableName full_table_name{DatabaseCatalog::TEMPORARY_DATABASE, table_name};
            const auto & new_table_name = data.renaming_map.getNewTableName(full_table_name);
            if (new_table_name != full_table_name)
            {
                create.setTable(new_table_name.table);
                if (new_table_name.database != DatabaseCatalog::TEMPORARY_DATABASE)
                {
                    create.setIsTemporary(false);
                    create.setDatabase(new_table_name.database);
                }
            }
        }
        else if (create.table)
        {
            /// CREATE TABLE or CREATE DICTIONARY or CREATE VIEW
            QualifiedTableName full_name;
            full_name.table = create.getTable();
            full_name.database = create.getDatabase();

            if (!full_name.database.empty() && !full_name.table.empty())
            {
                auto new_table_name = data.renaming_map.getNewTableName(full_name);
                if (new_table_name != full_name)
                {
                    create.setTable(new_table_name.table);
                    if (new_table_name.database == DatabaseCatalog::TEMPORARY_DATABASE)
                    {
                        create.setIsTemporary(true);
                        create.setDatabase("");
                    }
                    else
                    {
                        create.setDatabase(new_table_name.database);
                    }
                }
            }
        }
        else if (create.database)
        {
            /// CREATE DATABASE
            String database_name = create.getDatabase();
            if (!database_name.empty())
            {
                String new_database_name = data.renaming_map.getNewDatabaseName(database_name);
                create.setDatabase(new_database_name);
            }
        }

        QualifiedTableName as_table{create.as_database, create.as_table};
        if (!as_table.table.empty() && !as_table.database.empty())
        {
            auto as_table_new = data.renaming_map.getNewTableName(as_table);
            create.as_database = as_table_new.database;
            create.as_table = as_table_new.table;
        }

        if (create.targets)
        {
            for (auto & target : create.targets->targets)
            {
                auto & table_id = target.table_id;
                if (!table_id.database_name.empty() && !table_id.table_name.empty())
                {
                    QualifiedTableName target_name{table_id.database_name, table_id.table_name};
                    auto new_target_name = data.renaming_map.getNewTableName(target_name);
                    if (new_target_name != target_name)
                        table_id = StorageID{new_target_name.database, new_target_name.table};
                }
            }
        }
    }

    /// The arguments of these table functions name objects on other servers.
    bool isRemoteTableFunction(const IAST & ast)
    {
        const auto * function = ast.as<ASTFunction>();
        return function
            && (function->name == "remote" || function->name == "remoteSecure" || function->name == "cluster"
                || function->name == "clusterAllReplicas");
    }

    /// A parameterized view is called like a table function named `db.view`.
    void visitParameterizedViewCall(ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        Strings parts;
        splitInto<'.'>(parts, function.name);
        if (parts.size() != 2 || parts[0].empty() || parts[1].empty())
            return;

        auto new_name = data.renaming_map.getNewTableName({parts[0], parts[1]});
        /// The call splits the name on dots.
        if (new_name.database.contains('.') || new_name.table.contains('.'))
            return;
        function.name = new_name.getFullName();
    }

    /// ASTTableExpression represents a reference to a table in SELECT query.
    /// DDLRenamingVisitor should handle ASTTableExpression because some CREATE queries can contain SELECT queries after AS (e.g. CREATE VIEW).
    /// We'll try to replace database and table name.
    void visitTableExpression(ASTTableExpression & expr, const DDLRenamingVisitor::Data & data)
    {
        if (auto * function = expr.table_function ? expr.table_function->as<ASTFunction>() : nullptr)
            visitParameterizedViewCall(*function, data);

        if (!expr.database_and_table_name)
            return;

        ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(expr.database_and_table_name.get());
        if (!identifier)
            return;

        auto table_identifier = identifier->createTable();
        if (!table_identifier)
            return;

        QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
        if (qualified_name.table.empty() || qualified_name.database.empty())
            return;

        auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
        if (new_qualified_name == qualified_name)
            return;

        /// A table reference carries state besides its name, notably the alias the surrounding query
        /// refers to, so rename it in place instead of substituting a newly built node.
        if (auto * table_reference = expr.database_and_table_name->as<ASTTableIdentifier>())
        {
            table_reference->resetTable(new_qualified_name.database, new_qualified_name.table);
            return;
        }

        /// `database_and_table_name` is registered as a child, so appending the renamed identifier
        /// would leave the pre-rename one behind next to it. `replace` swaps both slots at once.
        expr.replace(
            expr.database_and_table_name,
            make_intrusive<ASTTableIdentifier>(new_qualified_name.database, new_qualified_name.table));
    }

    /// `REFRESH ... DEPENDS ON db.tbl` names other tables, so the dependency list is a table reference
    /// like any other. The refresh scheduler keys its dependency graph on these full names, so a
    /// dependency naming the pre-rename database occupies a different graph slot than the renamed table.
    void visitRefreshStrategy(ASTRefreshStrategy & refresh, const DDLRenamingVisitor::Data & data)
    {
        if (!refresh.dependencies)
            return;

        for (auto & child : refresh.dependencies->children)
        {
            const auto * identifier = child ? child->as<ASTTableIdentifier>() : nullptr;
            /// A parameterized name is only known when the view is called, so it has no name to rename.
            if (!identifier || identifier->isParam())
                continue;

            QualifiedTableName qualified_name{identifier->getDatabaseName(), identifier->shortName()};
            if (qualified_name.database.empty() || qualified_name.table.empty())
                continue;

            auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
            if (new_qualified_name == qualified_name)
                continue;

            child = make_intrusive<ASTTableIdentifier>(new_qualified_name.database, new_qualified_name.table);
        }
    }

    /// ASTDictionary keeps a dictionary definition, for example
    /// PRIMARY KEY key_column1, key_column2
    /// SOURCE(CLICKHOUSE(host '127.0.0.1' port 9000 user 'default' password '' db 'default' table 'ids' where 'id=10' query 'SELECT id, value_1, value_2 FROM default.ids'))
    /// LAYOUT ... LIFETIME ... RANGE ...
    ///
    /// We'll try to replace database and table name in SOURCE if the specified `host` is local.
    /// TODO: Probably we could try to replace database and table name in `query` too.
    void visitDictionaryDef(ASTDictionary & dictionary, const DDLRenamingVisitor::Data & data)
    {
        if (!dictionary.source || dictionary.source->name != "clickhouse" || !dictionary.source->elements)
            return;

        auto config = getDictionaryConfigurationFromAST(data.create_query->as<ASTCreateQuery &>(), data.global_context);
        auto info = getInfoIfClickHouseDictionarySource(config, data.global_context);
        if (!info || !info->is_local || info->table_name.table.empty())
            return;

        auto * source_list = dictionary.source->elements->as<ASTExpressionList>();
        if (!source_list)
            return;

        auto & source_elements = source_list->children;

        Field * database_name_field = nullptr;
        Field * table_name_field = nullptr;

        for (const auto & source_element : source_elements)
        {
            if (!source_element)
                continue;

            auto * pair = source_element->as<ASTPair>();
            if (!pair || !pair->second)
                continue;

            auto * literal = pair->second->as<ASTLiteral>();
            if (!literal)
                continue;

            if (literal->value.getType() == Field::Types::String)
            {
                if (pair->first == "db")
                    database_name_field = &literal->value;
                else if (pair->first == "table")
                    table_name_field = &literal->value;
            }
        }

        if (database_name_field && table_name_field)
        {
            QualifiedTableName qualified_name{database_name_field->safeGet<String>(), table_name_field->safeGet<String>()};
            if (!qualified_name.database.empty() && !qualified_name.table.empty())
            {
                auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
                if (new_qualified_name != qualified_name)
                {
                    *database_name_field = new_qualified_name.database;
                    *table_name_field = new_qualified_name.table;
                }
            }
        }
    }

    /// Replaces a qualified table name in a specified function's argument.
    /// It can be either a string or an identifier with a dot in the middle.
    void replaceTableNameInArgument(const ASTFunction & function, const DDLRenamingVisitor::Data & data, size_t arg_idx, bool accept_string = true)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        auto & arg = function.arguments->as<ASTExpressionList>()->children[arg_idx];
        if (auto * literal = arg->as<ASTLiteral>())
        {
            if (!accept_string || literal->value.getType() != Field::Types::String)
                return;

            auto maybe_qualified_name = QualifiedTableName::tryParseFromString(literal->value.safeGet<String>());
            /// Just return if name if invalid
            if (!maybe_qualified_name || maybe_qualified_name->database.empty() || maybe_qualified_name->table.empty())
                return;

            auto new_qualified_name = data.renaming_map.getNewTableName(*maybe_qualified_name);
            literal->value = new_qualified_name.getFullName();
            return;
        }

        if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(arg.get()))
        {
            /// ASTIdentifier or ASTTableIdentifier
            auto table_identifier = identifier->createTable();
            /// Just return if table identified is invalid
            if (!table_identifier)
                return;

            QualifiedTableName qualified_name{table_identifier->getDatabaseName(), table_identifier->shortName()};
            if (qualified_name.database.empty() || qualified_name.table.empty())
                return;

            auto new_qualified_name = data.renaming_map.getNewTableName(qualified_name);
            arg = make_intrusive<ASTTableIdentifier>(new_qualified_name.database, new_qualified_name.table);
            return;
        }
    }

    /// Replaces a qualified database name in a specified function's argument.
    void replaceDatabaseNameInArguments(const ASTFunction & function, const DDLRenamingVisitor::Data & data, size_t arg_idx)
    {
        /// Just ignore incorrect arguments, proper exception will be thrown later
        if (!function.arguments || function.arguments->children.size() <= arg_idx)
            return;

        auto & arg = function.arguments->as<ASTExpressionList>()->children[arg_idx];
        auto * literal = arg->as<ASTLiteral>();
        if (!literal || (literal->value.getType() != Field::Types::String))
            return;

        auto database_name = literal->value.safeGet<String>();
        if (database_name.empty())
            return;

        auto new_database_name = data.renaming_map.getNewDatabaseName(database_name);
        literal->value = new_database_name;
    }

    void visitTableEngine(ASTStorage & storage, const DDLRenamingVisitor::Data & data)
    {
        if (!storage.engine)
            return;

        if (storage.engine->name == "Dictionary")
        {
            /// Syntax: CREATE TABLE table_name(<fields>) engine = Dictionary('dictionary_name')
            /// We'll try to replace the dictionary name.
            replaceTableNameInArgument(*storage.engine, data, 0);
        }
        else if (storage.engine->name == "Merge")
        {
            /// Syntax: CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
            /// We'll try to replace the database name but we can do nothing to 'tables_regexp'.
            replaceDatabaseNameInArguments(*storage.engine, data, 0);
        }
    }

    /// The argument of `function` that names a table, as in `dictGet('db.dict', ...)` or `x IN db.table`.
    std::optional<size_t> getTableNameArgumentIndex(const ASTFunction & function)
    {
        if (functionIsJoinGet(function.name) || functionIsDictGet(function.name) || function.name == "dictionary")
            return 0;
        if (functionIsInOrGlobalInOperator(function.name))
            return 1;
        return {};
    }

    void visitFunction(const ASTFunction & function, const DDLRenamingVisitor::Data & data)
    {
        if (auto table_name_argument = getTableNameArgumentIndex(function))
        {
            /// A string on the right side of `IN` is a value, not a table name.
            replaceTableNameInArgument(function, data, *table_name_argument, /*accept_string=*/ !functionIsInOrGlobalInOperator(function.name));
        }
        else if (function.name == "merge")
        {
            /// Syntax: merge('db_name', 'tables_regexp')
            /// We'll try to replace the database name but we can do nothing to 'tables_regexp'.
            replaceDatabaseNameInArguments(function, data, 0);
        }
    }
}

void DDLRenamingVisitor::visit(ASTPtr ast, const Data & data)
{
    if (auto * create = ast->as<ASTCreateQuery>())
        visitCreateQuery(*create, data);
    else if (auto * expr = ast->as<ASTTableExpression>())
        visitTableExpression(*expr, data);
    else if (auto * refresh = ast->as<ASTRefreshStrategy>())
        visitRefreshStrategy(*refresh, data);
    else if (auto * function = ast->as<ASTFunction>())
        visitFunction(*function, data);
    else if (auto * dictionary = ast->as<ASTDictionary>())
        visitDictionaryDef(*dictionary, data);
    else if (auto * storage = ast->as<ASTStorage>())
        visitTableEngine(*storage, data);
}

bool DDLRenamingVisitor::needChildVisit(const ASTPtr & node, const ASTPtr &) { return !isRemoteTableFunction(*node); }


void DDLRenamingMap::setNewTableName(const QualifiedTableName & old_table_name, const QualifiedTableName & new_table_name)
{
    if (old_table_name.table.empty() || old_table_name.database.empty() || new_table_name.table.empty() || new_table_name.database.empty())
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS, "Empty names are not allowed");

    auto it = old_to_new_table_names.find(old_table_name);
    if ((it != old_to_new_table_names.end()))
    {
        if (it->second == new_table_name)
            return;
        throw Exception(
            ErrorCodes::WRONG_DDL_RENAMING_SETTINGS,
            "Wrong renaming: it's specified that table {} should be renamed to {} and to {} at the same time",
            old_table_name.getFullName(),
            it->second.getFullName(),
            new_table_name.getFullName());
    }
    old_to_new_table_names[old_table_name] = new_table_name;
}

void DDLRenamingMap::setNewDatabaseName(const String & old_database_name, const String & new_database_name)
{
    if (old_database_name.empty() || new_database_name.empty())
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS, "Empty names are not allowed");

    auto it = old_to_new_database_names.find(old_database_name);
    if ((it != old_to_new_database_names.end()))
    {
        if (it->second == new_database_name)
            return;
        throw Exception(ErrorCodes::WRONG_DDL_RENAMING_SETTINGS,
                        "Wrong renaming: it's specified that database {} should be renamed "
                        "to {} and to {} at the same time",
                        backQuoteIfNeed(old_database_name), backQuoteIfNeed(it->second),
                        backQuoteIfNeed(new_database_name));
    }
    old_to_new_database_names[old_database_name] = new_database_name;
}


String DDLRenamingMap::getNewDatabaseName(const String & old_database_name) const
{
    auto it = old_to_new_database_names.find(old_database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return old_database_name;
}

QualifiedTableName DDLRenamingMap::getNewTableName(const QualifiedTableName & old_table_name) const
{
    auto it = old_to_new_table_names.find(old_table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(old_table_name.database), old_table_name.table};
}


void renameDatabaseAndTableNameInCreateQuery(ASTPtr ast, const DDLRenamingMap & renaming_map, const ContextPtr & global_context)
{
    DDLRenamingVisitor::Data data{ast, renaming_map, global_context};
    DDLRenamingVisitor::Visitor{data}.visit(ast);
}


namespace
{
    const ASTPtr * tryGetMatcherQualifier(const IAST & ast)
    {
        if (const auto * asterisk = ast.as<ASTQualifiedAsterisk>())
            return &asterisk->qualifier;
        if (const auto * regexp_matcher = ast.as<ASTQualifiedColumnsRegexpMatcher>())
            return &regexp_matcher->qualifier;
        if (const auto * list_matcher = ast.as<ASTQualifiedColumnsListMatcher>())
            return &list_matcher->qualifier;
        return nullptr;
    }

    Strings splitIntoComponents(const String & name)
    {
        Strings components;
        splitInto<'.'>(components, name);
        return components;
    }

    /// Whether the column name `name` contains, outside its string literals, `database.table.column` as the whole name
    /// or inside an expression like `abs(database.table.column)`, or `database.table` for a table of `in_tables`, as in
    /// `in(x, database.table)`. Or a literal of `function_literals` as the first argument of `dictGet` or `joinGet`.
    bool containsNameQualifiedWithDatabase(
        const String & name, const String & database, const NameSet & in_tables, const NameSet & function_literals)
    {
        const String prefix = database + ".";
        bool in_literal = false;
        bool after_prefix = false;
        for (size_t pos = 0; pos < name.size(); ++pos)
        {
            if (in_literal)
            {
                if (name[pos] == '\\')
                    ++pos;
                else if (name[pos] == '\'')
                    in_literal = false;
            }
            else if ((pos == 0 || !isWordCharASCII(name[pos - 1])) && name.compare(pos, prefix.size(), prefix) == 0)
            {
                size_t table_pos = pos + prefix.size();
                for (const auto & table : in_tables)
                {
                    size_t table_end = table_pos + table.size();
                    if (name.compare(table_pos, table.size(), table) == 0
                        && (table_end == name.size() || (!isWordCharASCII(name[table_end]) && name[table_end] != '.')))
                        return true;
                }
                after_prefix = true;
                pos += database.size();
            }
            else if (name[pos] == '.' && after_prefix)
            {
                return true;
            }
            else if (name[pos] == '\'')
            {
                in_literal = true;
                if (pos == 0 || name[pos - 1] != '(')
                    continue;
                size_t function_start = pos - 1;
                while (function_start > 0 && isWordCharASCII(name[function_start - 1]))
                    --function_start;
                String function = name.substr(function_start, pos - 1 - function_start);
                if ((functionIsJoinGet(function) || functionIsDictGet(function))
                    && std::ranges::any_of(function_literals, [&](const String & literal) { return name.compare(pos, literal.size(), literal) == 0; }))
                    return true;
            }
        }
        return false;
    }

    /// The table expressions of a SELECT, ARRAY JOIN elements excluded.
    std::vector<const ASTTableExpression *> getTableExpressions(const ASTSelectQuery & select)
    {
        std::vector<const ASTTableExpression *> res;
        if (auto tables = select.tables())
        {
            for (const auto & element : tables->children)
            {
                const auto * tables_element = element->as<ASTTablesInSelectQueryElement>();
                if (tables_element && tables_element->table_expression)
                    res.push_back(tables_element->table_expression->as<ASTTableExpression>());
            }
        }
        return res;
    }

    const ASTIdentifier * tryGetTableName(const ASTTableExpression & table_expression)
    {
        const auto * identifier = dynamic_cast<const ASTIdentifier *>(table_expression.database_and_table_name.get());
        if (!identifier || identifier->isParam() || identifier->name_parts.size() > 2)
            return nullptr;
        return identifier;
    }

    /// The table functions that read a table of a local database, with the number of arguments of their form whose first
    /// argument is the database (0 if every form). The form with one argument less takes the table, maybe as `db.t`.
    constexpr std::pair<std::string_view, size_t> local_table_functions[] = {
        {"loop", 2}, {"merge", 2}, {"mergeTreeAnalyzeIndexes", 0}, {"mergeTreeCodecBlockCounts", 0}, {"mergeTreeIndex", 0},
        {"mergeTreeProjection", 0}, {"mergeTreeTextIndex", 0}, {"prometheusQuery", 4}, {"prometheusQueryRange", 6},
        {"timeSeriesData", 2}, {"timeSeriesMetricFamilies", 2}, {"timeSeriesMetrics", 2}, {"timeSeriesSamples", 2},
        {"timeSeriesSelector", 5}, {"timeSeriesTags", 2}, {"timeSeriesTimeRanges", 2}};

    /// Whether the table function `function` reads a table of `database` or is a parameterized view call `database.view(...)`.
    bool tableFunctionNamesDatabase(const ASTFunction & function, const String & database)
    {
        if (function.name.starts_with(database + "."))
            return true;
        const auto * form = std::ranges::find(local_table_functions, std::string_view(function.name), [](const auto & entry) { return entry.first; });
        if (form == std::end(local_table_functions) || !function.arguments || function.arguments->children.empty())
            return false;

        const auto & arguments = function.arguments->children;
        const auto * identifier = dynamic_cast<const ASTIdentifier *>(arguments[0].get());
        if (identifier && identifier->isParam())
            return false;
        if (form->second == 0 || arguments.size() == form->second)
        {
            if (const auto * literal = arguments[0]->as<ASTLiteral>())
                return literal->value.getType() == Field::Types::String && literal->value.safeGet<String>() == database;
            return identifier && identifier->name() == database;
        }
        return arguments.size() + 1 == form->second && identifier && identifier->name_parts.size() == 2
            && identifier->name_parts[0] == database;
    }

    /// `function`, or a function passed to it as in `loop(mergeTreeIndex(...))`, that names `database`.
    const ASTFunction * findInTableFunction(const ASTFunction & function, const String & database)
    {
        if (isRemoteTableFunction(function))
            return nullptr;
        if (tableFunctionNamesDatabase(function, database))
            return &function;
        if (function.arguments)
            for (const auto & argument : function.arguments->children)
                if (const auto * nested = argument->as<ASTFunction>())
                    if (const auto * found = findInTableFunction(*nested, database))
                        return found;
        return nullptr;
    }

    const ASTFunction * findTableFunctionNamingDatabase(const IAST & ast, const String & database)
    {
        if (isRemoteTableFunction(ast))
            return nullptr;
        if (const auto * table_expression = ast.as<ASTTableExpression>(); table_expression && table_expression->table_function)
            if (const auto * function = table_expression->table_function->as<ASTFunction>())
                if (const auto * found = findInTableFunction(*function, database))
                    return found;
        for (const auto & child : ast.children)
            if (const auto * found = findTableFunctionNamingDatabase(*child, database))
                return found;
        return nullptr;
    }
}

DatabaseRenamingInViewDefinition::DatabaseRenamingInViewDefinition(
    ASTPtr create_query_, String old_database_name_, String new_database_name_, QualifiedTableName view_name_)
    : create_query(std::move(create_query_))
    , old_database_name(std::move(old_database_name_))
    , new_database_name(std::move(new_database_name_))
    , view_name(std::move(view_name_))
{
    collect(create_query, NamePosition::Expression);
}

void DatabaseRenamingInViewDefinition::addName(const Strings & parts, bool multi_component)
{
    WrittenName name;
    name.multi_component = multi_component || parts.size() > 1;
    for (const auto & part : parts)
    {
        auto components = splitIntoComponents(part);
        name.multi_component |= components.size() > 1;
        name.components.insert(name.components.end(), components.begin(), components.end());
    }
    names.push_back(std::move(name));
}

void DatabaseRenamingInViewDefinition::addTableArgument(const ASTFunction & function, const IAST & argument)
{
    bool is_in = functionIsInOrGlobalInOperator(function.name);
    String table_name;
    if (const auto * literal = argument.as<ASTLiteral>(); literal && !is_in && literal->value.getType() == Field::Types::String)
        table_name = literal->value.safeGet<String>();
    else if (const auto * identifier = dynamic_cast<const ASTIdentifier *>(&argument); identifier && !identifier->isParam())
        table_name = identifier->name();

    auto qualified_name = QualifiedTableName::tryParseFromString(table_name);
    if (!qualified_name || qualified_name->database != old_database_name)
        return;
    if (is_in)
        in_tables.insert(qualified_name->table);
    else
        function_literals.insert(applyVisitor(FieldVisitorToString(), Field(table_name)));
}

void DatabaseRenamingInViewDefinition::collect(const ASTPtr & ast, NamePosition position)
{
    if (!ast)
        return;

    if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(ast.get()); with_alias && !with_alias->alias.empty())
        addName({with_alias->alias});

    if (auto * identifier = dynamic_cast<ASTIdentifier *>(ast.get()))
    {
        collectIdentifier(*identifier, position);
    }
    else if (const auto * create = ast->as<ASTCreateQuery>())
    {
        /// The name of the definition itself is not a reference.
        for (const auto & child : ast->children)
            if (child != create->database && child != create->table)
                collect(child, NamePosition::Expression);
    }
    else if (const auto * select = ast->as<ASTSelectQuery>())
    {
        current_scopes.push_back(select);
        for (const auto & child : ast->children)
            collect(child, NamePosition::Expression);
        current_scopes.pop_back();
    }
    else if (const auto * table_expression = ast->as<ASTTableExpression>())
    {
        for (const auto & child : ast->children)
            collect(child, child == table_expression->database_and_table_name ? NamePosition::Table : NamePosition::Expression);
    }
    else if (const auto * function = ast->as<ASTFunction>())
    {
        if (isRemoteTableFunction(*function))
            return;

        auto table_name_argument = getTableNameArgumentIndex(*function);
        for (const auto & child : ast->children)
        {
            if (child != function->arguments || !table_name_argument)
            {
                collect(child, NamePosition::Expression);
                continue;
            }
            for (size_t i = 0; i < child->children.size(); ++i)
            {
                if (i == *table_name_argument)
                    addTableArgument(*function, *child->children[i]);
                collect(child->children[i], i == *table_name_argument ? NamePosition::Table : NamePosition::Expression);
            }
        }
    }
    else if (const auto * refresh = ast->as<ASTRefreshStrategy>())
    {
        for (const auto & child : ast->children)
        {
            if (child != refresh->dependencies)
            {
                collect(child, NamePosition::Expression);
                continue;
            }
            for (const auto & dependency : child->children)
                collect(dependency, NamePosition::Table);
        }
    }
    else if (const auto * with_element = ast->as<ASTWithElement>())
    {
        cte_names.insert(with_element->name);
        addName({with_element->name});
        for (const auto & child : ast->children)
            collect(child, NamePosition::Expression);
    }
    else if (const auto * qualifier = tryGetMatcherQualifier(*ast))
    {
        for (const auto & child : ast->children)
            collect(child, child == *qualifier ? NamePosition::Qualifier : NamePosition::Expression);
    }
    else
    {
        for (const auto & child : ast->children)
            collect(child, NamePosition::Expression);
    }
}

void DatabaseRenamingInViewDefinition::collectIdentifier(ASTIdentifier & identifier, NamePosition position)
{
    /// The name of a query parameter is only known when the view is called.
    if (identifier.isParam())
        return;

    const auto & parts = identifier.name_parts;

    if ((position == NamePosition::Table || identifier.as<ASTTableIdentifier>()) && parts.size() <= 2)
    {
        /// The database part is renamed with the table reference.
        addName({parts.back()});
        return;
    }

    bool can_be_qualified_with_database = position == NamePosition::Qualifier ? parts.size() == 2 : parts.size() >= 3;
    if (can_be_qualified_with_database && parts[0] == old_database_name)
    {
        if (auto scope = findTableExpressionScope(parts[1]))
        {
            candidates.push_back(&identifier);
            for (size_t i = *scope; i < current_scopes.size(); ++i)
            {
                for (const auto * table_expression : getTableExpressions(*current_scopes[i]))
                {
                    const auto * table_name = tryGetTableName(*table_expression);
                    scope_table_expressions.push_back({identifier.name(), table_name ? table_name->name_parts : Strings{}});
                }
            }
            addName(Strings(parts.begin() + 1, parts.end()), /*multi_component=*/ true);
            return;
        }
    }

    addName(parts);
}

std::optional<size_t> DatabaseRenamingInViewDefinition::findTableExpressionScope(const String & table_name) const
{
    for (size_t i = current_scopes.size(); i > 0; --i)
    {
        for (const auto * table_expression : getTableExpressions(*current_scopes[i - 1]))
        {
            const auto * identifier = tryGetTableName(*table_expression);
            if (identifier && identifier->name_parts.size() == 2 && identifier->name_parts[0] == old_database_name
                && identifier->name_parts[1] == table_name)
                return i - 1;
        }
    }
    return {};
}

std::vector<QualifiedTableName> DatabaseRenamingInViewDefinition::getScopeTables(bool throw_if_not_table) const
{
    std::set<QualifiedTableName> tables;
    for (const auto & table_expression : scope_table_expressions)
    {
        const auto & parts = table_expression.table_name_parts;
        if (parts.empty() || (parts.size() == 1 && cte_names.contains(parts[0])))
        {
            if (throw_if_not_table)
                throwAmbiguous(fmt::format("{} is used next to a subquery, a table function or a CTE", table_expression.renamed_name));
            continue;
        }
        tables.insert({parts.size() == 2 ? parts[0] : view_name.database, parts.back()});
    }
    return {tables.begin(), tables.end()};
}

std::vector<QualifiedTableName> DatabaseRenamingInViewDefinition::getTablesToCheck() const
{
    return getScopeTables(/*throw_if_not_table=*/ false);
}

void DatabaseRenamingInViewDefinition::check(const ColumnsDescription & view_columns, const ContextPtr & context) const
{
    auto is_renamed_database = [&](const String & name) { return name == old_database_name || name == new_database_name; };

    if (!candidates.empty() && (old_database_name.contains('.') || new_database_name.contains('.')))
        throwAmbiguous("a database name contains a dot");

    for (const auto & name : names)
    {
        if ((name.multi_component || !candidates.empty()) && std::ranges::any_of(name.components, is_renamed_database))
            throwAmbiguous(fmt::format("it uses the name {}", fmt::join(name.components, ".")));
    }

    /// A column named `db.table.column` is how the analyzer names a column qualified with the database.
    for (const auto & column : view_columns)
        if (containsNameQualifiedWithDatabase(column.name, old_database_name, in_tables, function_literals))
            throwAmbiguous(fmt::format("it has the column {}", column.name));

    if (candidates.empty())
        return;

    for (const auto & table_name : getScopeTables(/*throw_if_not_table=*/ true))
    {
        auto table = DatabaseCatalog::instance().tryGetTable(StorageID{table_name.database, table_name.table}, context);
        if (!table)
            throwAmbiguous(fmt::format("table {} does not exist", table_name.getFullName()));

        auto metadata = table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ true);
        auto check_column = [&](const String & column_name)
        {
            auto components = splitIntoComponents(column_name);
            if (!components.empty() && is_renamed_database(components.front()))
                throwAmbiguous(fmt::format("table {} has the column {}", table_name.getFullName(), column_name));
        };
        for (const auto & column : metadata->getColumns())
            check_column(column.name);
        for (const auto & column : metadata->virtuals)
            check_column(column.name);
    }
}

void DatabaseRenamingInViewDefinition::apply(const ContextPtr & global_context)
{
    for (auto * candidate : candidates)
        candidate->setNamePart(0, new_database_name);

    DDLRenamingMap renaming_map;
    renaming_map.setNewDatabaseName(old_database_name, new_database_name);
    renameDatabaseAndTableNameInCreateQuery(create_query, renaming_map, global_context);
}

void DatabaseRenamingInViewDefinition::checkTableFunctions() const
{
    if (const auto * function = findTableFunctionNamingDatabase(*create_query, old_database_name))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot rename database {} to {}, because {} refers to it in the table function {}, which the rename cannot rewrite. "
            "Drop or rewrite it before renaming",
            backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), view_name.getFullName(), function->name);
}

void DatabaseRenamingInViewDefinition::throwAmbiguous(const String & reason) const
{
    throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
        "Cannot rename database {} to {}, because the rename could change what the names in {} refer to: {}. "
        "Qualify the columns with the table name or an alias instead of the database name",
        backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), view_name.getFullName(), reason);
}

}
