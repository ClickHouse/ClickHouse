#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Storages/StorageMerge.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <TableFunctions/TableFunctionMerge.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_TABLE;
}

namespace
{
    [[noreturn]] void throwNoTablesMatchRegexp(const String & source_database_regexp, const String & source_table_regexp)
    {
        throw Exception(
            "Error while executing table function merge. Neither no one database matches regular expression " + source_database_regexp
                + " nor in database matches " + source_database_regexp + " no one table matches regular expression: " + source_table_regexp,
            ErrorCodes::UNKNOWN_TABLE);
    }
}


void TableFunctionMerge::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function 'merge' requires exactly 2 arguments"
            " - name of source database and regexp for table names.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 2)
        throw Exception("Table function 'merge' requires exactly 2 arguments"
            " - name of source database and regexp for table names.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
    args[1] = evaluateConstantExpressionAsLiteral(args[1], context);

    source_database_regexp = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    source_table_regexp = args[1]->as<ASTLiteral &>().value.safeGet<String>();
}


const std::unordered_map<String, std::unordered_set<String>> & TableFunctionMerge::getSourceDatabasesAndTables(ContextPtr context) const
{
    if (source_databases_and_tables)
        return *source_databases_and_tables;

    OptimizedRegularExpression database_re(source_database_regexp);
    OptimizedRegularExpression table_re(source_table_regexp);

    auto table_name_match = [&](const String & table_name_) { return table_re.fullMatch(table_name_); };

    auto access = context->getAccess();

    auto databases = DatabaseCatalog::instance().getDatabases();

    for (const auto & db : databases)
    {
        if (database_re.fullMatch(db.first))
        {
            bool granted_show_on_all_tables = access->isGranted(AccessType::SHOW_TABLES, db.first);
            bool granted_select_on_all_tables = access->isGranted(AccessType::SELECT, db.first);
            std::unordered_set<String> source_tables;
            for (auto it = db.second->getTablesIterator(context, table_name_match); it->isValid(); it->next())
            {
                if (!it->table())
                    continue;
                bool granted_show = granted_show_on_all_tables || access->isGranted(AccessType::SHOW_TABLES, db.first, it->name());
                if (!granted_show)
                    continue;
                if (!granted_select_on_all_tables)
                    access->checkAccess(AccessType::SELECT, db.first, it->name());
                source_tables.insert(it->name());
            }

            if (!source_tables.empty())
              (*source_databases_and_tables)[db.first] = source_tables;
        }
    }

    if ((*source_databases_and_tables).empty())
        throwNoTablesMatchRegexp(source_database_regexp, source_table_regexp);

    return *source_databases_and_tables;
}


ColumnsDescription TableFunctionMerge::getActualTableStructure(ContextPtr context) const
{
    for (const auto & db_with_tables : getSourceDatabasesAndTables(context))
    {
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{db_with_tables.first, *db_with_tables.second.begin()}, context);
        if (storage)
            return ColumnsDescription{storage->getInMemoryMetadataPtr()->getColumns().getAllPhysical()};
    }

    throwNoTablesMatchRegexp(source_database_regexp, source_table_regexp);
}


StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto res = StorageMerge::create(
        StorageID(getDatabaseName(), table_name),
        getActualTableStructure(context),
        String{},
        source_database_regexp,
        getSourceDatabasesAndTables(context),
        context);

    res->startup();
    return res;
}


void registerTableFunctionMerge(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMerge>();
}

}
