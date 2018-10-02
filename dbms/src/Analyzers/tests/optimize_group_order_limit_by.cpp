#include <Analyzers/CollectAliases.h>
#include <Analyzers/CollectTables.h>
#include <Analyzers/AnalyzeColumns.h>
#include <Analyzers/AnalyzeLambdas.h>
#include <Analyzers/ExecuteTableFunctions.h>
#include <Analyzers/TypeAndConstantInference.h>
#include <Analyzers/TranslatePositionalArguments.h>
#include <Analyzers/OptimizeGroupOrderLimitBy.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/formatAST.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Databases/DatabaseMemory.h>


/// Parses query from stdin and print same query with optimized GROUP BY, ORDER BY, LIMIT BY.

int main(int, char **)
try
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String query;
    readStringUntilEOF(query, in);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "query", 0);

    Context context = Context::createGlobal();

    auto system_database = std::make_shared<DatabaseMemory>("system");
    context.addDatabase("system", system_database);
    system_database->attachTable("one", StorageSystemOne::create("one"));
    system_database->attachTable("numbers", StorageSystemNumbers::create("numbers", false));
    context.setCurrentDatabase("system");

    AnalyzeLambdas analyze_lambdas;
    analyze_lambdas.process(ast);

    CollectAliases collect_aliases;
    collect_aliases.process(ast);

    ExecuteTableFunctions execute_table_functions;
    execute_table_functions.process(ast, context);

    CollectTables collect_tables;
    collect_tables.process(ast, context, collect_aliases, execute_table_functions);

    AnalyzeColumns analyze_columns;
    analyze_columns.process(ast, collect_aliases, collect_tables);

    TypeAndConstantInference inference;
    inference.process(ast, context, collect_aliases, analyze_columns, analyze_lambdas, execute_table_functions);

    TranslatePositionalArguments translation;
    translation.process(ast);

    OptimizeGroupOrderLimitBy optimizer;
    optimizer.process(ast, inference);

    formatAST(*ast, std::cout, false);
    std::cout << "\n";
    return 0;

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
