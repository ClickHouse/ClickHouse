#include <Analyzers/AnalyzeResultOfQuery.h>
#include <Analyzers/ExecuteTableFunctions.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Databases/DatabaseMemory.h>


/// Parses query from stdin and print names and types of result columns.

int main(int, char **)
try
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String query;
    readStringUntilEOF(query, in);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "query");

    Context context = Context::createGlobal();

    auto system_database = std::make_shared<DatabaseMemory>("system");
    context.addDatabase("system", system_database);
    context.setCurrentDatabase("system");
    system_database->attachTable("one", StorageSystemOne::create("one"));
    system_database->attachTable("numbers", StorageSystemNumbers::create("numbers", false));

    ExecuteTableFunctions execute_table_functions;
    execute_table_functions.process(ast, context);

    AnalyzeResultOfQuery analyzer;
    analyzer.process(ast, context, execute_table_functions);

    analyzer.dump(out);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
