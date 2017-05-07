#include <Analyzers/CollectAliases.h>
#include <Analyzers/CollectTables.h>
#include <Analyzers/AnalyzeColumns.h>
#include <Analyzers/AnalyzeLambdas.h>
#include <Analyzers/TypeAndConstantInference.h>
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
#include <Functions/registerFunctions.h>


/// Parses query from stdin and print data types of expressions; and for constant expressions, print its values.

int main(int argc, char ** argv)
try
{
    using namespace DB;

    registerFunctions();

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String query;
    readStringUntilEOF(query, in);

    ParserSelectQuery parser;
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "query");

    Context context;

    auto system_database = std::make_shared<DatabaseMemory>("system");
    context.addDatabase("system", system_database);
    system_database->attachTable("one",            StorageSystemOne::create("one"));
    system_database->attachTable("numbers",     StorageSystemNumbers::create("numbers"));
    context.setCurrentDatabase("system");

    AnalyzeLambdas analyze_lambdas;
    analyze_lambdas.process(ast);

    CollectAliases collect_aliases;
    collect_aliases.process(ast);

    CollectTables collect_tables;
    collect_tables.process(ast, context, collect_aliases);

    AnalyzeColumns analyze_columns;
    analyze_columns.process(ast, collect_aliases, collect_tables);

    TypeAndConstantInference inference;
    inference.process(ast, context, collect_aliases, analyze_columns, analyze_lambdas);

    inference.dump(out);
    out.next();

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
