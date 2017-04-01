#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/CollectTables.h>
#include <DB/Analyzers/AnalyzeColumns.h>
#include <DB/Analyzers/AnalyzeLambdas.h>
#include <DB/Analyzers/TypeAndConstantInference.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Common/Exception.h>
#include <DB/Interpreters/Context.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Databases/DatabaseMemory.h>


/// Parses query from stdin and print data types of expressions; and for constant expressions, print its values.

int main(int argc, char ** argv)
try
{
    using namespace DB;

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
