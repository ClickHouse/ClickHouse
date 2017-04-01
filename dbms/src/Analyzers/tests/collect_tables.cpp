#include <DB/Analyzers/CollectAliases.h>
#include <DB/Analyzers/CollectTables.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Common/Exception.h>
#include <DB/Interpreters/Context.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Databases/DatabaseMemory.h>


/// Parses query from stdin and print found tables.

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
    context.setCurrentDatabase("system");
    system_database->attachTable("one",            StorageSystemOne::create("one"));
    system_database->attachTable("numbers",     StorageSystemNumbers::create("numbers"));

    CollectAliases collect_aliases;
    collect_aliases.process(ast);

    CollectTables collect_tables;
    collect_tables.process(ast, context, collect_aliases);
    collect_tables.dump(out);

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
