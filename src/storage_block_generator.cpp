#include <ext/shared_ptr_helper.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/Settings.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/IDatabase.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Sources/PushingSource.h>
#include <Storages/IStorage.h>
#include <Common/typeid_cast.h>

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Transforms/MaterializingTransform.h>

using namespace DB;

bool parse(ASTPtr & ast, const std::string & query)
{
    ParserSelectQuery parser;
    std::string message;
    const auto * begin = query.data();
    const auto * end = begin + query.size();
    ast = tryParseQuery(parser, begin, end, message, false, "", false, 0, 0);
    return ast != nullptr;
}

auto getBlock(const std::vector<std::pair<std::string, std::string>> & structure)
{
    Block sample_block;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    for (const auto & elem : structure)
    {
        ColumnWithTypeAndName column;
        column.name = elem.first;
        column.type = data_type_factory.get(elem.second);
        column.column = column.type->createColumn();
        sample_block.insert(std::move(column));
    }
    return sample_block;
}

bool run()
{
    static SharedContextHolder shared_context = Context::createShared();
    static Context context = Context::createGlobal(shared_context.get());
    context.makeGlobalContext();
    context.makeQueryContext();

    registerFunctions();
    registerAggregateFunctions();

    try
    {
        Block sample = getBlock({{"i", "int"}});

        // auto storage = StorageBlockGenerator::create("remote_db", "remote_visits", entry.shard_count);
        auto in = std::make_shared<PushingSource>(sample);

        DatabasePtr database = std::make_shared<DatabaseOrdinary>(
            "default", "/home/zhengtianqi/gentoo.tmp/home/amos/git/ClickHouse/data/metadata/default/", context);
        DatabaseCatalog::instance().attachDatabase("default", database);
        // database->attachTable("a", storage);
        // context.setCurrentDatabase("test");

        ASTPtr query;
        if (!parse(query, "select count() where i % 2 = 1 "))
        {
            DUMP("parse error");
            return false;
        }

        // Console output.
        WriteBufferFromFileDescriptor std_out{STDOUT_FILENO};

        // auto pipeline = InterpreterSelectQuery(query, context, SelectQueryOptions()).execute().pipeline;
        auto pipeline = InterpreterSelectQuery(query, context, Pipe(in)).execute().pipeline;

        pipeline.addSimpleTransform([](const Block & header) { return std::make_shared<MaterializingTransform>(header); });

        // TODO set to a part writer
        auto out = context.getOutputFormatProcessor("Pretty", std_out, pipeline.getHeader());
        out->setAutoFlush();

        pipeline.setOutputFormat(std::move(out));

        auto executor = std::make_unique<PushingPipelineExecutor>(pipeline, *in);

        for (auto i = 0ul; i < 10; ++i)
        {
            Block a = sample.cloneEmpty();
            auto columns = a.mutateColumns();
            columns[0]->insert(i);
            a.setColumns(std::move(columns));
            DUMP(a);
            if (!executor->push(a))
                return false;
        }

        std::cout << "push done!" << std::endl;
        executor->push({});
        DatabaseCatalog::instance().detachDatabase("default");
        return true;
    }
    catch (Exception & e)
    {
        DatabaseCatalog::instance().detachDatabase("default");
        std::cerr << e.displayText() << std::endl;
        std::cerr << e.getStackTraceString() << std::endl;
        return false;
    }
}

int main()
{
    return run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
