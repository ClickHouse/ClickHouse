#include <Storages/StorageFuzzer.h>

#include <Common/typeid_cast.h>
#include <common/logger_useful.h>
#include <Columns/ColumnString.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/QueryFuzzer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Storages/StorageFactory.h>

namespace DB
{

class FuzzerSink : public SinkToStorage
{
public:
    explicit FuzzerSink(const Block & header, ContextPtr context_)
        : SinkToStorage(header), context(Context::createCopy(context_))
    {
        log = &Poco::Logger::get("FuzzerSink");
    }

    void consume(Chunk chunk) override;
    void onStart() override {}
    void onFinish() override {}
    String getName() const override { return "FuzzerSink"; }

private:
    void fuzzAndExecuteQuery(StringRef query);

    ContextMutablePtr context;

    Poco::Logger * log{nullptr};

    QueryFuzzer fuzzer;
};


void FuzzerSink::fuzzAndExecuteQuery(StringRef query)
{
    LOG_INFO(log, "Fuzzing {}", query.data);

    ASTPtr orig_ast;

    try
    {
        DB::ParserQuery parser(query.data + query.size);
        orig_ast = parseQuery(parser, query.data, query.data + query.size, "", 0, 1000);
    }
    catch (...)
    {
        /// We got the query from query log, and most likely we
        /// have enabled setting to limit the length of a query.
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    /// The same logic as in fuzzer built-in Client.cpp:
    /// We don't want to fuzz create, insert and drop queries
    if (orig_ast->as<ASTInsertQuery>() || orig_ast->as<ASTCreateQuery>() || orig_ast->as<ASTDropQuery>())
        return;


    for (size_t i = 0; i < 100; ++i)
    {
        fuzzer.fuzzMain(orig_ast);

        WriteBufferFromOwnString dump_before_fuzz;
        orig_ast->dumpTree(dump_before_fuzz);

        LOG_INFO(log, "Got query {}, will try to execute", orig_ast->formatForErrorMessage());

        auto io = InterpreterFactory::get(orig_ast, context)->execute();

        PullingPipelineExecutor executor(io.pipeline);
        Block res;
        while (!res && executor.pull(res));

        LOG_INFO(log, "Successfully executed");
    }
}


void FuzzerSink::consume(Chunk chunk)
{
    assert(chunk.getNumColumns() == 1);
    auto column = chunk.detachColumns().front();

    const auto & column_string = typeid_cast<const ColumnString &>(*column);

    for (size_t i = 0; i < column_string.size(); ++i)
    {
        auto query = column_string.getDataAt(i);
        fuzzAndExecuteQuery(query);
    }
}


SinkToStoragePtr StorageFuzzer::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<FuzzerSink>(metadata_snapshot->getSampleBlock(), context);
}


void registerStorageFuzzer(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    factory.registerStorage("Fuzzer", [](const StorageFactory::Arguments & factory_args)
    {
        return StorageFuzzer::create(
            factory_args.table_id,
            factory_args.columns,
            factory_args.constraints,
            factory_args.comment
        );
    }, features);
}

}
