#include <Storages/StorageFuzzer.h>

#include <common/logger_useful.h>
#include <Columns/ColumnString.h>
#include <Parsers/QueryFuzzer.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageFactory.h>

namespace DB
{

class FuzzerSink : public SinkToStorage
{
public:
    explicit FuzzerSink(const Block & header) : SinkToStorage(header)
    {
        log = &Poco::Logger::get("FuzzerSink");
    }

    void consume(Chunk chunk) override;
    void onStart() override {}
    void onFinish() override {}
    String getName() const override { return "FuzzerSink"; }

private:
    void fuzzAndExecuteQuery(StringRef query);

    Poco::Logger * log{nullptr};

    QueryFuzzer fuzzer;
};


void FuzzerSink::fuzzAndExecuteQuery(StringRef query)
{
    LOG_INFO(log, "Starting to fuzz {}", query.data);

    ASTPtr orig_ast;

    try
    {
        DB::ParserQuery parser(query.data + query.size);
        orig_ast = parseQuery(parser, query.data, query.data + query.size, "", 0, 1000);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    for (size_t i = 0; i < 100; ++i)
    {
        fuzzer.fuzzMain(orig_ast);

        WriteBufferFromOwnString dump_before_fuzz;
        orig_ast->dumpTree(dump_before_fuzz);

        LOG_INFO(log, "Fuzzed {}", dump_before_fuzz.str());
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


SinkToStoragePtr StorageFuzzer::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    return std::make_shared<FuzzerSink>(metadata_snapshot->getSampleBlock());
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
