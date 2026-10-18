#include <cstddef>
#include <vector>
#include <memory>
#include <mutex>
#include <boost/core/noncopyable.hpp>
#include <gtest/gtest.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/ThreadPool.h>
#include <Common/tests/gtest_global_register.h>

#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/PatternFormatter.h>
#include <Common/tests/gtest_global_context.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/ProtocolDefines.h>
#include <Core/UUID.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/registerDisks.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/Session.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Processors/Sinks/NativeCompressedSink.h>
#include <Processors/Sources/NativeCompressedSource.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <base/defines.h>

#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Server/StatelessWorker/StatelessWorkerTaskSerialization.h>
#include <Common/Exception.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IParameterLookup.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>

#include <QueryPipeline/QueryPipeline.h>

#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/ConfigHelper.h>
#include <Core/ServerUUID.h>


namespace DB
{

/// Read test data from a file
class ReadFromFileStep : public ISourceStep
{
public:
    ReadFromFileStep(SharedHeader header_, const String & file_name_)
        : ISourceStep(std::move(header_))
        , file_name(file_name_)
    {
    }

    [[nodiscard]] String getName() const override { return "ReadFromFile"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/) override
    {
        pipeline.init(Pipe(std::make_shared<NativeCompressedSource>(output_header, std::make_unique<ReadBufferFromFile>(file_name), file_name)));
    }

    void serialize(Serialization & ctx) const override
    {
        writeStringBinary(file_name, ctx.out);
    }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx)
    {
        String file_name;
        readStringBinary(file_name, ctx.in);
        return std::make_unique<ReadFromFileStep>(ctx.output_header, file_name);
    }

private:
    const String file_name;
};

static void registerReadFromFileStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("ReadFromFile", ReadFromFileStep::deserialize);
}


/// Print test result
class PrintTSVSink : public ISink
{
public:
    explicit PrintTSVSink(SharedHeader header_)
        : ISink(std::move(header_))
        , out("/dev/stdout")
        , output_format(std::make_shared<TabSeparatedRowOutputFormat>(out, input.getSharedHeader(), false, false, false, DB::FormatSettings{}))
    {}

    String getName() const override { return "PrintTSVSink"; }

protected:
    void consume(Chunk chunk) override
    {
        Block block = input.getHeader().cloneWithColumns(chunk.getColumns());
        output_format->write(block);
    }

    void onFinish() override
    {
        output_format->finalize();
        out.finalize();
    }

private:
    WriteBufferFromFile out;
    OutputFormatPtr output_format;
};


class PrintTSVStep : public IQueryPlanStep
{
public:
    explicit PrintTSVStep(SharedHeader input_header_)
    {
        updateInputHeaders({input_header_});
    }

    [[nodiscard]] String getName() const override { return "PrintTSV"; }

    [[nodiscard]] bool hasOutputStream() const { return false; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & /*settings*/) override
    {
        auto & pipeline = *pipelines.front();
        Block stream_header = pipeline.getHeader();

        /// Single sink to print to stdout
        pipeline.resize(1);

        pipeline.setSinks([&](const SharedHeader & header, Pipe::StreamType stream_type) -> ProcessorPtr
        {
            chassert(stream_type == Pipe::StreamType::Main);
            return std::make_shared<PrintTSVSink>(header);
        });

        return std::move(pipelines.front());
    }

    void serialize(Serialization & /*ctx*/) const override {}

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx)
    {
        return std::make_unique<PrintTSVStep>(ctx.input_headers.front());
    }

private:
    void updateOutputHeader() override {}
};

static void registerPrintTSVStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("PrintTSV", PrintTSVStep::deserialize);
}


static SharedHeader prepareSourceFileInNativeFormat(const String & file_name, const String & data, size_t input_replicate_count)
{
    ReadBufferFromString read_buffer(data);

    Block header;

    {
        TabSeparatedSchemaReader schema_reader(read_buffer, true, true, false, FormatSettings{});
        auto schema = schema_reader.readSchema();

        for (const auto & [name, type] : schema)
            header.insert({type->createColumn(), type, name});
    }

    auto shared_header = std::make_shared<const Block>(std::move(header));

    {
        auto file_buffer = std::make_unique<WriteBufferFromFile>(file_name);
        NativeCompressedSink sink(shared_header, *file_buffer, file_name);

        auto reader = std::make_shared<TabSeparatedRowInputFormat>(
            shared_header, read_buffer, IRowInputFormat::Params{}, true, true, false, FormatSettings{});

        while (auto chunk = reader->read())
        {
            Block block = shared_header->cloneWithColumns(chunk.getColumns());

            /// Repeat the same block multiple times to make the file bigger
            for (size_t i = 0; i < input_replicate_count; ++i)
                sink.consume(Chunk(block.getColumns(), block.rows()));
        }
        sink.onFinish();
    }

    return shared_header;
}

static QueryPlanStepPtr createSourceStepFromFileInNativeFormat(SharedHeader header, const String & file_name)
{
    auto step = std::make_unique<ReadFromFileStep>(std::move(header), file_name);
    return step;
}

/// Simple plan that joins two tables
static QueryPlan createHashJoinQueryPlan(const String & data_a, const String & data_b)
{
    const String file_name_a = "/tmp/file_a";
    const String file_name_b = "/tmp/file_b";
//    const size_t num_shards_a = 3;
//    const size_t num_shards_b = 2;
    const size_t replicate_input_count = 2; // Replicate the same data many times just for testing

    SharedHeader header_a = prepareSourceFileInNativeFormat(file_name_a, data_a, replicate_input_count);
    SharedHeader header_b = prepareSourceFileInNativeFormat(file_name_b, data_b, replicate_input_count);

    /// Create source for table A
    QueryPlan left_plan;
    {
        left_plan.addStep(createSourceStepFromFileInNativeFormat(header_a, file_name_a));
    }

    /// Create source for table B
    QueryPlan right_plan;
    {
        right_plan.addStep(createSourceStepFromFileInNativeFormat(header_b, file_name_b));
    }

    /// Create join step
    QueryPlan query_plan;
    {
        auto remove_column_pointers = [](const ColumnsWithTypeAndName & header) -> ColumnsWithTypeAndName
        {
            ColumnsWithTypeAndName result = header;
            for (auto & element : result)
                element.column = nullptr;
            return result;
        };

        JoinExpressionActions join_expression_actions(
            remove_column_pointers(header_a->getColumnsWithTypeAndName()),
            remove_column_pointers(header_b->getColumnsWithTypeAndName()));

        JoinOperator join_info(JoinKind::Inner);
        /// Construct contidion "t1.c1 == t2.c1 AND t1.c2 == t2.c2"
        {
            auto actions_dag = join_expression_actions.getActionsDAG();
            actions_dag->getOutputs() = actions_dag->getInputs();

            join_info.expression.push_back(JoinActionRef::transform({
                JoinActionRef(actions_dag->tryFindInOutputs("t1.c1"), join_expression_actions),
                JoinActionRef(actions_dag->tryFindInOutputs("t2.c1"), join_expression_actions),
            }, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
            join_info.expression.push_back(JoinActionRef::transform({
                JoinActionRef(actions_dag->tryFindInOutputs("t1.c2"), join_expression_actions),
                JoinActionRef(actions_dag->tryFindInOutputs("t2.c2"), join_expression_actions),
            }, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));

        }

        NameSet required_output_columns = {"t1.c1", "t1.c2", "t1.va", "t2.vb"};
        ContextPtr query_context = getContext().context;

        auto join_settings = JoinSettings(query_context->getSettingsRef());
        join_settings.enable_lazy_columns_replication = false;

        auto join_step = std::make_unique<JoinStepLogical>(
            header_a,
            header_b,
            std::move(join_info),
            std::move(join_expression_actions),
            std::move(required_output_columns),
            std::unordered_map<String, const ActionsDAG::Node *>{},
            false,
            join_settings,
            SortingStep::Settings(query_context->getSettingsRef()));

        join_step->setStepDescription("Join");

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

        query_plan.unitePlans(std::move(join_step), {std::move(plans)});
    }

    /// Create sink
    query_plan.addStep(std::make_unique<PrintTSVStep>(query_plan.getCurrentHeader()));

    return query_plan;
}

struct DistributedQueryPlanSettings
{
    size_t num_buckets_for_shuffle = 5;
};

String data_a =
            "t1.c1\tt1.c2\tt1.va\n"
            "String\tUInt64\tString\n"
            "a\t1\t1ab\n"
            "g\t2\t2ba\n"
            "c\t1\t3abc\n"
            "a\t1\t4bad\n"
            "f\t1\t5abe\n"
            "a\t1\t6baf\n"
            "d\t1\t3abc\n"
            "e\t1\t4bad\n"
            "f\t1\t5abe\n"
            "a\t1\t6baf\n"
            "b\t2\t7bb\n"
            "g\t1\t71bb\n"
            "b\t2\t72bb\n"
            "b\t2\t73bb\n"
            "a\t2\t8bb\n"
            "c\t3\t9cc\n";

String data_b =
            "t2.c1\tt2.c2\tt2.vb\n"
            "String\tUInt64\tString\n"
            "a\t2\t1baaa\n"
            "c\t3\t2bddd\n"
            "b\t2\t31bbbb\n"
            "g\t2\t32bbbb\n"
            "a\t1\t4baaa\n"
            "c\t1\t5baab\n"
            "a\t1\t6baac\n"
            "d\t1\t5baab\n"
            "e\t1\t6baac\n"
            "f\t1\t5baab\n"
            "g\t1\t6baac\n"
            "a\t1\t7baad\n"
            "c\t3\t8bccc\n";

void registerObjectStorages();

} // namespace DB

using namespace DB;


namespace
{

std::string getConfig()
{
    std::string s = R"(
<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>

    <distributed_query>
        <temporary_files_storage>
            <type>local</type>
            <path>./local_object_storage/</path>
            <endpoint_subpath>distributed_query_temp_files/</endpoint_subpath>
        </temporary_files_storage>
    </distributed_query>

</clickhouse>
)";

    DB::WriteBufferFromFile f("./config_file_for_test.xml");
    DB::writeText(s, f);
    f.finalize();
    return "./config_file_for_test.xml";
}

}

void registerPlanSteps();

class DistributedQueryTest : public ::testing::Test
{
public:
    DistributedQueryTest()
    {
        previous_thread_status = current_thread;
        current_thread = nullptr;
    }

    ~DistributedQueryTest() override
    {
        current_thread = previous_thread_status;
    }

    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel(std::cerr));
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%Y-%m-%d %H:%M:%S.%i [ %I ] %T <%p> (%s) %t"));
        Poco::AutoPtr<Poco::FormattingChannel> channel(new Poco::FormattingChannel(formatter, console_channel));
        Poco::Logger::root().setChannel(channel);
        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            Poco::Logger::root().setLevel(test_log_level);
        else
            Poco::Logger::root().setLevel("none");

        DB::ServerUUID::setRandomForUnitTests();

        namespace fs = std::filesystem;
        if (fs::exists("./config_file_for_test.xml"))
            fs::remove_all("./config_file_for_test.xml");

        auto config_path = getConfig();
        DB::ConfigProcessor config_processor(config_path, true, true);
        config = config_processor.loadConfig(false);
        context_holder.context->setConfig(config.configuration);

        tryRegisterFunctions();
        registerObjectStorages();
        registerPlanSteps();

        GlobalThreadPool::instance();
    }

    void TearDown() override
    {
        DB::clearDiskRegistry();
    }

private:
    const ContextHolder & context_holder = getContext();
    DB::ConfigProcessor::LoadedConfig config;
    ThreadStatus* previous_thread_status = nullptr;
};

namespace DB
{

void registerShuffleSendStep(QueryPlanStepRegistry & registry);
void registerShuffleReceiveStep(QueryPlanStepRegistry & registry);
void registerGatherSendStep(QueryPlanStepRegistry & registry);
void registerGatherReceiveStep(QueryPlanStepRegistry & registry);
void registerJoinStep(QueryPlanStepRegistry & registry);
void registerFilterStep(QueryPlanStepRegistry & registry);
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);

}

void registerPlanSteps()
{
    /// The registry is process-wide and rejects a second registration of the same step, so register
    /// once for the whole binary instead of per test.
    static std::once_flag registered;
    std::call_once(registered, []
    {
        QueryPlanStepRegistry & registry = QueryPlanStepRegistry::instance();

        registerReadFromFileStep(registry);
        registerShuffleSendStep(registry);
        registerShuffleReceiveStep(registry);
        registerJoinStep(registry);
        registerGatherSendStep(registry);
        registerGatherReceiveStep(registry);
        registerPrintTSVStep(registry);
        registerFilterStep(registry);
        registerBuildRuntimeFilterStep(registry);
    });
}


namespace DB
{
namespace QueryPlanOptimizations
{

DistributedQueryPlan makeDistributedPlan(QueryPlan::Nodes nodes, QueryPlan::Node * root, const QueryPlanOptimizationSettings & optimization_settings);

}
}

static void executeTestWithExchangeKind(const String & exchangeKind)
try
{
    DistributedQueryPlan distributed_query_plan;

    const char * env_val = std::getenv("DISTRIBUTED_PLAN_SINGLE_STAGE"); // NOLINT(concurrency-mt-unsafe)
    bool distributed_plan_single_stage = env_val && std::string(env_val) != "0";

    ThreadStatus thread_status;
    auto session = std::make_unique<Session>(getContext().context, ClientInfo::Interface::TCP_INTERSERVER);
    auto query_context = session->makeQueryContext();
    auto thread_group = ThreadGroup::createForQuery(query_context);
    CurrentThread::attachToGroup(thread_group);

    query_context->setSetting("distributed_plan_force_exchange_kind", exchangeKind);

    /// Each task runs its own pipeline; 4 is the shuffle bucket count the other pipelines use.
    query_context->setSetting("max_threads", 4);

    {
        /// Create JOIN query plan
        auto query_plan = createHashJoinQueryPlan(data_a, data_b);

        /// Optimize query plan for distributed execution
        QueryPlanOptimizationSettings optimization_settings(query_context);
        optimization_settings.make_distributed_plan = true;
        optimization_settings.distributed_plan_default_shuffle_join_bucket_count = 4;
        optimization_settings.distributed_plan_single_stage = distributed_plan_single_stage;  /// For debugging
        query_plan.optimize(optimization_settings);

        auto * root = query_plan.getRootNode();
        auto plan_internals = QueryPlan::detachNodesAndResources(std::move(query_plan));

        distributed_query_plan = QueryPlanOptimizations::makeDistributedPlan(std::move(plan_internals.first), root, optimization_settings);
    }

    Strings all_temporary_files_for_cleanup;
    for (const auto & stage : distributed_query_plan.stages)
    {
        for (const auto & task : stage.second.tasks)
        {
            for (const auto & stream_id : task.output_exchange_streams)
            {
                if (distributed_query_plan.exchange_descriptions.at(stream_id.exchange_id).kind == ExchangeDescription::Kind::Persisted)
                    all_temporary_files_for_cleanup.push_back(stream_id.toString());
            }
        }
    }

    const UUID query_uuid = UUIDHelpers::generateV4();
    auto [object_storage, path] = getObjectStorageForTemporaryFiles(toString(query_uuid), query_context);
    auto cleanup = makeTemporaryFilesCleaner(object_storage, path, all_temporary_files_for_cleanup);

    query_context->setSetting("distributed_plan_execute_locally", 1);
    auto cancellation = std::make_shared<DistributedQueryCancellation>();

    /// Just execute the distributed query plan without checking the result
    auto executor = createDistributedQueryExecutor(
        query_uuid, distributed_query_plan, nullptr, query_context, cancellation, std::make_shared<WakeupFd>());

    try
    {
        executor->start();
        while (!executor->execute(/*poll_timeout_ms=*/ 100));
        executor->cleanup();
    }
    catch (...)
    {
        executor->cleanup();
        throw;
    }
}
catch (Exception & e)
{
    std::cout << e.getStackTraceString() << std::endl;
    throw;
}

TEST_F(DistributedQueryTest, ShuffleHashJoin)
{
    executeTestWithExchangeKind("Persisted");
    executeTestWithExchangeKind("Streaming");
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INVALID_CONFIG_PARAMETER;
}
}

/// Planning must size a query's distributed plan from the stateless-worker cluster it selects via
/// `distributed_plan_worker_cluster`, and reject an unusable selection instead of silently planning
/// zero workers. Covers default/named selection, the named default (`default_cluster_name`), and the
/// no-default (symmetric) config where a query must name its cluster.
TEST_F(DistributedQueryTest, StatelessWorkerClusterPlanSizing)
{
    auto apply_config = [&](const std::string & sw_block)
    {
        const std::string xml = "<clickhouse>\n"
            "<remote_servers><two><shard>"
            "<replica><host>pair-a</host><port>9000</port></replica>"
            "<replica><host>pair-b</host><port>9000</port></replica>"
            "</shard></two></remote_servers>\n" + sw_block + "</clickhouse>\n";
        DB::WriteBufferFromFile f("./sw_cluster_config_for_test.xml");
        DB::writeText(xml, f);
        f.finalize();
        DB::ConfigProcessor processor("./sw_cluster_config_for_test.xml", true, true);
        auto loaded = processor.loadConfig(false);
        getContext().context->setConfig(loaded.configuration);
        getContext().context->setClustersConfig(loaded.configuration);
        /// Planning resolves providers through the process-global registry; keep its cached providers
        /// in step with each sub-config.
        getContext().context->reloadStatelessWorkersConfig(getContext().context->getConfigRef());
    };

    auto make_query_context = [&](const String & cluster, UInt64 workers_num)
    {
        auto context = Context::createCopy(getContext().context);
        context->setSetting("distributed_plan_workers_num", workers_num);
        context->setSetting("distributed_plan_worker_cluster", cluster);
        context->setQueryContext(context);
        return context;
    };

    auto plan_nodes = [&](const String & cluster, UInt64 workers_num) -> size_t
    {
        return getCascadesPlanningNodeCount(make_query_context(cluster, workers_num));
    };

    auto expect_rejected = [&](const String & cluster, int expected_code)
    {
        try
        {
            getCascadesPlanningNodeCount(make_query_context(cluster, 5));
            FAIL() << "expected planning to reject cluster '" << cluster << "'";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), expected_code);
        }
    };

    /// A static cluster plans for its configured workers, regardless of the requested count.
    apply_config(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <host>default-host</host>
        <clusters>
            <stat><host>stat-host</host></stat>
            <pair><cluster>two</cluster></pair>
            <sourceless><enabled>1</enabled></sourceless>
        </clusters>
    </stateless_worker_client>
)");
    EXPECT_EQ(plan_nodes("", 5), 1u);
    EXPECT_EQ(plan_nodes("stat", 5), 1u);
    EXPECT_EQ(plan_nodes("pair", 5), 2u);
    expect_rejected("nope", DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG);       /// unknown cluster
    expect_rejected("sourceless", DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG); /// configured but names no worker source

    /// Planning resolves the cluster from the query-level context, not the ambient (subquery) one, so a
    /// copied context overriding `distributed_plan_worker_cluster` cannot plan a different topology
    /// than execution allocates from.
    {
        auto query_ctx = make_query_context("pair", 5);
        auto ambient_ctx = Context::createCopy(query_ctx);
        ambient_ctx->setSetting("distributed_plan_worker_cluster", String("stat"));
        EXPECT_EQ(getCascadesPlanningNodeCount(ambient_ctx), 2u);
    }

    /// A named default: the flat block is reachable both by an empty value and by its `default_cluster_name`.
    apply_config(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <default_cluster_name>main</default_cluster_name>
        <cluster>two</cluster>
    </stateless_worker_client>
)");
    EXPECT_EQ(plan_nodes("", 5), 2u);
    EXPECT_EQ(plan_nodes("main", 5), 2u);

    /// No default (symmetric config): the flat block names no worker source, only `clusters.<name>`.
    /// An empty selection is rejected; a named cluster still resolves.
    apply_config(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <clusters>
            <stat><host>stat-host</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    expect_rejected("", DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    EXPECT_EQ(plan_nodes("stat", 5), 1u);

    /// Client disabled: the default (empty) selection plans locally (0 nodes), but an explicitly named
    /// cluster is rejected rather than silently planning local.
    apply_config(R"(
    <stateless_worker_client>
        <enabled>0</enabled>
        <clusters>
            <stat><host>stat-host</host></stat>
        </clusters>
    </stateless_worker_client>
)");
    EXPECT_EQ(plan_nodes("", 5), 0u);
    expect_rejected("stat", DB::ErrorCodes::SUPPORT_IS_DISABLED);

    /// A `default_cluster_name` that also names a cluster under `clusters` is an ambiguous collision:
    /// reject it (otherwise the named cluster is unreachable and the query silently uses the default).
    apply_config(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <default_cluster_name>blue</default_cluster_name>
        <host>default-host</host>
        <clusters>
            <blue><host>blue-host</host></blue>
        </clusters>
    </stateless_worker_client>
)");
    expect_rejected("blue", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
    expect_rejected("", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);

    /// A cluster must name exactly one worker source; multiple sources are an ambiguous config and are
    /// rejected rather than silently resolved by precedence.
    apply_config(R"(
    <stateless_worker_client>
        <enabled>1</enabled>
        <cluster>two</cluster>
        <host>stray-host</host>
        <clusters>
            <multi><cluster>two</cluster><host>some-host</host></multi>
        </clusters>
    </stateless_worker_client>
)");
    expect_rejected("multi", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
    expect_rejected("", DB::ErrorCodes::INVALID_CONFIG_PARAMETER);
}

/// A stream with no columns (case of `SELECT count()`) carries only row counts, so its chunks are
/// told apart from the end-of-data marker by the row count alone.
TEST_F(DistributedQueryTest, InMemoryExchangeStreamWithoutColumns)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("distributed_plan_execute_locally", true);

    /// In-memory exchanges are kept per query id for the whole process, so a fixed id would make a
    /// repeated run read the exchange the previous run already used.
    auto exchange_lookup = createExchangeLookup(
        toString(UUIDHelpers::generateV4()), ExchangeDescriptions{}, ExchangeStreamSources{}, /*temporary_files_=*/ nullptr, context,
        /*execute_locally=*/true, /*cancellation=*/ nullptr);

    auto header = std::make_shared<const Block>();
    const ExchangeStreamId stream_id("test_exchange", 0, 0);

    Chunks chunks;
    chunks.emplace_back(Columns{}, 3);
    chunks.emplace_back(Columns{}, 4);

    {
        QueryPipelineBuilder builder;
        builder.init(Pipe(std::make_shared<SourceFromChunks>(header, std::move(chunks))));
        builder.setSinks([&](const SharedHeader & sink_header, Pipe::StreamType) -> ProcessorPtr
        {
            return exchange_lookup->createSink(sink_header, stream_id, /*advisory*/ false);
        });
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
        CompletedPipelineExecutor executor(pipeline);
        executor.execute();
    }

    size_t total_rows = 0;
    {
        QueryPipeline pipeline(
            Pipe(exchange_lookup->createSource(header, stream_id, /*output_is_serialized=*/ false, /*advisory=*/ false)));
        PullingPipelineExecutor executor(pipeline);
        Chunk chunk;
        while (executor.pull(chunk))
            total_rows += chunk.getNumRows();
    }

    EXPECT_EQ(total_rows, 7u);
}

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{

RuntimeFilterReceiveDescriptor makeDescriptor(const String & key, const DataTypePtr & type, const ExchangeStreamId & stream)
{
    RuntimeFilterReceiveDescriptor descriptor;
    descriptor.filter_key = key;
    descriptor.filter_name = "filter_" + key;
    descriptor.key_column_type = type;
    descriptor.geometry = RuntimeFilterGeometry{
        .exact_values_limit = 100,
        .exact_bytes_limit = 4096,
        .bloom_filter_bytes = 2048,
        .bloom_filter_hash_functions = 4,
        .pass_ratio_threshold_for_disabling = 0.55,
        .blocks_to_skip_before_reenabling = 7,
        .max_ratio_of_set_bits_in_bloom_filter = 0.4,
    };
    descriptor.stream = stream;
    return descriptor;
}

String serializeTaskWithDescriptors(std::vector<RuntimeFilterReceiveDescriptor> descriptors)
{
    DistributedQueryTaskDescription description;
    description.serialization_version = DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_RUNTIME_FILTERS;
    description.initial_query_id = "q1";
    description.task.task_id = "t0";
    description.task.runtime_filter_descriptors = std::move(descriptors);
    WriteBufferFromOwnString out;
    serializeTask(description, out);
    return out.str();
}

}

/// The restored task serializes to the same bytes, so no field of a descriptor is lost on the way.
TEST(DistributedTaskSerialization, RuntimeFilterDescriptorsRoundTrip)
{
    auto first = makeDescriptor("key_a", std::make_shared<DataTypeUInt64>(), ExchangeStreamId("ex1", "1", "0"));
    auto second = makeDescriptor("key_b", std::make_shared<DataTypeString>(), ExchangeStreamId("ex2", "0", "1"));
    second.geometry.pass_ratio_threshold_for_disabling = 0.25;
    second.geometry.max_ratio_of_set_bits_in_bloom_filter = 0.15;
    const String bytes = serializeTaskWithDescriptors({first, second});

    ReadBufferFromString in(bytes);
    DistributedQueryTaskDescription restored;
    deserializeTask(restored, in);

    ASSERT_EQ(restored.task.runtime_filter_descriptors.size(), 2u);
    EXPECT_EQ(restored.task.runtime_filter_descriptors[1].filter_key, "key_b");
    EXPECT_EQ(serializeTaskWithDescriptors(restored.task.runtime_filter_descriptors), bytes);
}

/// `deserializeTask` rejects descriptors that a correct initiator never sends.
TEST(DistributedTaskSerialization, InvalidRuntimeFilterDescriptorsAreRejected)
{
    auto valid = makeDescriptor("key_a", std::make_shared<DataTypeUInt64>(), ExchangeStreamId("ex", "0", "0"));
    auto without_key = valid;
    without_key.filter_key.clear();

    const std::vector<std::pair<const char *, std::vector<RuntimeFilterReceiveDescriptor>>> cases = {
        {"an empty filter key", {without_key}},
        {"a duplicate filter key", {valid, valid}},
    };
    for (const auto & [what, descriptors] : cases)
    {
        const String bytes = serializeTaskWithDescriptors(descriptors);
        ReadBufferFromString in(bytes);
        DistributedQueryTaskDescription restored;
        try
        {
            deserializeTask(restored, in);
            ADD_FAILURE() << what << " was accepted";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA) << what;
        }
    }
}
