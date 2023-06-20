#pragma once

#include <memory>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <QueryCoordination/ExchangeDataStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryCoordination/DataSink.h>
#include <QueryCoordination/DataPartition.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

using FragmentID = Int32;
using PlanID = Int32;

namespace JSONBuilder
{
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}
/**
 * AggregatingInOrderTransform (DB)
AggregatingTransform (DB)
ConcatProcessor (DB)
ConvertingAggregatedToChunksTransform (DB)
CopyingDataToViewsTransform (DB)
CopyTransform (DB)
DelayedJoinedBlocksTransform (DB)
DelayedJoinedBlocksWorkerTransform (DB)
DelayedPortsProcessor (DB)
DelayedSource (DB)
ExceptionKeepingTransform (DB)
    CheckConstraintsTransform (DB)
    ConvertingTransform (DB)
    CountingTransform (DB)
    ExecutingInnerQueryFromViewTransform (DB)
    SinkToStorage (DB)
        BufferSink (DB)
        DistributedSink (DB)
        EmbeddedRocksDBSink (DB)
        LiveViewSink (DB)
        LogSink (DB)
        MemorySink (DB)
        MergeTreeSink (DB)
        MessageQueueSink (DB)
        NullSinkToStorage (DB)
        PartitionedSink (DB)
            PartitionedStorageFileSink (DB)
            PartitionedStorageS3Sink (DB)
            PartitionedStorageURLSink (DB)
        PostgreSQLSink (DB)
        PushingToLiveViewSink (DB)
        PushingToWindowViewSink (DB)
        RemoteSink (DB)
        ReplicatedMergeTreeSinkImpl (DB)
        SetOrJoinSink (DB)
        SinkMeiliSearch (DB)
        SQLiteSink (DB)
        StorageFileSink (DB)
        StorageKeeperMapSink (DB)
        StorageMongoDBSink (DB)
        StorageS3Sink (DB)
        StorageURLSink (DB)
        StripeLogSink (DB)
        ZooKeeperSink (DB)
    SquashingChunksTransform (DB)
FillingRightJoinSideTransform (DB)
FinalizingViewsTransform (DB)
FlattenChunksToMergeTransform (DB)
ForkProcessor (DB)
GroupingAggregatedTransform (DB)
IAccumulatingTransform (DB)
    BufferingToFileTransform (DB)
    CreatingSetsTransform (DB)
    GroupByModifierTransform (DB)
        CubeTransform (DB)
        RollupTransform (DB)
    MergingAggregatedTransform (DB)
    QueueBuffer (DB)
    TTLCalcTransform (DB)
    TTLTransform (DB)
IInflatingTransform (DB)
IMergingTransformBase (DB)
    IMergingTransform (DB)
IntersectOrExceptTransform (DB)
IOutputFormat (DB)
    ArrowBlockOutputFormat (DB)
    IRowOutputFormat (DB)
        AvroRowOutputFormat (DB)
        BinaryRowOutputFormat (DB)
        BSONEachRowRowOutputFormat (DB)
        CapnProtoRowOutputFormat (DB)
        CSVRowOutputFormat (DB)
        CustomSeparatedRowOutputFormat (DB)
        MarkdownRowOutputFormat (DB)
        MsgPackRowOutputFormat (DB)
        PrometheusTextOutputFormat (DB)
        ProtobufListOutputFormat (DB)
        ProtobufRowOutputFormat (DB)
        RawBLOBRowOutputFormat (DB)
        SQLInsertRowOutputFormat (DB)
        TabSeparatedRowOutputFormat (DB)
        ValuesRowOutputFormat (DB)
        VerticalRowOutputFormat (DB)
    LazyOutputFormat (DB)
    MySQLOutputFormat (DB)
    NativeOutputFormat (DB)
    NullOutputFormat (DB)
    ODBCDriver2BlockOutputFormat (DB)
    ORCBlockOutputFormat (DB)
    ParallelFormattingOutputFormat (DB)
    ParquetBlockOutputFormat (DB)
    PostgreSQLOutputFormat (DB)
    PrettyBlockOutputFormat (DB)
        PrettyCompactBlockOutputFormat (DB)
        PrettySpaceBlockOutputFormat (DB)
    PullingOutputFormat (DB)
    TemplateBlockOutputFormat (DB)
ISimpleTransform (DB)
    AddingAggregatedChunkInfoTransform (DB)
    AddingDefaultsTransform (DB)
    ArrayJoinTransform (DB)
    CheckSortedTransform (DB)
    CreatingSetsOnTheFlyTransform (DB)
    DistinctSortedChunkTransform (DB)
    DistinctSortedTransform (DB)
    DistinctTransform (DB)
    ExpressionTransform (DB)
    ExtremesTransform (DB)
    FillingNoopTransform (DB)
    FillingTransform (DB)
    FilterBySetOnTheFlyTransform (DB)
    FilterSortedStreamByRange (DB)
    FilterTransform (DB)
    FinalizeAggregatedTransform (DB)
    LimitByTransform (DB)
    LimitsCheckingTransform (DB)
    MaterializingTransform (DB)
    MergingAggregatedBucketTransform (DB)
    PartialSortingTransform (DB)
    ReverseTransform (DB)
    SendingChunkHeaderTransform (DB)
    SimpleSquashingChunksTransform (DB)
    StreamInQueryCacheTransform (DB)
    TotalsHavingTransform (DB)
    TransformWithAdditionalColumns (DB)
    WatermarkTransform (DB)
ISink (DB)
    DataSink (DB)
    EmptySink (DB)
    ExternalTableDataSink (DB)
    NullSink (DB)
ISource (DB)
    BlocksListSource (DB)
    BlocksSource (DB)
    BufferSource (DB)
    CassandraSource (DB)
    ColumnsSource (DB)
    ConvertingAggregatedToChunksSource (DB)
    ConvertingAggregatedToChunksWithMergingSource (DB)
    DataSkippingIndicesSource (DB)
    DDLQueryStatusSource (DB)
    DetachedPartsSource (DB)
    DictionarySource (DB)
    DistributedAsyncInsertSource (DB)
    EmbeddedRocksDBSource (DB)
    ExchangeDataReceiver (DB)
    ExchangeDataReceiver (DB)
    GenerateSource (DB)
    IInputFormat (DB)
    JoinSource (DB)
    KafkaSource (DB)
    LiveViewEventsSource (DB)
    LiveViewSource (DB)
    LogSource (DB)
    MeiliSearchSource (DB)
    MemorySource (DB)
    MergeSorterSource (DB)
    MergeTreeSequentialSource (DB)
    MergeTreeSource (DB)
    MongoDBSource (DB)
    MySQLSource (DB)
    NATSSource (DB)
    NullSource (DB)
    NumbersMultiThreadedSource (DB)
    NumbersSource (DB)
    PostgreSQLSource (DB)
    PushingAsyncSource (DB)
    PushingSource (DB)
    RabbitMQSource (DB)
    RedisSource (DB)
    RemoteExtremesSource (DB)
    RemoteSource (DB)
    RemoteTotalsSource (DB)
    ShellCommandSource (DB)
    SourceFromChunks (DB)
    SourceFromChunks (DB)
    SourceFromNativeStream (DB)
    SourceFromQueryPipeline (DB)
    SourceFromSingleChunk (DB)
    SQLiteSource (DB)
    StorageFileSource (DB)
    StorageInputSource (DB)
    StorageKeeperMapSource (DB)
    StorageS3Source (DB)
    StorageURLSource (DB)
    StripeLogSource (DB)
    SyncKillQuerySource (DB)
    TablesBlockSource (DB)
    TemporaryFileLazySource (DB)
    ThrowingExceptionSource (DB)
    WaitForAsyncInsertSource (DB)
    WindowViewSource (DB)
    ZerosSource (DB)
ISource (DB)
    BlocksListSource (DB)
    BlocksSource (DB)
    BufferSource (DB)
    CassandraSource (DB)
    ColumnsSource (DB)
    ConvertingAggregatedToChunksSource (DB)
    ConvertingAggregatedToChunksWithMergingSource (DB)
    DataSkippingIndicesSource (DB)
    DDLQueryStatusSource (DB)
    DetachedPartsSource (DB)
    DictionarySource (DB)
    DistributedAsyncInsertSource (DB)
    EmbeddedRocksDBSource (DB)
    ExchangeDataReceiver (DB)
    ExchangeDataReceiver (DB)
    GenerateSource (DB)
    IInputFormat (DB)
    JoinSource (DB)
    KafkaSource (DB)
    LiveViewEventsSource (DB)
    LiveViewSource (DB)
    LogSource (DB)
    MeiliSearchSource (DB)
    MemorySource (DB)
    MergeSorterSource (DB)
    MergeTreeSequentialSource (DB)
    MergeTreeSource (DB)
    MongoDBSource (DB)
    MySQLSource (DB)
    NATSSource (DB)
    NullSource (DB)
    NumbersMultiThreadedSource (DB)
    NumbersSource (DB)
    PostgreSQLSource (DB)
    PushingAsyncSource (DB)
    PushingSource (DB)
    RabbitMQSource (DB)
    RedisSource (DB)
    RemoteExtremesSource (DB)
    RemoteSource (DB)
    RemoteTotalsSource (DB)
    ShellCommandSource (DB)
    SourceFromChunks (DB)
    SourceFromChunks (DB)
    SourceFromNativeStream (DB)
    SourceFromQueryPipeline (DB)
    SourceFromSingleChunk (DB)
    SQLiteSource (DB)
    StorageFileSource (DB)
    StorageInputSource (DB)
    StorageKeeperMapSource (DB)
    StorageS3Source (DB)
    StorageURLSource (DB)
    StripeLogSource (DB)
    SyncKillQuerySource (DB)
    TablesBlockSource (DB)
    TemporaryFileLazySource (DB)
    ThrowingExceptionSource (DB)
    WaitForAsyncInsertSource (DB)
    WindowViewSource (DB)
    ZerosSource (DB)
JoiningTransform (DB)
LimitTransform (DB)
OffsetTransform (DB)
PingPongProcessor (DB)
    ReadHeadBalancedProcessor (DB)
ResizeProcessor (DB)
SortingAggregatedForMemoryBoundMergingTransform (DB)
SortingAggregatedTransform (DB)
SortingTransform (DB)
    FinishSortingTransform (DB)
    MergeSortingTransform (DB)
StrictResizeProcessor (DB)
WindowTransform (DB)

 */
class PlanFragment : public std::enable_shared_from_this<PlanFragment>
{
public:
    using Node = QueryPlan::Node;

    explicit PlanFragment(ContextMutablePtr & context_, DataPartition & partition)
        : context(context_), data_partition(partition)
    {
    }


    struct ExplainPlanOptions
    {
        /// Add output header to step.
        bool header = false;
        /// Add description of step.
        bool description = true;
        /// Add detailed information about step actions.
        bool actions = false;
        /// Add information about indexes actions.
        bool indexes = false;
        /// Add information about sorting
        bool sorting = false;
    };

    struct ExplainPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    /**
     * Assigns 'this' as fragment of all PlanNodes in the plan tree rooted at node.
     * Does not traverse the children of ExchangeNodes because those must belong to a
     * different fragment.
     */
    void setFragmentInPlanTree(Node * node)
    {
        if (!node || !node->step)
        {
            return;
        }
        node->fragment = shared_from_this();

        if (dynamic_cast<ExchangeDataStep *>(node->step.get()))
        {
            return;
        }

        for (Node * child : node->children)
        {
            setFragmentInPlanTree(child);
        }
    }

    PlanFragmentPtr getDestFragment() const
    {
        if (!dest_node || !dest_node->step)
        {
            return nullptr;
        }
        return dest_node->fragment;
    }

    PlanID getDestExchangeID() const
    {
        if (!dest_node || !dest_node->step)
        {
            return std::numeric_limits<uint32_t>::max();
        }
        return dest_node->plan_id;
    }

    void addChild(PlanFragmentPtr fragment)
    {
        children.emplace_back(fragment);
    }

    PlanFragmentPtrs getChildren()
    {
        return children;
    }

    void setDestination(Node * node)
    {
        dest_node = node;
        PlanFragmentPtr dest = getDestFragment();
        dest->addChild(shared_from_this());
    }

    Node * getRootNode() const { return root; }

    const DataPartition & getDataPartition() const { return data_partition; }

    bool isPartitioned() const { return data_partition.type != PartitionType::UNPARTITIONED; }

    void dump(WriteBufferFromOwnString & buffer)
    {
        PlanFragment::ExplainPlanOptions settings;
        buffer.write('\n');
        std::string str("Fragment " + std::to_string(fragment_id));
        buffer.write(str.c_str(), str.size());
        buffer.write('\n');

        explainPlan(buffer, settings);

        for (const auto & child_fragment : children)
        {
            child_fragment->dump(buffer);
        }
    }

    std::shared_ptr<Cluster> getCluster() const { return cluster; }

    void setCluster(std::shared_ptr<Cluster> cluster_) { cluster = cluster_; }

    void setFragmentId(UInt32 id) { fragment_id = id; }

    Int32 getFragmentId() const { return fragment_id; }

    QueryPipeline buildQueryPipeline(std::vector<DataSink::Channel> & channels, const String & local_host);

    void unitePlanFragments(QueryPlanStepPtr step, std::vector<std::shared_ptr<PlanFragment>> fragments, StorageLimitsList storage_limits_ = {});
    void addStep(QueryPlanStepPtr step);

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings,
        const BuildQueryPipelineSettings & build_pipeline_settings);

    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options);
    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options);
    void explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options);
    void explainEstimate(MutableColumns & columns);

    /// Do not allow to change the table while the pipeline alive.
    void addTableLock(TableLockHolder lock) { resources.table_locks.emplace_back(std::move(lock)); }
    void addInterpreterContext(std::shared_ptr<const Context> context_) { resources.interpreter_context.emplace_back(std::move(context_)); }
    void addStorageHolder(StoragePtr storage) { resources.storage_holders.emplace_back(std::move(storage)); }

    void addResources(QueryPlanResourceHolder resources_) { resources = std::move(resources_); }

    /// Set upper limit for the recommend number of threads. Will be applied to the newly-created pipelines.
    /// TODO: make it in a better way.
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }
    size_t getMaxThreads() const { return max_threads; }

    void setOutputPartition(DataPartition output_partition_)
    {
        output_partition = output_partition_;
    }

    using Nodes = std::list<Node>;

    const Nodes & getNodes() const { return nodes; }
private:

    Node makeNewNode(QueryPlanStepPtr step, std::vector<PlanNode *> children_ = {})
    {
        Node node;
        node.step = step;
        node.fragment = shared_from_this();
        node.plan_id = ++plan_id_counter;
        node.children = children_;
        return node;
    }

    QueryPlanResourceHolder resources;
    Nodes nodes;
    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;

    ContextMutablePtr context;

    // id for this plan fragment
    FragmentID fragment_id;

    // exchange node to which this fragment sends its output
    Node * dest_node = nullptr;

    PlanFragmentPtrs children;

    DataPartition data_partition;

    DataPartition output_partition;

    // if null, outputs the entire row produced by planRoot
    // ArrayList<Expr> outputExprs;

    // If the fragment has a scanstep, it is scheduled according to the cluster copy fragment,
    // otherwise it is scheduled to the cluster node according to the DataPartition, the principle of minimum data movement.
    std::shared_ptr<Cluster> cluster;

    PlanID plan_id_counter = 0;
};

using PlanFragmentPtr = std::shared_ptr<PlanFragment>;
using PlanFragmentPtrs = std::vector<PlanFragmentPtr>;

}
