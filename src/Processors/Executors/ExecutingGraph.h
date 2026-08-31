#pragma once

#include <Processors/Port.h>
#include <Processors/IProcessor.h>
#include <Common/SharedMutex.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include <boost/container/devector.hpp>

namespace DB
{

/// Graph of executing pipeline.
class ExecutingGraph
{
public:
    struct Node;

    /// Edge represents connection between OutputPort and InputPort.
    /// For every connection, two edges are created: direct and backward (it is specified by backward flag).
    struct Edge
    {
        Edge(Node * to_, bool backward_,
             InputPort * input_port_, OutputPort * output_port_,
             std::vector<void *> * update_list)
            : to(to_), backward(backward_)
            , input_port(input_port_), output_port(output_port_)
        {
            update_info.update_list = update_list;
            update_info.id = this;
        }

        /// Node this edge points to.
        /// It is the node with output_port for direct edge or the node with input_port for backward edge.
        Node * to = nullptr;
        bool backward;
        /// Ports of this connection. Both pointers are set regardless of direction:
        /// for a direct edge (out -> in) input_port belongs to `to`; for a back edge the reverse.
        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;

        /// Edge version is increased when port's state is changed (e.g. when data is pushed). See Port.h for details.
        /// To compare version with prev_version we can decide if neighbour processor need to be prepared.
        Port::UpdateInfo update_info;
    };

    /// Use std::list because new ports can be added to processor during execution.
    using Edges = std::list<Edge>;

    /// Status for processor.
    /// Can be owning or not. Owning means that executor who set this status can change node's data and nobody else can.
    enum class ExecStatus : uint8_t
    {
        Idle,  /// prepare returned NeedData or PortFull. Non-owning.
        Preparing,  /// some executor is preparing processor, or processor is in task_queue. Owning.
        Executing,  /// prepare returned Ready and task is executing. Owning.
        Finished,  /// prepare returned Finished. Non-owning.
        Async  /// prepare returned Async. Owning.
    };

    /// Forward decl so Node can hold an iterator into the owning Nodes list.
    using Nodes = std::list<struct Node>;

    /// Graph node. Represents single Processor.
    struct Node
    {
        /// Iterator into the graph's processors list.
        Processors::iterator processor_iter{};

        /// Iterator into the graph's nodes list pointing at this node.
        Nodes::iterator self_iter{};

        /// Stable, monotonically-assigned id. Never reused. Used only for diagnostics (logs, system.processors_profile_log).
        uint64_t processors_id = 0;

        IProcessor * processor() const { return processor_iter->get(); }

        /// Direct edges are for output ports, back edges are for input ports.
        Edges direct_edges;
        Edges back_edges;

        /// Current status. It is accessed concurrently, using mutex.
        ExecStatus status = ExecStatus::Idle;
        std::mutex status_mutex;

        /// Exception which happened after processor execution.
        std::exception_ptr exception;

        /// Last state for profiling.
        std::optional<IProcessor::Status> last_processor_status;

        /// Ports which have changed their state since last processor->prepare() call.
        /// They changed when neighbour processors interact with connected ports.
        /// They will be used as arguments for next processor->prepare() (and will be cleaned after that).
        IProcessor::UpdatedInputPorts updated_input_ports;
        IProcessor::UpdatedOutputPorts updated_output_ports;

        /// Ports that have changed their state during last processor->prepare() call.
        /// We use this data to fill updated_input_ports and updated_output_ports for neighbour nodes.
        /// This containers are temporary, and used only after processor->prepare() is called.
        /// They could have been local variables, but we need persistent storage for Port::UpdateInfo.
        Port::UpdateInfo::UpdateList post_updated_input_ports;
        Port::UpdateInfo::UpdateList post_updated_output_ports;

        /// Counters for profiling.
        uint64_t num_executed_jobs = 0;
        uint64_t execution_time_ns = 0;
        uint64_t preparation_time_ns = 0;

        Node(Processors::iterator processor_iter_, uint64_t processors_id_)
            : processor_iter(processor_iter_), processors_id(processors_id_)
        {
        }
    };

    /// This queue can grow a lot and lead to OOM. That is why we use non-default
    /// allocator for container which throws exceptions in operator new
    using DequeWithMemoryTracker = boost::container::devector<ExecutingGraph::Node *, AllocatorWithMemoryTracking<ExecutingGraph::Node *>>;
    using Queue = std::queue<ExecutingGraph::Node *, DequeWithMemoryTracker>;

    /// All graph nodes. Nodes type is forward-declared above so Node can hold a self-iterator.
    Nodes nodes;

    /// Each processor is directly tied to pipeline graph node.
    using ProcessorsMap = std::unordered_map<const IProcessor *, Node *>;
    ProcessorsMap processors_map;

    explicit ExecutingGraph(std::shared_ptr<Processors> processors_, bool profile_processors_);

    const Processors & getProcessors() const { return *processors; }

    /// Traverse graph the first time to update all the childless nodes.
    void initializeExecution(Queue & queue, Queue & async_queue);

    enum class UpdateNodeStatus
    {
        Done,
        Exception,
        Cancelled,
    };

    /// Update processor at `start_node` (call IProcessor::prepare).
    /// Check parents and children of current processor and push them to stacks if they also need to be updated.
    /// If processor wants to be expanded, lock will be upgraded to get write access to pipeline.
    UpdateNodeStatus updateNode(Node * start_node, Queue & queue, Queue & async_queue);

    /// Cancel every processor with the given reason.
    void cancel(IProcessor::CancelReason reason);

private:
    /// Append a processor to the graph's processors list, create its Node, assign a stable id,
    /// register it in the processors map. Does not create edges — that is done separately by addEdges.
    Node & addNode(ProcessorPtr processor);
    Node & addNode(Processors::iterator processor_iter);
    Node * removeNode(ProcessorPtr processor);

    /// Add single edge to edges list. Check processor is known.
    Edge & addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to);

    /// Edges newly appended for a node by a call to addEdges.
    struct NewEdges
    {
        std::vector<Edge *> back;    // back edges added (inputs side of this node)
        std::vector<Edge *> direct;  // direct edges added (outputs side of this node)
        bool empty() const { return back.empty() && direct.empty(); }
    };
    NewEdges addEdges(Node & node);
    bool removeAffectedEdges(Node & node, const std::unordered_set<Node *> & removed_nodes);

    /// Update graph after processor `node` returned UpdatePipeline status.
    /// All new nodes and nodes with updated ports are pushed into stack.
    UpdateNodeStatus updatePipeline(boost::container::devector<Node *> & stack, Node & node, Processors & delayed_destruction);

    /// Shared with QueryPipeline.
    std::shared_ptr<Processors> processors;
    std::mutex processors_mutex;

    /// Monotonic counter for assigning Node::processors_id.
    uint64_t next_node_id = 0;

    SharedMutex nodes_mutex;

    const bool profile_processors;
    IProcessor::CancelReason cancel_reason = IProcessor::CancelReason::NotCancelled;
};

}
