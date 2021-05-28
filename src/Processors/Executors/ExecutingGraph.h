#pragma once
#include <Processors/Port.h>
#include <Processors/IProcessor.h>
#include <mutex>

namespace DB
{

/// Graph of executing pipeline.
class ExecutingGraph
{
public:
    /// Edge represents connection between OutputPort and InputPort.
    /// For every connection, two edges are created: direct and backward (it is specified by backward flag).
    struct Edge
    {
        Edge(uint64_t to_, bool backward_,
             uint64_t input_port_number_, uint64_t output_port_number_,
             std::vector<void *> * update_list)
            : to(to_), backward(backward_)
            , input_port_number(input_port_number_), output_port_number(output_port_number_)
        {
            update_info.update_list = update_list;
            update_info.id = this;
        }

        /// Processor id this edge points to.
        /// It is processor with output_port for direct edge or processor with input_port for backward.
        uint64_t to = std::numeric_limits<uint64_t>::max();
        bool backward;
        /// Port numbers. They are same for direct and backward edges.
        uint64_t input_port_number;
        uint64_t output_port_number;

        /// Edge version is increased when port's state is changed (e.g. when data is pushed). See Port.h for details.
        /// To compare version with prev_version we can decide if neighbour processor need to be prepared.
        Port::UpdateInfo update_info;
    };

    /// Use std::list because new ports can be added to processor during execution.
    using Edges = std::list<Edge>;

    /// Small structure with context of executing job.
    struct ExecutionState
    {
        std::exception_ptr exception;
        std::function<void()> job;

        IProcessor * processor = nullptr;
        uint64_t processors_id = 0;

        /// Counters for profiling.
        uint64_t num_executed_jobs = 0;
        uint64_t execution_time_ns = 0;
        uint64_t preparation_time_ns = 0;
    };

    /// Status for processor.
    /// Can be owning or not. Owning means that executor who set this status can change node's data and nobody else can.
    enum class ExecStatus
    {
        Idle,  /// prepare returned NeedData or PortFull. Non-owning.
        Preparing,  /// some executor is preparing processor, or processor is in task_queue. Owning.
        Executing,  /// prepare returned Ready and task is executing. Owning.
        Finished,  /// prepare returned Finished. Non-owning.
        Async  /// prepare returned Async. Owning.
    };

    /// Graph node. Represents single Processor.
    struct Node
    {
        /// Processor and it's position in graph.
        IProcessor * processor = nullptr;
        uint64_t processors_id = 0;

        /// Direct edges are for output ports, back edges are for input ports.
        Edges direct_edges;
        Edges back_edges;

        /// Current status. It is accessed concurrently, using mutex.
        ExecStatus status = ExecStatus::Idle;
        std::mutex status_mutex;

        /// Job and exception. Job calls processor->work() inside and catch exception.
        std::function<void()> job;
        std::exception_ptr exception;

        /// Last state for profiling.
        IProcessor::Status last_processor_status = IProcessor::Status::NeedData;

        /// Ports which have changed their state since last processor->prepare() call.
        /// They changed when neighbour processors interact with connected ports.
        /// They will be used as arguments for next processor->prepare() (and will be cleaned after that).
        IProcessor::PortNumbers updated_input_ports;
        IProcessor::PortNumbers updated_output_ports;

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

        Node(IProcessor * processor_, uint64_t processor_id)
            : processor(processor_), processors_id(processor_id)
        {
        }
    };

    using NodePtr = std::unique_ptr<Node>;
    using Nodes = std::vector<NodePtr>;
    Nodes nodes;

    /// IProcessor * -> processors_id (position in graph)
    using ProcessorsMap = std::unordered_map<const IProcessor *, uint64_t>;
    ProcessorsMap processors_map;

    explicit ExecutingGraph(const Processors & processors);

    /// Update graph after processor returned ExpandPipeline status.
    /// Processors should already contain newly-added processors.
    /// Returns newly-added nodes and nodes which edges were modified.
    std::vector<uint64_t> expandPipeline(const Processors & processors);

private:
    /// Add single edge to edges list. Check processor is known.
    Edge & addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to);

    /// Append new edges for node. It is called for new node or when new port were added after ExpandPipeline.
    /// Returns true if new edge was added.
    bool addEdges(uint64_t node);
};

}
