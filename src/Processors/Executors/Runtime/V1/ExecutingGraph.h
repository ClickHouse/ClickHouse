#pragma once

#include <Common/CacheLine.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/ListWithMemoryTracking.h>
#include <Processors/Port.h>
#include <Processors/IProcessor.h>
#include <absl/container/inlined_vector.h>
#include <Common/SharedMutex.h>
#include <atomic>
#include <list>
#include <memory>
#include <mutex>


namespace DB
{

struct MemoryReservation;
class MemorySpillScheduler;

namespace Runtime::V1
{

class ExecutingGraph
{
public:
    /// Graph node. Represents single Processor. It is defined below.
    struct Node;

private:
    /// Edge represents connection between OutputPort and InputPort.
    /// For every connection, two edges are created: direct and backward (it is specified by backward flag).
    struct Edge
    {
        Edge(Node * to_, bool backward_,
             InputPort * input_port_, OutputPort * output_port_,
             VectorWithMemoryTracking<void *> * update_list)
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
    using Edges = ListWithMemoryTracking<Edge>;

    /// Status for processor.
    /// Can be owning or not. Owning means that executor who set this status can change node's data and nobody else can.
    enum class ExecStatus : uint8_t
    {
        Idle,  /// prepare returned NeedData or PortFull. Non-owning.
        Preparing,  /// some executor is preparing processor, or processor is in task_queue. Owning.
        Executing,  /// prepare returned Ready or Async and task is executing. Owning.
        Finished,  /// prepare returned Finished. Non-owning.
    };

    /// Forward decl so Node can hold an iterator into the owning Nodes list.
    using Nodes = ListWithMemoryTracking<struct Node>;

public:
    /// Graph node. Represents single Processor.
    /// The tasks of the executor are the nodes whose processors have to be executed: the executor keeps
    /// the node of a processor, so that the graph does not look it up when the processor has been executed.
    struct Node
    {
        /// Iterator into the graph's processors list.
        Processors::iterator processor_iter{};

        /// The processor itself: the executor reads it without the mutex of the graph,
        /// while an expansion of the pipeline may change the list of processors.
        IProcessor * processor_ptr = nullptr;

        /// Iterator into the graph's nodes list pointing at this node.
        Nodes::iterator self_iter{};

        /// Stable, monotonically-assigned id. Never reused. Used only for diagnostics (logs, system.processors_profile_log).
        uint64_t processors_id = 0;

        [[nodiscard]] IProcessor * processor() const { return processor_ptr; }

        /// Direct edges are for output ports, back edges are for input ports.
        Edges direct_edges;
        Edges back_edges;

        /// Current status. It is accessed concurrently, using mutex,
        /// unless a single thread updates the graph (see `multithreaded`).
        ExecStatus status = ExecStatus::Idle;
        std::mutex status_mutex;
        /// Non-null for an independently scheduled spill; normal `work` must not run for this task.
        ISpillable * spill_target = nullptr;
        absl::InlinedVector<ISpillable *, 2> registered_spillables;

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

        Node(Processors::iterator processor_iter_, uint64_t processors_id_)
            : processor_iter(processor_iter_), processor_ptr(processor_iter_->get()), processors_id(processors_id_)
        {
        }
    };

    /// A queue of tasks. The graph fills it, and then the executor takes all the tasks out of it, after every
    /// executed task. Its memory is kept in between and is used again from the start once the queue is empty,
    /// so that pushing a task does not allocate memory or move the other tasks every time.
    /// This queue can grow a lot and lead to OOM, so it is allocated through a tracking
    /// allocator that throws instead of overcommitting.
    class Queue
    {
    public:
        void push(Node * node) { nodes.push_back(node); }

        [[nodiscard]] Node * front() const { return nodes[head]; }

        void pop()
        {
            ++head;
            if (head == nodes.size())
            {
                nodes.clear();
                head = 0;
            }
        }

        [[nodiscard]] bool empty() const { return head == nodes.size(); }
        [[nodiscard]] size_t size() const { return nodes.size() - head; }

    private:
        VectorWithMemoryTracking<Node *> nodes;
        /// The index of the first task that is not taken yet.
        size_t head = 0;
    };

    ExecutingGraph(std::shared_ptr<Processors> processors_, bool profile_processors_, MemoryReservation * memory_reservation_);
    ~ExecutingGraph();

    /// Called after workers stop, including cancellation and exception paths.
    void detachSpillables();
    /// `thread_num` is the number of the calling thread, as in `updateNode`.
    void scheduleSpills(Queue & queue, size_t thread_num);

    /// Traverse graph the first time to update all the childless nodes.
    /// `num_threads` is the maximum number of threads that will update the graph.
    void initializeExecution(Queue & queue, Queue & async_queue, size_t num_threads);

    enum class UpdateNodeStatus
    {
        Done,
        Cancelled,
    };

    /// Working memory of `updateNode`. The caller keeps it between the calls,
    /// so that memory is not allocated every time a processor is updated.
    struct UpdateNodeBuffers
    {
        /// Stacks: the last added element is processed first.
        VectorWithMemoryTracking<Edge *> updated_edges;
        VectorWithMemoryTracking<Node *> updated_processors;
        VectorWithMemoryTracking<Node *> pending_expansion;

        /// A buffer that has grown larger than this number of elements is released instead of being kept,
        /// so that a single wide wave of updates does not hold its memory until the end of the execution.
        static constexpr size_t max_kept_capacity = 1024;

        /// Empty the buffers before the next use.
        void reset();
    };

    /// Update the processor of the `initial` node (call `IProcessor::prepare`).
    /// Check parents and children of current processor and push them to stacks if they also need to be updated.
    /// If processor wants to be expanded, lock will be upgraded to get write access to pipeline.
    /// `thread_num` is the number of the calling thread, less than `num_threads` of `initializeExecution`.
    UpdateNodeStatus updateNode(Node & initial, Queue & queue, Queue & async_queue, UpdateNodeBuffers & buffers, size_t thread_num);

    /// Cancel every processor with the given reason.
    void cancel(IProcessor::CancelReason reason);

    const Processors & getProcessors() const;
    bool isAllFinished() const;

    String dump() const;

private:
    /// All graph nodes. Nodes type is forward-declared above so Node can hold a self-iterator.
    Nodes nodes;

    /// Each processor is directly tied to pipeline graph node.
    using ProcessorsMap = UnorderedMapWithMemoryTracking<const IProcessor *, Node *>;
    ProcessorsMap processors_map;

    struct SpillState
    {
        SpillState(Nodes & nodes_, MemoryReservation * memory_reservation_);
        ~SpillState();

        void detachSpillables();
        void registerSpillables(Node & node);
        void unregisterSpillables(Node & node);
        bool scheduleSpill(Node & node, ISpillable * spillable, Queue & queue) const;
        /// Caller holds the graph's `nodes_mutex` to keep candidate nodes alive.
        void scheduleSpills(Queue & queue);

        Nodes & nodes;
        /// Node lifetimes are protected by `nodes_mutex`; membership changes also take this mutex.
        std::mutex spillables_mutex;
        UnorderedMapWithMemoryTracking<ISpillable *, absl::InlinedVector<Node *, 2>> spillable_owners;
        MemoryReservation * memory_reservation = nullptr;
        std::shared_ptr<MemorySpillScheduler> memory_spill_scheduler;
    };

    /// Append a processor to the graph's processors list, create its Node, assign a stable id,
    /// register it in the processors map. Does not create edges — that is done separately by addEdges.
    Node & addNode(ProcessorPtr processor);
    Node & addNode(Processors::iterator processor_iter);
    Node * getNodeToRemove(const ProcessorPtr & processor) const;
    void removeNode(Node & node);

    /// Add single edge to edges list. Check processor is known.
    Edge & addEdge(Edges & edges, Edge edge, const Node & from, const IProcessor * to);

    /// Edges newly appended for a node by a call to addEdges.
    struct NewEdges
    {
        VectorWithMemoryTracking<Edge *> back;    // back edges added (inputs side of this node)
        VectorWithMemoryTracking<Edge *> direct;  // direct edges added (outputs side of this node)
        [[nodiscard]] bool empty() const { return back.empty() && direct.empty(); }
    };
    NewEdges addEdges(Node & node);
    void removeAffectedEdges(Node & node, const UnorderedSetWithMemoryTracking<Node *> & removed_nodes);

    /// Update graph after processor `node` returned UpdatePipeline status.
    /// All new nodes and nodes with updated ports are pushed into stack.
    UpdateNodeStatus updatePipeline(VectorWithMemoryTracking<Node *> & stack, Node & node);
    UpdateNodeStatus updatePipelineImpl(VectorWithMemoryTracking<Node *> & stack, Node & node, IProcessor::PipelineUpdate & update);

    /// Shared with QueryPipeline.
    std::shared_ptr<Processors> processors;
    std::mutex processors_mutex;

    struct PendingRemovalGroup
    {
        Processors processors;
        std::atomic<size_t> not_finished = 0;
    };
    UnorderedMapWithMemoryTracking<ProcessorPtr, std::shared_ptr<PendingRemovalGroup>> removed_processors;

    void removePendingGroup(PendingRemovalGroup & group, Processors & delayed_destruction);
    void removeReadyGroups(Processors & delayed_destruction);
    std::shared_ptr<PendingRemovalGroup> findGroupReadyForRemoval();
    void accountFinishedProcessorInGroup(const ProcessorPtr & processor);

    /// Monotonic counter for assigning Node::processors_id.
    uint64_t next_node_id = 0;

    /// A readers-writer mutex for the structure of the graph: the lists of nodes and edges, `processors_map` and
    /// `removed_processors`. Every thread reads the structure after every executed task, and it is changed only
    /// when the pipeline is expanded or processors are removed from it. With a single shared mutex, all the threads
    /// would write to its cache line just to read the graph. So there is a mutex for every thread: a reader
    /// locks only the mutex of its thread, and a writer locks the mutexes of all the threads.
    /// The writer also has exclusive access to every node, as all the nodes are updated under the reader lock.
    class NodesMutex
    {
    public:
        /// What a thread locks to read the graph.
        class ForThread
        {
        public:
            explicit ForThread(SharedMutex * mutex_) : mutex(mutex_) {}

            void lock_shared();
            void unlock_shared();

        private:
            SharedMutex * mutex;
        };

        /// Called before the threads start. Nothing is locked if a single thread updates the graph.
        void init(size_t num_threads);

        ForThread forThread(size_t thread_num);

        /// Exclusive ownership.
        void lock();
        void unlock();

    private:
        struct alignas(CH_CACHE_LINE_SIZE) ThreadMutex
        {
            SharedMutex mutex;
        };

        std::unique_ptr<ThreadMutex[]> thread_mutexes;
        size_t num_thread_mutexes = 0;
    };

    NodesMutex nodes_mutex;

    /// Whether several threads update the graph. If not, `updateNode` locks neither `nodes_mutex` nor the mutexes
    /// of the nodes: it is called after every executed task, and the locks would be a noticeable part of its time.
    /// `processors_mutex` is locked regardless, because `cancel` is called from other threads.
    bool multithreaded = false;

    /// Lock the status of a node, unless a single thread updates the graph.
    std::unique_lock<std::mutex> lockStatus(Node & node) const;

    /// Set when `updatePipeline` threw halfway through recording the processors an expansion
    /// added (an allocation failed, for example): the parent's ports are already connected to
    /// processors that never became nodes, so the nodes and edges no longer describe the ports and
    /// any further expansion would trip over them. The exception cancels the executor, but another thread
    /// may see the flag before that; later expansions only bail out. Guarded by the `nodes_mutex` write lock.
    bool expansion_failed = false;

    const bool profile_processors;
    IProcessor::CancelReason cancel_reason = IProcessor::CancelReason::NotCancelled;

    /// Declared last so spillables are detached before `nodes` and their processors are destroyed,
    /// including when graph construction fails.
    SpillState spill_state;
};

}

}
