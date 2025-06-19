#pragma once

#include <base/defines.h>
#include <base/scope_guard.h>

#include <Common/Logger.h>
#include <Common/Scheduler/WorkloadSettings.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/SchedulerRoot.h>
#include <Common/Scheduler/Nodes/UnifiedSchedulerNode.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>

#include <Parsers/IAST_fwd.h>

#include <boost/core/noncopyable.hpp>

#include <exception>
#include <memory>
#include <mutex>
#include <future>
#include <unordered_set>

namespace DB
{

/*
 * Implementation of `IResourceManager` that creates hierarchy of scheduler nodes according to
 * workload entities (WORKLOADs and RESOURCEs). It subscribes for updates in IWorkloadEntityStorage and
 * creates hierarchy of UnifiedSchedulerNode identical to the hierarchy of WORKLOADs.
 * For every RESOURCE an independent hierarchy of scheduler nodes is created.
 *
 * Manager process updates of WORKLOADs and RESOURCEs: CREATE/DROP/ALTER.
 * When a RESOURCE is created (dropped) a corresponding scheduler nodes hierarchy is created (destroyed).
 * After DROP RESOURCE parts of hierarchy might be kept alive while at least one query uses it.
 *
 * Classifiers are used (1) to access resources and (2) to keep shared ownership of scheduling nodes.
 * This allows `ResourceRequest` and `ResourceLink` to hold raw pointers as long as
 * `ClassifierPtr` is acquired and held.
 *
 * === RESOURCE ARCHITECTURE ===
 * Let's consider how a single resource is implemented. Every workload is represented by corresponding UnifiedSchedulerNode.
 * Every UnifiedSchedulerNode manages its own subtree of ISchedulerNode objects (see details in UnifiedSchedulerNode.h)
 * UnifiedSchedulerNode for workload w/o children has a queue, which provide a ResourceLink for consumption.
 * Parent of the root workload for a resource is SchedulerRoot with its own scheduler thread.
 * So every resource has its dedicated thread for processing of resource request and other events (see EventQueue).
 *
 * Here is an example of SQL and corresponding hierarchy of scheduler nodes:
 *    CREATE RESOURCE my_io_resource (...)
 *    CREATE WORKLOAD all
 *    CREATE WORKLOAD production PARENT all
 *    CREATE WORKLOAD development PARENT all
 *
 *             root                - SchedulerRoot (with scheduler thread and EventQueue)
 *               |
 *              all                - UnifiedSchedulerNode
 *               |
 *            p0_fair              - FairPolicy (part of parent UnifiedSchedulerNode internal structure)
 *            /     \
 *    production     development   - UnifiedSchedulerNode
 *        |               |
 *      queue           queue      - FifoQueue (part of parent UnifiedSchedulerNode internal structure)
 *
 * === UPDATING WORKLOADS ===
 * Workload may be created, updated or deleted.
 * Updating a child of a workload might lead to updating other workloads:
 *  1. Workload itself: it's structure depend on settings of children workloads
 *     (e.g. fifo node of a leaf workload is remove when the first child is added;
 *      and a fair node is inserted after the first two children are added).
 *  2. Other children: for them path to root might be changed (e.g. intermediate priority node is inserted)
 *
 * === VERSION CONTROL ===
 * Versions are created on hierarchy updates and hold ownership of nodes that are used through raw pointers.
 * Classifier reference version of every resource it use. Older version reference newer version.
 * Here is a diagram explaining version control based on Version objects (for 1 resource):
 *
 *       [nodes]      [nodes]         [nodes]
 *          ^            ^               ^
 *          |            |               |
 *       version1 --> version2 -...-> versionN
 *          ^                           ^  ^
 *          |                           |  |
 *       old_classifier    new_classifier  current_version
 *
 * Previous version should hold reference to a newer version. It is required for proper handling of updates.
 * Classifiers that were created for any of old versions may use nodes of newer version due to updateNode().
 * It may move a queue to a new position in the hierarchy or create/destroy constraints, thus resource requests
 * created by old classifier may reference constraints of newer versions through `request->constraints` which
 * is filled during dequeueRequest().
 *
 * === THREADS ===
 * scheduler thread:
 *  - one thread per resource
 *  - uses event_queue (per resource) for processing w/o holding mutex for every scheduler node
 *  - handle resource requests
 *  - node activations
 *  - scheduler hierarchy updates
 * query thread:
 *  - multiple independent threads
 *  - send resource requests
 *  - acquire and release classifiers (via scheduler event queues)
 * control thread:
 *  - modify workload and resources through subscription
 *
 * === SYNCHRONIZATION ===
 * List of related sync primitives and their roles:
 * WorkloadResourceManager::mutex
 *  - protects resource manager data structures - resource and workloads
 *  - serialize control thread actions
 * WorkloadResourceManager::Resource::scheduler->event_queue
 *  - serializes scheduler hierarchy events
 *  - events are created in control and query threads
 *  - all events are processed by specific scheduler thread
 *  - hierarchy-wide actions: requests dequeueing, activations propagation and nodes updates.
 *  - resource version control management
 * FifoQueue::mutex and SemaphoreContraint::mutex
 *  - serializes query and scheduler threads on specific node accesses
 *  - resource request processing: enqueueRequest(), dequeueRequest() and finishRequest()
 */
class WorkloadResourceManager : public IResourceManager
{
public:
    explicit WorkloadResourceManager(IWorkloadEntityStorage & storage_);
    ~WorkloadResourceManager() override;
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config) override;
    bool hasResource(const String & resource_name) const override;
    ClassifierPtr acquire(const String & workload_name, const ClassifierSettings & settings) override;
    void forEachNode(VisitorFunc visitor) override;

private:
    // Forward declarations
    struct NodeInfo;
    struct Version;
    class Resource;
    struct Workload;
    class Classifier;

    friend struct Workload;

    using VersionPtr = std::shared_ptr<Version>;
    using ResourcePtr = std::shared_ptr<Resource>;
    using WorkloadPtr = std::shared_ptr<Workload>;

    /// Helper for parsing workload AST for a specific resource
    struct NodeInfo
    {
        String name; // Workload name
        String parent; // Name of parent workload
        WorkloadSettings settings; // Settings specific for a given resource

        NodeInfo(CostUnit unit, const ASTPtr & ast, const String & resource_name);
    };

    /// Ownership control for scheduler nodes, which could be referenced by raw pointers
    struct Version
    {
        std::vector<SchedulerNodePtr> nodes;
        VersionPtr newer_version;
    };

    /// Holds a thread and hierarchy of unified scheduler nodes for specific RESOURCE
    class Resource : public std::enable_shared_from_this<Resource>, boost::noncopyable
    {
    public:
        explicit Resource(const ASTPtr & resource_entity_);
        ~Resource();

        const String & getName() const { return resource_name; }
        CostUnit getUnit() const { return unit; }

        /// Hierarchy management
        void createNode(const NodeInfo & info);
        void deleteNode(const NodeInfo & info);
        void updateNode(const NodeInfo & old_info, const NodeInfo & new_info);

        /// Updates resource entity
        void updateResource(const ASTPtr & new_resource_entity);

        /// Updates a classifier to contain a reference for specified workload
        std::future<void> attachClassifier(Classifier & classifier, const String & workload_name);

        /// Remove classifier reference. This destroys scheduler nodes in proper scheduler thread
        std::future<void> detachClassifier(VersionPtr && version);

        /// Introspection
        void forEachResourceNode(WorkloadResourceManager::VisitorFunc & visitor);

    private:
        void updateCurrentVersion();

        template <class Task>
        void executeInSchedulerThread(Task && task)
        {
            std::promise<void> promise;
            auto future = promise.get_future();
            scheduler.event_queue->enqueue([&]
            {
                try
                {
                    task();
                    promise.set_value();
                }
                catch (...)
                {
                    promise.set_exception(std::current_exception());
                }
            });
            future.get(); // Blocks until execution is done in the scheduler thread
        }

        ASTPtr resource_entity;
        const String resource_name;
        const CostUnit unit;
        SchedulerRoot scheduler;

        // TODO(serxa): consider using resource_manager->mutex + scheduler thread for updates and mutex only for reading to avoid slow acquire/release of classifier
        /// These field should be accessed only by the scheduler thread
        std::unordered_map<String, UnifiedSchedulerNodePtr> node_for_workload;
        UnifiedSchedulerNodePtr root_node;
        VersionPtr current_version;
    };

    struct Workload : boost::noncopyable
    {
        WorkloadResourceManager * resource_manager;
        ASTPtr workload_entity;

        Workload(WorkloadResourceManager * resource_manager_, const ASTPtr & workload_entity_);
        ~Workload();

        void updateWorkload(const ASTPtr & new_entity);
        String getParent() const;
    };

    class Classifier : public IClassifier
    {
    public:
        explicit Classifier(const ClassifierSettings & settings_);
        ~Classifier() override;

        /// Implements IClassifier interface
        /// NOTE: It is called from query threads (possibly multiple)
        bool has(const String & resource_name) override;
        ResourceLink get(const String & resource_name) override;

        /// Attaches/detaches a specific resource
        /// NOTE: It is called from scheduler threads (possibly multiple)
        void attach(const ResourcePtr & resource, const VersionPtr & version, ResourceLink link);
        void detach(const ResourcePtr & resource);

    private:
        const ClassifierSettings settings;
        WorkloadResourceManager * resource_manager;
        std::mutex mutex;
        struct Attachment
        {
            ResourcePtr resource;
            VersionPtr version;
            ResourceLink link;
        };
        std::unordered_map<String, Attachment> attachments; // TSA_GUARDED_BY(mutex);
    };

    void createOrUpdateWorkload(const String & workload_name, const ASTPtr & ast);
    void deleteWorkload(const String & workload_name);
    void createOrUpdateResource(const String & resource_name, const ASTPtr & ast);
    void deleteResource(const String & resource_name);

    // Topological sorting of workloads
    void topologicallySortedWorkloadsImpl(Workload * workload, std::unordered_set<Workload *> & visited, std::vector<Workload *> & sorted_workloads);
    std::vector<Workload *> topologicallySortedWorkloads();

    IWorkloadEntityStorage & storage;
    scope_guard subscription;

    mutable std::mutex mutex;
    std::unordered_map<String, WorkloadPtr> workloads; // TSA_GUARDED_BY(mutex);
    std::unordered_map<String, ResourcePtr> resources; // TSA_GUARDED_BY(mutex);

    LoggerPtr log;
};

}
