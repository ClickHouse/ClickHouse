#pragma once

#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/SchedulerRoot.h>
#include <Common/Scheduler/Nodes/ClassifiersConfig.h>

#include <mutex>

namespace DB
{

/*
 * Implementation of `IResourceManager` supporting arbitrary dynamic hierarchy of scheduler nodes.
 * All resources are controlled by single root `SchedulerRoot`.
 *
 * State of manager is set of resources attached to the scheduler. States are referenced by classifiers.
 * Classifiers are used (1) to access resources and (2) to keep shared ownership of resources with pending
 * resource requests. This allows `ResourceRequest` and `ResourceLink` to hold raw pointers as long as
 * `ClassifierPtr` is acquired and held.
 *
 * Manager can update configuration after initialization. During update, new version of resources are also
 * attached to scheduler, so multiple version can coexist for a short period. This will violate constraints
 * (e.g. in-fly-limit), because different version have independent nodes to impose constraints, the same
 * violation will apply to fairness. Old version exists as long as there is at least one classifier
 * instance referencing it. Classifiers are typically attached to queries and will be destructed with them.
 */
class DynamicResourceManager : public IResourceManager
{
public:
    DynamicResourceManager();
    void updateConfiguration(const Poco::Util::AbstractConfiguration & config) override;
    ClassifierPtr acquire(const String & classifier_name) override;
    void forEachNode(VisitorFunc visitor) override;

private:
    /// Holds everything required to work with one specific configuration
    struct State
    {
        struct Node
        {
            String type;
            SchedulerNodePtr ptr;

            Node(
                const String & name,
                EventQueue * event_queue,
                const Poco::Util::AbstractConfiguration & config,
                const std::string & config_prefix);
            bool equals(const Node & o) const;
        };

        struct Resource
        {
            std::unordered_map<String, Node> nodes; // by path
            SchedulerRoot * attached_to = nullptr;

            Resource(
                const String & name,
                EventQueue * event_queue,
                const Poco::Util::AbstractConfiguration & config,
                const std::string & config_prefix);
            ~Resource(); // unregisters resource from scheduler
            bool equals(const Resource & o) const;
        };

        using ResourcePtr = std::shared_ptr<Resource>;

        std::unordered_map<String, ResourcePtr> resources; // by name
        ClassifiersConfig classifiers;

        State() = default;
        explicit State(EventQueue * event_queue, const Poco::Util::AbstractConfiguration & config);
    };

    using StatePtr = std::shared_ptr<State>;

    /// Created per query, holds State used by that query
    class Classifier : public IClassifier
    {
    public:
        Classifier(const StatePtr & state_, const String & classifier_name);
        ResourceLink get(const String & resource_name) override;
    private:
        std::unordered_map<String, ResourceLink> resources; // accessible resources by names
        StatePtr state; // hold state to avoid ResourceLink invalidation due to resource deregistration from SchedulerRoot
    };

    SchedulerRoot scheduler;
    std::mutex mutex;
    StatePtr state;
};

}
