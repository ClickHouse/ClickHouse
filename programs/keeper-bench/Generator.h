#pragma once
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <optional>
#include <pcg-random/pcg_random.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/randomSeed.h>

/// Maps tag name → list of znode paths created with that tag during setup.
using TaggedPaths = std::unordered_map<std::string, std::vector<std::string>>;

/// Returns child names of a znode by path. Used by PathGetter to resolve
/// `children_of` references during generator startup. The default
/// implementation (in GeneratedRunner) queries a running Keeper; the
/// storage-only runner supplies a callable that reads from an in-process
/// KeeperStorage.
using ListChildrenFn = std::function<std::vector<std::string>(const std::string &)>;

/// Per-thread state passed to every `generate` call. Generators themselves are
/// immutable after startup and shared by all worker threads.
struct GenerateContext
{
    pcg64 & rng;
    size_t thread_idx = 0;
};

struct NumberGetter
{
    static NumberGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value = std::nullopt);
    uint64_t getNumber(pcg64 & rng) const;
    std::string description() const;
private:
    struct NumberRange
    {
        uint64_t min_value;
        uint64_t max_value;
    };

    std::variant<uint64_t, NumberRange> value;
};

struct StringGetter
{
    explicit StringGetter(NumberGetter number_getter)
        : value(std::move(number_getter))
    {}

    StringGetter() = default;

    static StringGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);
    void setString(std::string name);
    std::string getString(pcg64 & rng) const;
    std::string description() const;
    bool isRandom() const;
private:
    std::variant<std::string, NumberGetter> value;
};

struct PathGetter
{
    static PathGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::string getPath(GenerateContext & ctx) const;
    std::string description() const;

    void initialize(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths = nullptr);
private:
    std::vector<std::string> parent_paths;
    std::vector<std::string> tag_names;

    bool initialized = false;

    std::vector<std::string> paths;
};

/// Default ACLs used throughout keeper-bench (world:anyone with all permissions)
Coordination::ACLs getDefaultACLs();

struct ZooKeeperRequestWithCallbacks
{
    Coordination::ZooKeeperRequestPtr request;
    /// Response may be nullptr, meaning some error.
    std::function<void(const Coordination::Response *)> callback {};
};

struct RequestGenerator
{
    virtual ~RequestGenerator() = default;

    void getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    ZooKeeperRequestWithCallbacks generate(GenerateContext & ctx, const Coordination::ACLs & acls);

    std::string description();

    void startup(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths = nullptr);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);

    size_t getWeight() const;
private:
    virtual void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) = 0;
    virtual std::string descriptionImpl() = 0;
    virtual ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) = 0;
    virtual void startupImpl(const ListChildrenFn &, const TaggedPaths *) {}
    virtual void setWatchCallbackImpl(Coordination::WatchCallbackPtr) {}

    size_t weight = 1;
protected:
    Coordination::WatchCallbackPtr watch_callback_ptr;
};

using RequestGeneratorPtr = std::shared_ptr<RequestGenerator>;

struct CreateRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void startupImpl(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths) override;

    PathGetter parent_path;
    StringGetter name;
    std::optional<StringGetter> data;

    std::optional<double> remove_factor;

    std::mutex paths_mutex;
    std::unordered_set<std::string> paths_pending;

    /// O(1) random-access set using vector + index map (swap-and-pop for removal)
    std::vector<std::string> paths_created_vec;
    std::unordered_map<std::string, size_t> paths_created_index;
};

struct SetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void startupImpl(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths) override;

    PathGetter path;
    StringGetter data;
};

struct GetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void startupImpl(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths) override;

    PathGetter path;
    std::optional<double> watch_probability;
};

struct ListRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void startupImpl(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths) override;

    PathGetter path;
    std::optional<double> watch_probability;
};

struct RequestGetter
{
    explicit RequestGetter(std::vector<RequestGeneratorPtr> request_generators_);

    RequestGetter() = default;

    static RequestGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, bool for_multi = false);

    RequestGeneratorPtr getRequestGenerator(pcg64 & rng) const;
    std::string description() const;
    void startup(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths = nullptr);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);
    const std::vector<RequestGeneratorPtr> & requestGenerators() const;
private:
    std::vector<RequestGeneratorPtr> request_generators;
    std::vector<size_t> weights;
    /// Upper bound (inclusive) for the random pick in getRequestGenerator.
    size_t picker_max = 0;
};

struct MultiRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void startupImpl(const ListChildrenFn & list_children, const TaggedPaths * tagged_paths) override;
    void setWatchCallbackImpl(Coordination::WatchCallbackPtr callback) override;

    std::optional<NumberGetter> size;
    RequestGetter request_getter;
};

/// Produces the benchmark workload described by the `generator` config section.
/// Immutable after `startup`; one instance is shared by all worker threads, each
/// thread passing its own `GenerateContext` to `generate`.
class Generator
{
public:
    Generator() = default;

    void startup(const Poco::Util::AbstractConfiguration & config, const ListChildrenFn & list_children, const TaggedPaths * tagged_paths = nullptr);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);
    ZooKeeperRequestWithCallbacks generate(GenerateContext & ctx);

    /// Seed for the given worker thread's rng: `generator.seed` config (plus
    /// thread index) if set, random otherwise.
    uint64_t getSeedFor(size_t thread_idx) const { return base_seed + thread_idx; }
private:
    uint64_t base_seed = 0;

    RequestGetter request_getter;
    Coordination::ACLs default_acls;
};
