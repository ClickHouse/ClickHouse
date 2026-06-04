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


struct NumberGetter
{
    static NumberGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value = std::nullopt);
    uint64_t getNumber() const;
    std::string description() const;
    void setSeed(uint64_t seed) { rng.seed(seed); }
private:
    struct NumberRange
    {
        uint64_t min_value;
        uint64_t max_value;
    };

    std::variant<uint64_t, NumberRange> value;
    mutable pcg64 rng{randomSeed()};
};

struct StringGetter
{
    explicit StringGetter(NumberGetter number_getter)
        : value(std::move(number_getter))
    {}

    StringGetter() = default;

    static StringGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);
    void setString(std::string name);
    std::string getString() const;
    std::string description() const;
    bool isRandom() const;
    void setSeed(uint64_t seed);
private:
    std::variant<std::string, NumberGetter> value;
    mutable pcg64 rng{randomSeed()};
};

struct PathGetter
{
    static PathGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::string getPath() const;
    std::string description() const;

    void initialize(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths = nullptr);
    void setSeed(uint64_t seed) { rng.seed(seed); }
private:
    std::vector<std::string> parent_paths;
    std::vector<std::string> tag_names;

    bool initialized = false;

    std::vector<std::string> paths;
    mutable std::uniform_int_distribution<size_t> path_picker;
    mutable pcg64 rng{randomSeed()};
};

/// Default ACLs used throughout keeper-bench (world:anyone with all permissions)
Coordination::ACLs getDefaultACLs();

struct ZooKeeperRequestWithCallbacks
{
    Coordination::ZooKeeperRequestPtr request;
    std::vector<std::function<void()>> on_success_callbacks;
    std::vector<std::function<void()>> on_failure_callbacks;
};

struct RequestGenerator
{
    virtual ~RequestGenerator() = default;

    void getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    ZooKeeperRequestWithCallbacks generate(const Coordination::ACLs & acls);

    std::string description();

    void startup(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths = nullptr);
    void setSeed(uint64_t seed);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);

    size_t getWeight() const;
private:
    virtual void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) = 0;
    virtual std::string descriptionImpl() = 0;
    virtual ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) = 0;
    virtual void startupImpl(Coordination::ZooKeeper &, const TaggedPaths *) {}
    virtual void setSeedImpl(uint64_t) {}
    virtual void setWatchCallbackImpl(Coordination::WatchCallbackPtr) {}

    size_t weight = 1;
protected:
    Coordination::WatchCallbackPtr watch_callback_ptr;
};

using RequestGeneratorPtr = std::shared_ptr<RequestGenerator>;

struct CreateRequestGenerator final : public RequestGenerator
{
    CreateRequestGenerator();
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths) override;
    void setSeedImpl(uint64_t seed) override;

    PathGetter parent_path;
    StringGetter name;
    std::optional<StringGetter> data;

    std::optional<double> remove_factor;
    pcg64 rng;
    std::uniform_real_distribution<double> remove_picker;

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
    ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths) override;
    void setSeedImpl(uint64_t seed) override;

    PathGetter path;
    StringGetter data;
};

struct GetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths) override;
    void setSeedImpl(uint64_t seed) override;

    PathGetter path;
    std::optional<double> watch_probability;
    pcg64 watch_rng{randomSeed()};
    std::uniform_real_distribution<double> watch_picker{0, 1.0};
};

struct ListRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths) override;
    void setSeedImpl(uint64_t seed) override;

    PathGetter path;
    std::optional<double> watch_probability;
    pcg64 watch_rng{randomSeed()};
    std::uniform_real_distribution<double> watch_picker{0, 1.0};
};

struct RequestGetter
{
    explicit RequestGetter(std::vector<RequestGeneratorPtr> request_generators_);

    RequestGetter() = default;

    static RequestGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, bool for_multi = false);

    RequestGeneratorPtr getRequestGenerator() const;
    std::string description() const;
    void startup(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths = nullptr);
    void setSeed(uint64_t seed);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);
    const std::vector<RequestGeneratorPtr> & requestGenerators() const;
private:
    std::vector<RequestGeneratorPtr> request_generators;
    std::vector<size_t> weights;
    mutable std::uniform_int_distribution<size_t> request_generator_picker;
    mutable pcg64 rng{randomSeed()};
};

struct MultiRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper, const TaggedPaths * tagged_paths) override;
    void setSeedImpl(uint64_t seed) override;
    void setWatchCallbackImpl(Coordination::WatchCallbackPtr callback) override;

    std::optional<NumberGetter> size;
    RequestGetter request_getter;
};

class Generator
{
public:
    Generator() = default;

    void startup(const Poco::Util::AbstractConfiguration & config, Coordination::ZooKeeper & zookeeper, size_t thread_idx, const TaggedPaths * tagged_paths = nullptr);
    void setWatchCallback(Coordination::WatchCallbackPtr callback);
    ZooKeeperRequestWithCallbacks generate();

    uint64_t getSeed() const { return seed; }
private:
    uint64_t seed;

    std::uniform_int_distribution<size_t> request_picker;
    RequestGetter request_getter;
    Coordination::ACLs default_acls;
};
