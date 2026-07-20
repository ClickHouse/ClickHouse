#pragma once
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <optional>
#include <pcg-random/pcg_random.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/randomSeed.h>

#include <PathSet.h>

class NodesSetup;

/// Per-thread state passed to every `generate` call. Generators themselves are
/// immutable after parsing and shared by all worker threads.
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

/// Draws paths from exactly one `PathSet`: a literal path list, one `tagged`
/// reference, or one `children_of` reference.
struct PathGetter
{
    static PathGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup);

    /// nullopt if the set (shard) is currently empty, which can happen for
    /// dynamic sets; the request generator then declines to produce a request.
    std::optional<std::string> getPath(GenerateContext & ctx) const;
    std::string description() const;

    bool isDynamic() const { return set->is_dynamic; }
    const PathSetPtr & pathSet() const { return set; }

private:
    PathSetPtr set;
};

/// Default ACLs used throughout keeper-bench (world:anyone with all permissions)
Coordination::ACLs getDefaultACLs();

struct ZooKeeperRequestWithCallbacks
{
    /// nullptr if the generator declined to produce a request (e.g. its input
    /// path set is currently empty).
    Coordination::ZooKeeperRequestPtr request;
    /// Response may be nullptr, meaning some error.
    std::function<void(const Coordination::Response *)> callback {};
    /// The request draws paths from a dynamic path set, which may lag behind the
    /// real state, so "node doesn't exist" / "node already exists" results are
    /// expected: the runner counts them as ignored errors rather than real ones
    /// (even with `continue_on_error` disabled).
    bool ignore_missing_nodes = false;
};

struct RequestGenerator
{
    virtual ~RequestGenerator() = default;

    void getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup);

    ZooKeeperRequestWithCallbacks generate(GenerateContext & ctx, const Coordination::ACLs & acls);

    std::string description();

    void setWatchCallback(Coordination::WatchCallbackPtr callback);

    size_t getWeight() const;
private:
    virtual void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) = 0;
    virtual std::string descriptionImpl() = 0;
    virtual ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) = 0;
    virtual void setWatchCallbackImpl(Coordination::WatchCallbackPtr) {}

    size_t weight = 1;
protected:
    Coordination::WatchCallbackPtr watch_callback_ptr;
};

using RequestGeneratorPtr = std::shared_ptr<RequestGenerator>;

struct CreateRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;

    PathGetter parent_path;
    StringGetter name;
    std::optional<StringGetter> data;

    std::optional<double> remove_factor;

    /// Where the created paths are recorded (and taken from for removes): the
    /// explicit output `tag`, the `children_of` set of a fixed parent, or an
    /// anonymous set when `remove_factor` needs one. May be nullptr, in which
    /// case created paths are not tracked.
    PathSetPtr output_set;
};

struct SetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;

    PathGetter path;
    StringGetter data;
};

struct GetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;

    PathGetter path;
    std::optional<double> watch_probability;
};

struct ListRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;

    PathGetter path;
    std::optional<double> watch_probability;
};

struct RequestGetter
{
    explicit RequestGetter(std::vector<RequestGeneratorPtr> request_generators_);

    RequestGetter() = default;

    static RequestGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup, bool for_multi = false);

    /// Picks a generator (weighted) and asks it to generate. If it declines
    /// (empty dynamic path set), tries the other generators; returns a null
    /// request if all of them decline.
    ZooKeeperRequestWithCallbacks generate(GenerateContext & ctx, const Coordination::ACLs & acls) const;

    RequestGeneratorPtr getRequestGenerator(pcg64 & rng) const;
    std::string description() const;
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
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup) override;
    std::string descriptionImpl() override;
    ZooKeeperRequestWithCallbacks generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls) override;
    void setWatchCallbackImpl(Coordination::WatchCallbackPtr callback) override;

    std::optional<NumberGetter> size;
    RequestGetter request_getter;
};

/// Produces the benchmark workload described by the `generator` config section.
/// Immutable after `parse`; one instance is shared by all worker threads, each
/// thread passing its own `GenerateContext` to `generate`.
class Generator
{
public:
    Generator() = default;

    /// Parses the generator config, registering the path sets it references in
    /// `nodes_setup`. Called before the setup tree is created.
    void parse(const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup);
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
