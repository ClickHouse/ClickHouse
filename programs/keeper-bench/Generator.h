#pragma once
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <functional>
#include <optional>
#include <pcg-random/pcg_random.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/randomSeed.h>

struct NumberGetter
{
    static NumberGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value = std::nullopt);
    uint64_t getNumber() const;
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
    std::string getString() const;
    std::string description() const;
    bool isRandom() const;
private:
    std::variant<std::string, NumberGetter> value;
};

struct PathGetter
{
    static PathGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::string getPath() const;
    std::string description() const;

    void initialize(Coordination::ZooKeeper & zookeeper);
private:
    std::vector<std::string> parent_paths;

    bool initialized = false;

    std::vector<std::string> paths;
    mutable std::uniform_int_distribution<size_t> path_picker;
};

struct RequestGenerator
{
    virtual ~RequestGenerator() = default;

    void getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    Coordination::ZooKeeperRequestPtr generate(const Coordination::ACLs & acls);

    std::string description();

    void startup(Coordination::ZooKeeper & zookeeper);

    size_t getWeight() const;
private:
    virtual void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) = 0;
    virtual std::string descriptionImpl() = 0;
    virtual Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) = 0;
    virtual void startupImpl(Coordination::ZooKeeper &) {}

    size_t weight = 1;
};

using RequestGeneratorPtr = std::shared_ptr<RequestGenerator>;

struct CreateRequestGenerator final : public RequestGenerator
{
    CreateRequestGenerator();
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter parent_path;
    StringGetter name;
    std::optional<StringGetter> data;

    std::optional<double> remove_factor;
    pcg64 rng;
    std::uniform_real_distribution<double> remove_picker;

    std::unordered_set<std::string> paths_created;
};

struct SetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
    StringGetter data;
};

struct GetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
};

struct ListRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
};

struct RequestGetter
{
    explicit RequestGetter(std::vector<RequestGeneratorPtr> request_generators_);

    RequestGetter() = default;

    static RequestGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, bool for_multi = false);

    RequestGeneratorPtr getRequestGenerator() const;
    std::string description() const;
    void startup(Coordination::ZooKeeper & zookeeper);
    const std::vector<RequestGeneratorPtr> & requestGenerators() const;
private:
    std::vector<RequestGeneratorPtr> request_generators;
    std::vector<size_t> weights;
    mutable std::uniform_int_distribution<size_t> request_generator_picker;
};

struct MultiRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    std::optional<NumberGetter> size;
    RequestGetter request_getter;
};

class Generator
{
public:
    explicit Generator(const Poco::Util::AbstractConfiguration & config);

    void startup(Coordination::ZooKeeper & zookeeper);
    Coordination::ZooKeeperRequestPtr generate();
private:

    std::uniform_int_distribution<size_t> request_picker;
    RequestGetter request_getter;
    Coordination::ACLs default_acls;
};
