#include <Generator.h>

#include <fmt/ranges.h>
#include <iostream>
#include <random>
#include <filesystem>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <NodesSetup.h>

using namespace Coordination;
using namespace zkutil;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

Coordination::ACLs getDefaultACLs()
{
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    return {std::move(acl)};
}

namespace
{
std::string generateRandomString(size_t length, pcg64 & rng)
{
    if (length == 0)
        return "";

    static const auto & chars = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::uniform_int_distribution<size_t> pick(0, sizeof(chars) - 2);

    std::string s;

    s.reserve(length);

    while (length--)
        s += chars[pick(rng)];

    return s;
}
}

NumberGetter
NumberGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value)
{
    NumberGetter number_getter;

    if (!config.has(key) && default_value.has_value())
    {
        number_getter.value = *default_value;
    }
    else if (config.has(key + ".min_value") && config.has(key + ".max_value"))
    {
        NumberRange range{.min_value = config.getUInt64(key + ".min_value"), .max_value = config.getUInt64(key + ".max_value")};
        if (range.max_value <= range.min_value)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Range is invalid for key {}: [{}, {}]", key, range.min_value, range.max_value);
        number_getter.value = range;
    }
    else
    {
        number_getter.value = config.getUInt64(key);
    }

    return number_getter;
}

std::string NumberGetter::description() const
{
    if (const auto * number = std::get_if<uint64_t>(&value))
        return std::to_string(*number);

    const auto & range = std::get<NumberRange>(value);
    return fmt::format("random value from range [{}, {}]", range.min_value, range.max_value);
}

uint64_t NumberGetter::getNumber(pcg64 & rng) const
{
    if (const auto * number = std::get_if<uint64_t>(&value))
        return *number;

    const auto & range = std::get<NumberRange>(value);
    return std::uniform_int_distribution<uint64_t>(range.min_value, range.max_value)(rng);
}

StringGetter StringGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    StringGetter string_getter;
    if (config.has(key + ".random_string"))
        string_getter.value
            = NumberGetter::fromConfig(key + ".random_string.size", config);
    else
        string_getter.value = config.getString(key);

    return string_getter;
}

void StringGetter::setString(std::string name)
{
    value = std::move(name);
}

std::string StringGetter::getString(pcg64 & rng) const
{
    if (const auto * string = std::get_if<std::string>(&value))
        return *string;

    const auto & number_getter = std::get<NumberGetter>(value);
    return generateRandomString(number_getter.getNumber(rng), rng);
}

std::string StringGetter::description() const
{
    if (const auto * string = std::get_if<std::string>(&value))
        return *string;

    const auto & number_getter = std::get<NumberGetter>(value);
    return fmt::format("random string with size of {}", number_getter.description());
}

bool StringGetter::isRandom() const
{
    return std::holds_alternative<NumberGetter>(value);
}

PathGetter PathGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    static constexpr std::string_view path_key_string = "path";

    std::vector<std::string> literal_paths;
    std::vector<std::string> parent_paths;
    std::vector<std::string> tag_names;

    Poco::Util::AbstractConfiguration::Keys path_keys;
    config.keys(key, path_keys);

    for (const auto & path_key : path_keys)
    {
        if (!path_key.starts_with(path_key_string))
            continue;

        const auto current_path_key_string = key + "." + path_key;
        const auto children_of_key = current_path_key_string + ".children_of";
        const auto tagged_key = current_path_key_string + ".tagged";
        if (config.has(children_of_key))
        {
            auto parent_node = config.getString(children_of_key);
            if (parent_node.empty() || parent_node[0] != '/')
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid path for request generator: '{}'", parent_node);
            parent_paths.push_back(std::move(parent_node));
        }
        else if (config.has(tagged_key))
        {
            auto tag_name = config.getString(tagged_key);
            if (tag_name.empty())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Empty tag name for request generator in key '{}'", current_path_key_string);
            tag_names.push_back(std::move(tag_name));
        }
        else
        {
            auto path = config.getString(key + "." + path_key);

            if (path.empty() || path[0] != '/')
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid path for request generator: '{}'", path);

            literal_paths.push_back(std::move(path));
        }
    }

    size_t num_sources = (literal_paths.empty() ? 0 : 1) + parent_paths.size() + tag_names.size();
    if (num_sources == 0)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "PathGetter has no paths configured for key '{}'", key);
    if (num_sources > 1)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "`path` for key '{}' must draw from exactly one source: literal path(s), one `tagged`, or one `children_of`",
            key);

    PathGetter path_getter;
    if (!literal_paths.empty())
        path_getter.set = nodes_setup.createLiteralSet(std::move(literal_paths));
    else if (!parent_paths.empty())
        path_getter.set = nodes_setup.getOrCreateChildrenOfSet(parent_paths[0]);
    else
        path_getter.set = nodes_setup.getOrCreateTagSet(tag_names[0]);

    path_getter.set->used_as_input = true;

    return path_getter;
}

std::string PathGetter::getPath(GenerateContext & ctx) const
{
    auto path = set->samplePath(ctx.rng, ctx.thread_idx);
    if (!path)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Path set '{}' is empty", set->name);
    return *std::move(path);
}

std::string PathGetter::description() const
{
    return set->name;
}

RequestGetter::RequestGetter(std::vector<RequestGeneratorPtr> request_generators_)
    : request_generators(std::move(request_generators_))
{}

RequestGetter RequestGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup, bool for_multi)
{
    RequestGetter request_getter;

    Poco::Util::AbstractConfiguration::Keys generator_keys;
    config.keys(key, generator_keys);

    bool use_weights = false;
    size_t weight_sum = 0;
    auto & generators = request_getter.request_generators;
    for (const auto & generator_key : generator_keys)
    {
        RequestGeneratorPtr request_generator;

        if (generator_key.starts_with("create"))
            request_generator = std::make_unique<CreateRequestGenerator>();
        else if (generator_key.starts_with("set"))
            request_generator = std::make_unique<SetRequestGenerator>();
        else if (generator_key.starts_with("get"))
            request_generator = std::make_unique<GetRequestGenerator>();
        else if (generator_key.starts_with("list"))
            request_generator = std::make_unique<ListRequestGenerator>();
        else if (generator_key.starts_with("multi"))
        {
            if (for_multi)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Nested multi requests are not allowed");
            request_generator = std::make_unique<MultiRequestGenerator>();
        }
        else
        {
            if (for_multi && (generator_key.starts_with("size") || generator_key.starts_with("weight")))
                continue;

            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown generator '{}' in key '{}'", generator_key, key);
        }

        request_generator->getFromConfig(key + "." + generator_key, config, nodes_setup);

        auto weight = request_generator->getWeight();
        use_weights |= weight != 1;
        weight_sum += weight;

        generators.push_back(std::move(request_generator));
    }

    if (generators.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No request generators found in config for key '{}'", key);


    request_getter.picker_max = use_weights ? weight_sum - 1 : generators.size() - 1;

    /// construct weight vector
    if (use_weights)
    {
        auto & weights = request_getter.weights;
        weights.reserve(generators.size());
        weights.push_back(generators[0]->getWeight() - 1);

        for (size_t i = 1; i < generators.size(); ++i)
            weights.push_back(weights.back() + generators[i]->getWeight());
    }

    return request_getter;
}

RequestGeneratorPtr RequestGetter::getRequestGenerator(pcg64 & rng) const
{
    auto random_number = std::uniform_int_distribution<size_t>(0, picker_max)(rng);

    if (weights.empty())
        return request_generators[random_number];

    auto it = std::lower_bound(weights.begin(), weights.end(), random_number);
    return request_generators[it - weights.begin()];
}

std::string RequestGetter::description() const
{
    std::string guard(30, '-');
    std::string description = guard;

    for (const auto & request_generator : request_generators)
        description += fmt::format("\n{}\n", request_generator->description());
    return description + guard;
}

void RequestGetter::setWatchCallback(Coordination::WatchCallbackPtr callback)
{
    for (auto & gen : request_generators)
        gen->setWatchCallback(callback);
}

const std::vector<RequestGeneratorPtr> & RequestGetter::requestGenerators() const
{
    return request_generators;
}

void RequestGenerator::getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    if (config.has(key + ".weight"))
    {
        weight = config.getUInt64(key + ".weight");
        if (weight == 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Generator weight must be >= 1, got 0 for key '{}'", key);
    }
    getFromConfigImpl(key, config, nodes_setup);
}

std::string RequestGenerator::description()
{
    std::string weight_string = weight == 1 ? "" : fmt::format("\n- weight: {}", weight);
    return fmt::format("{}{}", descriptionImpl(), weight_string);
}

ZooKeeperRequestWithCallbacks RequestGenerator::generate(GenerateContext & ctx, const Coordination::ACLs & acls)
{
    return generateImpl(ctx, acls);
}

void RequestGenerator::setWatchCallback(Coordination::WatchCallbackPtr callback)
{
    watch_callback_ptr = callback;
    setWatchCallbackImpl(std::move(callback));
}

size_t RequestGenerator::getWeight() const
{
    return weight;
}

void CreateRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    if (config.has(key + ".watch_probability"))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "watch_probability is not supported for create requests (key '{}')", key);

    parent_path = PathGetter::fromConfig(key, config, nodes_setup);

    name = StringGetter(NumberGetter::fromConfig(key + ".name_length", config, 10));

    if (config.has(key + ".data"))
        data = StringGetter::fromConfig(key + ".data", config);

    if (config.has(key + ".remove_factor"))
    {
        remove_factor = config.getDouble(key + ".remove_factor");
        if (*remove_factor < 0.0 || *remove_factor > 1.0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "remove_factor must be in [0.0, 1.0], got {}", *remove_factor);
    }
}

std::string CreateRequestGenerator::descriptionImpl()
{
    std::string data_string
        = data.has_value() ? fmt::format("data for created nodes: {}", data->description()) : "no data for created nodes";
    std::string remove_factor_string
        = remove_factor.has_value() ? fmt::format("- remove factor: {}", *remove_factor) : "- without removes";
    return fmt::format(
        "Create Request Generator\n"
        "- parent path(s) for created nodes: {}\n"
        "- name for created nodes: {}\n"
        "- {}\n"
        "{}",
        parent_path.description(),
        name.description(),
        data_string,
        remove_factor_string);
}

ZooKeeperRequestWithCallbacks CreateRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls)
{
    if (remove_factor.has_value() && std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *remove_factor)
    {
        std::lock_guard lock(paths_mutex);
        if (!paths_created_vec.empty())
        {
            auto request = std::make_shared<ZooKeeperRemoveRequest>();

            /// Pick a random element via swap-and-pop
            std::uniform_int_distribution<size_t> pick(0, paths_created_vec.size() - 1);
            size_t idx = pick(ctx.rng);

            request->path = paths_created_vec[idx];

            /// Swap with last, update index of swapped element, pop
            size_t last = paths_created_vec.size() - 1;
            if (idx != last)
            {
                paths_created_index[paths_created_vec[last]] = idx;
                std::swap(paths_created_vec[idx], paths_created_vec[last]);
            }
            paths_created_index.erase(request->path);
            paths_created_vec.pop_back();

            return {.request = request};
        }
    }

    auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->acls = acls;

    std::string node_candidate = std::filesystem::path(parent_path.getPath(ctx)) / name.getString(ctx.rng);

    {
        static constexpr size_t max_name_generation_retries = 1000;
        std::lock_guard lock(paths_mutex);
        size_t retries = 0;
        while (paths_created_index.contains(node_candidate) || paths_pending.contains(node_candidate))
        {
            if (++retries > max_name_generation_retries)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Failed to generate unique path after {} retries for parent '{}'. "
                    "Increase name_length or reduce create volume",
                    max_name_generation_retries,
                    parent_path.getPath(ctx));
            node_candidate = std::filesystem::path(parent_path.getPath(ctx)) / name.getString(ctx.rng);
        }

        paths_pending.insert(node_candidate);
    }

    request->path = node_candidate;

    if (data)
        request->data = data->getString(ctx.rng);

    auto callback = [this, candidate = std::move(node_candidate)](const Coordination::Response * response) mutable
    {
        std::lock_guard lock(paths_mutex);
        paths_pending.erase(candidate);
        if (response && response->error == Coordination::Error::ZOK)
        {
            paths_created_index[candidate] = paths_created_vec.size();
            paths_created_vec.push_back(std::move(candidate));
        }
    };

    return {.request = request, .callback = std::move(callback)};
}

void SetRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    if (config.has(key + ".watch_probability"))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "watch_probability is not supported for set requests (key '{}')", key);

    path = PathGetter::fromConfig(key, config, nodes_setup);

    data = StringGetter::fromConfig(key + ".data", config);
}

std::string SetRequestGenerator::descriptionImpl()
{
    return fmt::format(
        "Set Request Generator\n"
        "- path(s) to set: {}\n"
        "- data to set: {}",
        path.description(),
        data.description());
}

ZooKeeperRequestWithCallbacks SetRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperSetRequest>();
    request->path = path.getPath(ctx);
    request->data = data.getString(ctx.rng);
    return {.request = request};
}

void GetRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    path = PathGetter::fromConfig(key, config, nodes_setup);

    if (config.has(key + ".watch_probability"))
    {
        watch_probability = config.getDouble(key + ".watch_probability");
        if (*watch_probability < 0.0 || *watch_probability > 1.0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "watch_probability must be in [0.0, 1.0], got {}", *watch_probability);
    }
}

std::string GetRequestGenerator::descriptionImpl()
{
    std::string watch_string = watch_probability.has_value() ? fmt::format("\n- watch probability: {}", *watch_probability) : "";
    return fmt::format(
        "Get Request Generator\n"
        "- path(s) to get: {}{}",
        path.description(),
        watch_string);
}

ZooKeeperRequestWithCallbacks GetRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = path.getPath(ctx);
    if (watch_probability.has_value() && std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *watch_probability)
    {
        request->has_watch = true;
        request->watch_callback = watch_callback_ptr;
    }
    return {.request = request};
}

void ListRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    path = PathGetter::fromConfig(key, config, nodes_setup);

    if (config.has(key + ".watch_probability"))
    {
        watch_probability = config.getDouble(key + ".watch_probability");
        if (*watch_probability < 0.0 || *watch_probability > 1.0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "watch_probability must be in [0.0, 1.0], got {}", *watch_probability);
    }
}

std::string ListRequestGenerator::descriptionImpl()
{
    std::string watch_string = watch_probability.has_value() ? fmt::format("\n- watch probability: {}", *watch_probability) : "";
    return fmt::format(
        "List Request Generator\n"
        "- path(s) to list: {}{}",
        path.description(),
        watch_string);
}

ZooKeeperRequestWithCallbacks ListRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperListRequest>();
    request->path = path.getPath(ctx);
    request->list_request_type = ListRequestType::ALL;
    if (watch_probability.has_value() && std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *watch_probability)
    {
        request->has_watch = true;
        request->watch_callback = watch_callback_ptr;
    }
    return {.request = request};
}

void MultiRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    if (config.has(key + ".watch_probability"))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "watch_probability is not supported on multi requests directly; set it on individual get/list sub-requests instead (key '{}')", key);

    if (config.has(key + ".size"))
        size = NumberGetter::fromConfig(key + ".size", config);

    request_getter = RequestGetter::fromConfig(key, config, nodes_setup, /*for_multi*/ true);
};

std::string MultiRequestGenerator::descriptionImpl()
{
    std::string size_string = size.has_value() ? fmt::format("- number of requests: {}\n", size->description()) : "";
    return fmt::format(
        "Multi Request Generator\n"
        "{}"
        "- requests:\n{}",
        size_string,
        request_getter.description());
}

ZooKeeperRequestWithCallbacks MultiRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls)
{
    Coordination::Requests ops;
    std::vector<std::function<void(const Coordination::Response *)>> inner_callbacks;

    if (size)
    {
        auto request_count = size->getNumber(ctx.rng);

        for (size_t i = 0; i < request_count; ++i)
        {
            auto request_with_callbacks = request_getter.getRequestGenerator(ctx.rng)->generate(ctx, acls);
            ops.push_back(std::move(request_with_callbacks.request));
            inner_callbacks.push_back(std::move(request_with_callbacks.callback));
        }
    }
    else
    {
        for (const auto & request_generator : request_getter.requestGenerators())
        {
            auto request_with_callbacks = request_generator->generate(ctx, acls);
            ops.push_back(std::move(request_with_callbacks.request));
            inner_callbacks.push_back(std::move(request_with_callbacks.callback));
        }
    }

    auto request = std::make_shared<ZooKeeperMultiRequest>(ops, acls);
    bool is_read = request->isReadRequest();

    auto callback = [callbacks = std::move(inner_callbacks), is_read](const Coordination::Response * response)
    {
        const Coordination::MultiResponse * multi = nullptr;
        if (response)
        {
            multi = dynamic_cast<const Coordination::MultiResponse *>(response);
            chassert(multi);
        }

        /// No response (or a malformed one): sub-op outcomes are unknown.
        if (!multi || multi->responses.size() != callbacks.size())
        {
            for (const auto & inner_callback : callbacks)
                if (inner_callback)
                    inner_callback(nullptr);
            return;
        }

        /// A write multi is a transaction: if it failed, no sub-op was applied,
        /// even the ones whose own checks passed (they report ZOK).
        bool txn_failed = false;
        if (!is_read)
        {
            txn_failed = multi->error != Coordination::Error::ZOK;
            for (const auto & resp : multi->responses)
                txn_failed |= resp->error != Coordination::Error::ZOK;
        }

        for (size_t i = 0; i < callbacks.size(); ++i)
        {
            if (!callbacks[i])
                continue;

            const Coordination::Response * inner_response = multi->responses.at(i).get();
            if (txn_failed && inner_response->error == Coordination::Error::ZOK)
            {
                /// Report a synthetic error so the sub-op is not taken for an applied one.
                /// Only `error` is meaningful in this response object.
                Coordination::Response not_applied;
                not_applied.error = Coordination::Error::ZRUNTIMEINCONSISTENCY;
                callbacks[i](&not_applied);
            }
            else
                callbacks[i](inner_response);
        }
    };

    return {
        .request = std::move(request),
        .callback = std::move(callback),
    };
}

void MultiRequestGenerator::setWatchCallbackImpl(Coordination::WatchCallbackPtr callback)
{
    request_getter.setWatchCallback(std::move(callback));
}

void Generator::parse(const Poco::Util::AbstractConfiguration & config, NodesSetup & nodes_setup)
{
    if (config.has("generator.seed"))
        base_seed = config.getUInt64("generator.seed");
    else
        base_seed = randomSeed();

    default_acls = getDefaultACLs();

    static const std::string requests_key = "generator.requests";
    request_getter = RequestGetter::fromConfig(requests_key, config, nodes_setup);

    std::cerr << "Generator seed: " << base_seed << std::endl;
    std::cerr << request_getter.description() << std::endl;
}

void Generator::setWatchCallback(Coordination::WatchCallbackPtr callback)
{
    request_getter.setWatchCallback(std::move(callback));
}

ZooKeeperRequestWithCallbacks Generator::generate(GenerateContext & ctx)
{
    return request_getter.getRequestGenerator(ctx.rng)->generate(ctx, default_acls);
}
