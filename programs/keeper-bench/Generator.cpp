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

std::optional<std::string> PathGetter::getPath(GenerateContext & ctx) const
{
    return set->samplePath(ctx.rng, ctx.thread_idx);
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

ZooKeeperRequestWithCallbacks RequestGetter::generate(GenerateContext & ctx, const Coordination::ACLs & acls) const
{
    auto random_number = std::uniform_int_distribution<size_t>(0, picker_max)(ctx.rng);

    size_t picked = weights.empty()
        ? random_number
        : static_cast<size_t>(std::lower_bound(weights.begin(), weights.end(), random_number) - weights.begin());

    /// If the picked generator declines (its dynamic path set is empty), fall
    /// back to the remaining generators in order.
    for (size_t attempt = 0; attempt < request_generators.size(); ++attempt)
    {
        auto result = request_generators[(picked + attempt) % request_generators.size()]->generate(ctx, acls);
        if (result.request)
            return result;
    }

    return {};
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

    if (config.has(key + ".keep_count"))
    {
        if (remove_factor)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "remove_factor and keep_count are mutually exclusive (key '{}')", key);

        if (config.getString(key + ".keep_count") == "auto")
            keep_count = 0;
        else
        {
            keep_count = config.getUInt64(key + ".keep_count");
            if (*keep_count == 0)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "keep_count must be a positive number or 'auto' (key '{}')", key);
        }
    }

    bool needs_removes = remove_factor.has_value() || keep_count.has_value();

    /// Resolve the set that tracks the created nodes: an explicit output `tag`,
    /// the `children_of` set of a fixed parent, or an anonymous set if the
    /// removes need one.
    auto fixed_parent = parent_path.pathSet()->singleStagedPath();
    if (config.has(key + ".tag"))
    {
        auto tag_name = config.getString(key + ".tag");
        output_set = nodes_setup.getOrCreateTagSet(tag_name);
        /// Mixing an explicit tag with `children_of` references to the same parent
        /// would track the nodes in two sets; detected in validatePathSets.
        if (fixed_parent)
            nodes_setup.registerTagChildrenOfConflict(*fixed_parent, tag_name);
    }
    else if (fixed_parent)
    {
        output_set = nodes_setup.getOrCreateChildrenOfSet(*fixed_parent);
    }
    else if (needs_removes)
    {
        output_set = nodes_setup.createAnonymousSet(fmt::format("nodes created by '{}'", key));
    }

    if (output_set)
    {
        output_set->used_as_output = true;
        if (needs_removes)
            output_set->used_as_input = true;
        if (keep_count)
        {
            if (output_set->keep_count)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS, "Multiple create generators set keep_count for {} (key '{}')", output_set->name, key);
            output_set->keep_count = keep_count;
        }
    }
}

std::string CreateRequestGenerator::descriptionImpl()
{
    std::string data_string
        = data.has_value() ? fmt::format("data for created nodes: {}", data->description()) : "no data for created nodes";
    std::string remove_factor_string = "- without removes";
    if (remove_factor.has_value())
        remove_factor_string = fmt::format("- remove factor: {}", *remove_factor);
    else if (keep_count.has_value())
        remove_factor_string = *keep_count == 0 ? "- keep node count: auto" : fmt::format("- keep node count: {}", *keep_count);
    std::string output_string
        = output_set && output_set->used_as_input ? fmt::format("\n- created nodes tracked in: {}", output_set->name) : "";
    return fmt::format(
        "Create Request Generator\n"
        "- parent path(s) for created nodes: {}\n"
        "- name for created nodes: {}\n"
        "- {}\n"
        "{}{}",
        parent_path.description(),
        name.description(),
        data_string,
        remove_factor_string,
        output_string);
}

ZooKeeperRequestWithCallbacks CreateRequestGenerator::generateImpl(GenerateContext & ctx, const Coordination::ACLs & acls)
{
    /// The created/removed paths are recorded in the output set only if some
    /// generator (possibly this one) reads it.
    bool tracked = output_set && output_set->used_as_input;

    bool do_remove = false;
    if (tracked && remove_factor.has_value())
    {
        do_remove = std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *remove_factor;
    }
    else if (tracked && keep_count.has_value())
    {
        /// Choose between Create and Remove so the shard size hovers around the
        /// target: remove probability is 0.5 at the target and approaches 0/1 as
        /// the size deviates. Never remove the last path (so the set can only be
        /// empty if it started empty).
        size_t size = output_set->shardSize(ctx.thread_idx);
        if (size > 1)
        {
            double target = static_cast<double>(output_set->target_count_per_shard);
            double scale = std::max(1.0, target * 0.05);
            double remove_probability = 1.0 / (1.0 + std::exp((target - static_cast<double>(size)) / scale));
            do_remove = std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < remove_probability;
        }
    }

    if (do_remove)
    {
        if (auto taken = output_set->takeRandom(ctx.rng, ctx.thread_idx))
        {
            auto request = std::make_shared<ZooKeeperRemoveRequest>();
            request->path = *taken;

            auto callback = [set = output_set, path = *std::move(taken), thread_idx = ctx.thread_idx](const Coordination::Response * response) mutable
            {
                /// ZOK: removed. ZNONODE: was already gone. Anything else (including
                /// no response at all): the node may still exist, put it back.
                if (response && (response->error == Coordination::Error::ZOK || response->error == Coordination::Error::ZNONODE))
                    return;
                set->add(std::move(path), thread_idx);
            };

            return {.request = request, .callback = std::move(callback), .ignore_missing_nodes = true};
        }
        /// The shard is empty, nothing to remove: fall through to create.
    }

    auto parent = parent_path.getPath(ctx);
    if (!parent)
        return {};

    auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->acls = acls;
    request->path = std::filesystem::path(*parent) / name.getString(ctx.rng);

    if (data)
        request->data = data->getString(ctx.rng);

    ZooKeeperRequestWithCallbacks result{.request = request};
    result.ignore_missing_nodes = parent_path.isDynamic();
    if (tracked)
    {
        result.callback = [set = output_set, path = request->path, thread_idx = ctx.thread_idx](const Coordination::Response * response) mutable
        {
            if (response && response->error == Coordination::Error::ZOK)
                set->add(std::move(path), thread_idx);
        };
    }

    return result;
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
    auto target = path.getPath(ctx);
    if (!target)
        return {};

    auto request = std::make_shared<ZooKeeperSetRequest>();
    request->path = *std::move(target);
    request->data = data.getString(ctx.rng);
    return {.request = request, .ignore_missing_nodes = path.isDynamic()};
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
    auto target = path.getPath(ctx);
    if (!target)
        return {};

    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = *std::move(target);
    if (watch_probability.has_value() && std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *watch_probability)
    {
        request->has_watch = true;
        request->watch_callback = watch_callback_ptr;
    }
    return {.request = request, .ignore_missing_nodes = path.isDynamic()};
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
    auto target = path.getPath(ctx);
    if (!target)
        return {};

    auto request = std::make_shared<ZooKeeperListRequest>();
    request->path = *std::move(target);
    request->list_request_type = ListRequestType::ALL;
    if (watch_probability.has_value() && std::uniform_real_distribution<double>(0, 1.0)(ctx.rng) < *watch_probability)
    {
        request->has_watch = true;
        request->watch_callback = watch_callback_ptr;
    }
    return {.request = request, .ignore_missing_nodes = path.isDynamic()};
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
    bool ignore_missing_nodes = false;

    auto add_op = [&](ZooKeeperRequestWithCallbacks request_with_callbacks)
    {
        /// Sub-generators may decline (empty dynamic path set); skip them.
        if (!request_with_callbacks.request)
            return;
        ops.push_back(std::move(request_with_callbacks.request));
        inner_callbacks.push_back(std::move(request_with_callbacks.callback));
        ignore_missing_nodes |= request_with_callbacks.ignore_missing_nodes;
    };

    if (size)
    {
        auto request_count = size->getNumber(ctx.rng);

        for (size_t i = 0; i < request_count; ++i)
            add_op(request_getter.generate(ctx, acls));
    }
    else
    {
        for (const auto & request_generator : request_getter.requestGenerators())
            add_op(request_generator->generate(ctx, acls));
    }

    if (ops.empty())
        return {};

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
        .ignore_missing_nodes = ignore_missing_nodes,
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
    auto result = request_getter.generate(ctx, default_acls);
    if (!result.request)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "All request generators declined to produce a request (are all the dynamic path sets empty, with nothing creating nodes?)");
    return result;
}
