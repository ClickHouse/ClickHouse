#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TypeResolver.h>
#include <DataTypes/UDT/Catalog.h>

#include <Parsers/ASTDataType.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

constexpr UInt64 scale_definition_count = 100'000;
constexpr UInt64 retained_catalog_definition_count = 10'000;
constexpr UInt64 retained_publication_count = 10'000;
constexpr UInt64 normal_reader_count = 8;
constexpr UInt64 normal_publication_count = 256;
constexpr UInt64 acceptance_reader_count = 256;
constexpr UInt64 acceptance_publication_count = 10'000;

constexpr std::string_view acceptance_mode_environment = "CLICKHOUSE_UDT_CONCURRENCY_ACCEPTANCE";
constexpr std::string_view reader_count_environment = "CLICKHOUSE_UDT_CONCURRENCY_READERS";
constexpr std::string_view publication_count_environment = "CLICKHOUSE_UDT_CONCURRENCY_PUBLICATIONS";

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view description)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        throw std::overflow_error(String(description));
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view description)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        throw std::overflow_error(String(description));
    return lhs * rhs;
}

std::size_t checkedSize(UInt64 value, std::string_view description)
{
    if (!std::in_range<std::size_t>(value))
        throw std::overflow_error(String(description));
    return static_cast<std::size_t>(value);
}

UInt64 elapsedNanoseconds(std::chrono::steady_clock::time_point begin, std::chrono::steady_clock::time_point end)
{
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
    if (elapsed < 0 || !std::in_range<UInt64>(elapsed))
        throw std::overflow_error("elapsed time does not fit UInt64 nanoseconds");
    return static_cast<UInt64>(elapsed);
}

void record(std::string_view name, UInt64 value)
{
    testing::Test::RecordProperty(String(name), value);
}

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

const UUID scale_database_uuid = testUUID(0x5544545343414c45ULL, 0x2d44423030303031ULL);

DefinitionIdentity scaleIdentity(UInt64 ordinal)
{
    return {
        .database_uuid = scale_database_uuid,
        .type_uuid = testUUID(0x554454545950452dULL, checkedAdd(ordinal, 1, "scale identity ordinal overflow")),
        .revision = 1,
    };
}

String scaleLocalName(UInt64 ordinal)
{
    std::array<char, 20> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), ordinal);
    if (error != std::errc{})
        throw std::runtime_error("cannot format scale definition ordinal");
    const std::size_t digits = static_cast<std::size_t>(end - buffer.data());
    constexpr std::size_t minimum_digits = 6;
    if (digits > minimum_digits)
        throw std::overflow_error("scale definition ordinal exceeds the fixed name domain");

    String result = "ResolverCatalogType";
    result.append(minimum_digits - digits, '0');
    result.append(buffer.data(), digits);
    return result;
}

DefinitionInput scaleInput(UInt64 ordinal, std::string_view qualifier = "resolver_catalog")
{
    DefinitionInput input;
    input.identity = scaleIdentity(ordinal);
    input.normalized_local_name = scaleLocalName(ordinal);
    input.normalized_name = String(qualifier) + "." + input.normalized_local_name;
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return input;
}

std::vector<DefinitionInput> scaleInputs(UInt64 count)
{
    std::vector<DefinitionInput> inputs;
    inputs.reserve(checkedSize(count, "scale definition count does not fit size_t"));
    for (UInt64 ordinal = 0; ordinal < count; ++ordinal)
        inputs.push_back(scaleInput(ordinal));
    return inputs;
}

TemplateCheckerLimits scaleCheckerLimits(UInt64 count)
{
    TemplateCheckerLimits limits;
    limits.maximum_definitions = count;
    limits.maximum_catalog_input_bytes
        = std::max(limits.maximum_catalog_input_bytes, checkedMultiply(count, 1'024, "checker input budget overflow"));
    limits.maximum_catalog_checker_work
        = std::max(limits.maximum_catalog_checker_work, checkedMultiply(count, 256, "checker work budget overflow"));
    limits.maximum_canonical_catalog_bytes
        = std::max(limits.maximum_canonical_catalog_bytes, checkedMultiply(count, 2'048, "checker canonical budget overflow"));
    limits.maximum_scratch_bytes = std::max(limits.maximum_scratch_bytes, checkedMultiply(count, 4'096, "checker scratch budget overflow"));
    return limits;
}

TypeCatalogBuildLimits scaleCatalogLimits(UInt64 count)
{
    TypeCatalogBuildLimits limits;
    limits.shard_count = 64;
    limits.maximum_definitions = count;
    return limits;
}

Definition::Ptr checkedVariant(UInt64 ordinal, std::string_view qualifier)
{
    auto definitions = TemplateChecker::checkAll({scaleInput(ordinal, qualifier)});
    if (definitions.size() != 1)
        throw std::logic_error("single-definition check returned an unexpected result count");
    return definitions.front();
}

void verifyLookup(const TypeCatalogRoot & root, const std::vector<Definition::Ptr> & definitions, UInt64 ordinal)
{
    const auto index = checkedSize(ordinal, "lookup ordinal does not fit size_t");
    ASSERT_LT(index, definitions.size());
    const auto & expected = definitions[index];
    ASSERT_TRUE(expected);
    EXPECT_EQ(root.findByIdentity(scaleIdentity(ordinal)), expected);
    EXPECT_EQ(root.findByName(scaleLocalName(ordinal)), expected);
}

void runBulkScaleCase(UInt64 count)
{
    ASSERT_GT(count, 0);
    ASSERT_LE(count, scale_definition_count);

    const auto input_begin = std::chrono::steady_clock::now();
    auto inputs = scaleInputs(count);
    const auto input_end = std::chrono::steady_clock::now();
    TemplateCheckerStatistics checker_statistics;
    const auto checker_begin = std::chrono::steady_clock::now();
    auto definitions = TemplateChecker::checkAll(std::move(inputs), scaleCheckerLimits(count), &checker_statistics);
    const auto checker_end = std::chrono::steady_clock::now();

    ASSERT_EQ(definitions.size(), checkedSize(count, "checked definition count does not fit size_t"));
    EXPECT_EQ(checker_statistics.checked_definitions, count);
    EXPECT_EQ(checker_statistics.graph_edges, 0);
    EXPECT_GT(checker_statistics.accepted_input_bytes, 0);
    EXPECT_GT(checker_statistics.maximum_definition_input_bytes, 0);
    EXPECT_GT(checker_statistics.charged_work, 0);
    EXPECT_GT(checker_statistics.canonical_bytes, 0);
    EXPECT_GT(checker_statistics.scratch_peak_bytes, 0);

    const auto catalog_limits = scaleCatalogLimits(count);
    const auto build_begin = std::chrono::steady_clock::now();
    auto root = TypeCatalogBuilder::build(1, definitions, catalog_limits);
    const auto build_end = std::chrono::steady_clock::now();

    ASSERT_EQ(root->getDefinitionCount(), count);
    ASSERT_EQ(root->getShardCount(), catalog_limits.shard_count);
    EXPECT_EQ(root->getDatabaseUUID(), scale_database_uuid);
    EXPECT_GT(root->getAccountedBytes(), 0);
    verifyLookup(*root, definitions, 0);
    if (count > 2)
        verifyLookup(*root, definitions, count / 2);
    if (count > 1)
        verifyLookup(*root, definitions, count - 1);
    EXPECT_FALSE(root->findByIdentity(scaleIdentity(count)));
    EXPECT_FALSE(root->findByName(scaleLocalName(count)));

    UInt64 identity_entries = 0;
    UInt64 name_entries = 0;
    UInt64 identity_bytes = 0;
    UInt64 name_bytes = 0;
    UInt64 maximum_identity_shard_entries = 0;
    UInt64 maximum_name_shard_entries = 0;
    UInt64 maximum_identity_shard_bytes = 0;
    UInt64 maximum_name_shard_bytes = 0;
    for (std::size_t shard = 0; shard < root->getShardCount(); ++shard)
    {
        const auto identity = root->getIdentityShardAccounting(shard);
        const auto name = root->getNameShardAccounting(shard);
        identity_entries = checkedAdd(identity_entries, identity.entries, "identity entry accounting overflow");
        name_entries = checkedAdd(name_entries, name.entries, "name entry accounting overflow");
        identity_bytes = checkedAdd(identity_bytes, identity.accounted_bytes, "identity byte accounting overflow");
        name_bytes = checkedAdd(name_bytes, name.accounted_bytes, "name byte accounting overflow");
        maximum_identity_shard_entries = std::max(maximum_identity_shard_entries, identity.entries);
        maximum_name_shard_entries = std::max(maximum_name_shard_entries, name.entries);
        maximum_identity_shard_bytes = std::max(maximum_identity_shard_bytes, identity.accounted_bytes);
        maximum_name_shard_bytes = std::max(maximum_name_shard_bytes, name.accounted_bytes);
    }
    EXPECT_EQ(identity_entries, count);
    EXPECT_EQ(name_entries, count);
    EXPECT_GT(maximum_identity_shard_entries, 0);
    EXPECT_GT(maximum_name_shard_entries, 0);
    EXPECT_EQ(root->getMaximumIdentityShardEntries(), catalog_limits.maximum_identity_shard_entries);
    EXPECT_EQ(root->getMaximumNameShardEntries(), catalog_limits.maximum_name_shard_entries);
    EXPECT_EQ(root->getMaximumShardAccountedBytes(), catalog_limits.maximum_shard_accounted_bytes);
    EXPECT_LE(maximum_identity_shard_entries, root->getMaximumIdentityShardEntries());
    EXPECT_LE(maximum_name_shard_entries, root->getMaximumNameShardEntries());
    EXPECT_LE(maximum_identity_shard_bytes, root->getMaximumShardAccountedBytes());
    EXPECT_LE(maximum_name_shard_bytes, root->getMaximumShardAccountedBytes());
    EXPECT_LE(checkedAdd(identity_bytes, name_bytes, "combined shard byte accounting overflow"), root->getAccountedBytes());

    auto permuted_definitions = definitions;
    std::reverse(permuted_definitions.begin(), permuted_definitions.end());
    if (permuted_definitions.size() > 3)
    {
        const auto rotation = permuted_definitions.begin() + static_cast<std::ptrdiff_t>(permuted_definitions.size() / 3);
        std::rotate(permuted_definitions.begin(), rotation, permuted_definitions.end());
    }
    const auto permuted_build_begin = std::chrono::steady_clock::now();
    auto permuted_root = TypeCatalogBuilder::build(1, permuted_definitions, catalog_limits);
    const auto permuted_build_end = std::chrono::steady_clock::now();
    ASSERT_EQ(permuted_root->getDefinitionCount(), root->getDefinitionCount());
    EXPECT_EQ(permuted_root->getDatabaseUUID(), root->getDatabaseUUID());
    EXPECT_EQ(permuted_root->getGeneration(), root->getGeneration());
    EXPECT_EQ(permuted_root->getShardCount(), root->getShardCount());
    EXPECT_EQ(permuted_root->getShardHashKey0(), root->getShardHashKey0());
    EXPECT_EQ(permuted_root->getShardHashKey1(), root->getShardHashKey1());
    EXPECT_EQ(permuted_root->getAccountedBytes(), root->getAccountedBytes());
    verifyLookup(*permuted_root, definitions, 0);
    if (count > 2)
        verifyLookup(*permuted_root, definitions, count / 2);
    if (count > 1)
        verifyLookup(*permuted_root, definitions, count - 1);
    for (UInt64 ordinal = 0; ordinal < count; ++ordinal)
    {
        const auto & expected = definitions[checkedSize(ordinal, "permuted lookup ordinal does not fit size_t")];
        if (permuted_root->findByIdentity(expected->getIdentity()) != expected
            || permuted_root->findByName(expected->getNormalizedLocalName()) != expected)
        {
            ADD_FAILURE() << "permuted catalog changed the lookup result at definition ordinal " << ordinal;
            break;
        }
    }
    for (std::size_t shard = 0; shard < root->getShardCount(); ++shard)
    {
        EXPECT_EQ(permuted_root->getIdentityShardAccounting(shard), root->getIdentityShardAccounting(shard));
        EXPECT_EQ(permuted_root->getNameShardAccounting(shard), root->getNameShardAccounting(shard));
    }
    permuted_root.reset();
    permuted_definitions.clear();

    const auto mutation_begin = std::chrono::steady_clock::now();
    auto mutated
        = TypeCatalogBuilder::applyMutation(*root, 2, TypeCatalogMutation::remove(definitions.front()->getIdentity()), catalog_limits);
    const auto mutation_end = std::chrono::steady_clock::now();
    EXPECT_EQ(mutated->getDefinitionCount(), count - 1);
    EXPECT_FALSE(mutated->findByIdentity(definitions.front()->getIdentity()));
    EXPECT_FALSE(mutated->findByName(definitions.front()->getNormalizedLocalName()));
    EXPECT_EQ(root->findByIdentity(definitions.front()->getIdentity()), definitions.front());
    EXPECT_LT(mutated->getAccountedBytes(), root->getAccountedBytes());

    UInt64 copied_identity_shards = 0;
    UInt64 copied_name_shards = 0;
    for (std::size_t shard = 0; shard < root->getShardCount(); ++shard)
    {
        copied_identity_shards = checkedAdd(
            copied_identity_shards, root->sharesIdentityShardWith(*mutated, shard) ? 0 : 1, "identity shard-copy count overflow");
        copied_name_shards
            = checkedAdd(copied_name_shards, root->sharesNameShardWith(*mutated, shard) ? 0 : 1, "name shard-copy count overflow");
    }
    EXPECT_EQ(copied_identity_shards, 1);
    EXPECT_EQ(copied_name_shards, 1);

    record("definition_count", count);
    record("input_generation_elapsed_ns", elapsedNanoseconds(input_begin, input_end));
    record("checker_elapsed_ns", elapsedNanoseconds(checker_begin, checker_end));
    record("checker_accepted_input_bytes", checker_statistics.accepted_input_bytes);
    record("checker_charged_work", checker_statistics.charged_work);
    record("checker_canonical_bytes", checker_statistics.canonical_bytes);
    record("checker_scratch_peak_bytes", checker_statistics.scratch_peak_bytes);
    record("catalog_build_elapsed_ns", elapsedNanoseconds(build_begin, build_end));
    record("catalog_permuted_build_elapsed_ns", elapsedNanoseconds(permuted_build_begin, permuted_build_end));
    record("catalog_accounted_bytes", root->getAccountedBytes());
    record("identity_shard_max_entries", maximum_identity_shard_entries);
    record("name_shard_max_entries", maximum_name_shard_entries);
    record("identity_shard_max_accounted_bytes", maximum_identity_shard_bytes);
    record("name_shard_max_accounted_bytes", maximum_name_shard_bytes);
    record("mutation_elapsed_ns", elapsedNanoseconds(mutation_begin, mutation_end));
    record("mutation_copied_identity_shards", copied_identity_shards);
    record("mutation_copied_name_shards", copied_name_shards);
}

TypeAuthorityCapabilities scaleCapabilities(UInt64 definition_count)
{
    TypeAuthorityCapabilities capabilities;
    capabilities.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    capabilities.limits = {
        .maximum_definitions = definition_count,
        .maximum_definition_bytes = 1ULL << 20,
        .maximum_template_nodes = 4'096,
        .maximum_direct_dependencies = 256,
        .maximum_transitive_dependencies = 1'024,
        .maximum_checker_work = 65'536,
    };
    return capabilities;
}

struct AuthorityCounterSnapshot
{
    UInt64 capability_calls = 0;
    UInt64 database_calls = 0;
    UInt64 session_calls = 0;
    UInt64 require_calls = 0;

    bool operator==(const AuthorityCounterSnapshot &) const = default;
};

class PublishingTestAuthority final : public IAuthorityAdapter
{
public:
    PublishingTestAuthority(
        TypeCatalogRoot::Ptr initial_root, TypeAuthorityCapabilities capabilities_, const TypeCatalogPublicationLimits & publication_limits)
        : capabilities(std::move(capabilities_))
        , state(std::make_shared<State>(std::move(initial_root), publication_limits))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override
    {
        capability_calls.fetch_add(1, std::memory_order_relaxed);
        return capabilities;
    }

    UUID getDatabaseUUID() const noexcept override
    {
        database_calls.fetch_add(1, std::memory_order_relaxed);
        return scale_database_uuid;
    }

    ResolutionSession beginResolutionSession() const override
    {
        session_calls.fetch_add(1, std::memory_order_relaxed);
        auto session = state->catalog.beginResolutionSession();
        return makeResolutionSession(state, std::move(session));
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        require_calls.fetch_add(1, std::memory_order_relaxed);
        if (!capabilities.containsAll(required))
            throw std::runtime_error("test authority lacks capabilities required for " + String(operation));
    }

    void publish(TypeCatalogRoot::Ptr root) { state->catalog.publish(std::move(root)); }
    TypeCatalogRetirementState scanRetired() { return state->catalog.scanRetired(); }
    TypeCatalogRetirementState getRetirementState() const { return state->catalog.getRetirementState(); }

    Definition::Ptr findCurrentByIdentity(const DefinitionIdentity & identity) const
    {
        return state->catalog.findByIdentity(identity);
    }

    Definition::Ptr findCurrentByName(std::string_view normalized_local_name) const
    {
        return state->catalog.findByName(normalized_local_name);
    }

    UInt64 currentGeneration() const noexcept { return state->catalog.currentGeneration(); }

    AuthorityCounterSnapshot counters() const noexcept
    {
        return {
            .capability_calls = capability_calls.load(std::memory_order_relaxed),
            .database_calls = database_calls.load(std::memory_order_relaxed),
            .session_calls = session_calls.load(std::memory_order_relaxed),
            .require_calls = require_calls.load(std::memory_order_relaxed),
        };
    }

private:
    struct State
    {
        State(TypeCatalogRoot::Ptr root, const TypeCatalogPublicationLimits & limits)
            : catalog(std::move(root), limits)
        {
        }

        Catalog catalog;
    };

    const TypeAuthorityCapabilities capabilities;
    std::shared_ptr<State> state;
    mutable std::atomic<UInt64> capability_calls{0};
    mutable std::atomic<UInt64> database_calls{0};
    mutable std::atomic<UInt64> session_calls{0};
    mutable std::atomic<UInt64> require_calls{0};
};

boost::intrusive_ptr<ASTDataType> marker(String name)
{
    auto result = make_intrusive<ASTDataType>();
    result->name = std::move(name);
    return result;
}

CanonicalTypeArguments noArguments()
{
    return CanonicalTypeArguments::validate({}, {});
}

UInt64 parseBoundedEnvironment(std::string_view name, UInt64 fallback, UInt64 minimum, UInt64 maximum)
{
    const String environment_name(name);
    const char * raw = std::getenv(environment_name.c_str());
    if (!raw)
        return fallback;

    const std::string_view value(raw);
    if (value.empty())
        throw std::invalid_argument(environment_name + " must be a nonempty unsigned decimal integer");
    UInt64 parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed, 10);
    if (error != std::errc{} || end != value.data() + value.size())
        throw std::invalid_argument(environment_name + " must be an unsigned decimal integer with no whitespace or suffix");
    if (parsed < minimum || parsed > maximum)
        throw std::out_of_range(environment_name + " is outside its supported bounds");
    return parsed;
}

struct ConcurrencyConfiguration
{
    bool acceptance_mode = false;
    UInt64 readers = 0;
    UInt64 publications = 0;
};

ConcurrencyConfiguration concurrencyConfiguration()
{
    const bool acceptance_mode = parseBoundedEnvironment(acceptance_mode_environment, 0, 0, 1) == 1;
    return {
        .acceptance_mode = acceptance_mode,
        .readers = parseBoundedEnvironment(
            reader_count_environment,
            acceptance_mode ? acceptance_reader_count : normal_reader_count,
            acceptance_mode ? acceptance_reader_count : 1,
            512),
        .publications = parseBoundedEnvironment(
            publication_count_environment,
            acceptance_mode ? acceptance_publication_count : normal_publication_count,
            acceptance_mode ? acceptance_publication_count : 2,
            100'000),
    };
}

void setFirstFailure(std::atomic<UInt64> & failure, UInt64 code) noexcept
{
    UInt64 expected = 0;
    static_cast<void>(failure.compare_exchange_strong(expected, code, std::memory_order_relaxed));
}

}

TEST(UDTScale, BulkBuildOneDefinition)
{
    runBulkScaleCase(1);
}

TEST(UDTScale, BulkBuildTenThousandDefinitions)
{
    runBulkScaleCase(10'000);
}

TEST(UDTScale, BulkBuildOneHundredThousandDefinitions)
{
    runBulkScaleCase(scale_definition_count);
}

TEST(UDTScale, BoundResultRetainsOneDefinitionAcrossTenThousandPublications)
{
    TemplateCheckerStatistics checker_statistics;
    auto definitions = TemplateChecker::checkAll(
        scaleInputs(retained_catalog_definition_count), scaleCheckerLimits(retained_catalog_definition_count), &checker_statistics);
    ASSERT_EQ(definitions.size(), retained_catalog_definition_count);
    auto initial_definition = definitions.front();
    const auto retained_identity = initial_definition->getIdentity();
    const String retained_name = initial_definition->getNormalizedName();
    std::weak_ptr<const Definition> retained_weak = initial_definition;
    std::weak_ptr<const Definition> undemanded_weak = definitions.back();
    ASSERT_NE(definitions.back()->getIdentity(), retained_identity);

    const auto catalog_limits = scaleCatalogLimits(retained_catalog_definition_count);
    auto initial_root = TypeCatalogBuilder::build(1, definitions, catalog_limits);
    const UInt64 initial_root_bytes = initial_root->getAccountedBytes();
    const auto variant_b = checkedVariant(0, "variantb");
    const auto variant_c = checkedVariant(0, "variantc");
    ASSERT_TRUE(initial_definition->hasSameCheckedSemantics(*variant_b));
    ASSERT_TRUE(initial_definition->hasSameCheckedSemantics(*variant_c));
    auto pending
        = TypeCatalogBuilder::applyMutation(*initial_root, 2, TypeCatalogMutation::replace(retained_identity, variant_b), catalog_limits);

    TypeCatalogPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 8;
    publication_limits.maximum_retired_root_count = 1;
    publication_limits.maximum_retired_root_bytes = 1ULL << 30;

    BoundDeclaredTypeTree::Ptr retained_tree;
    DataTypePtr retained_physical_type;
    TypeResolverStatistics resolver_statistics;
    AuthorityCounterSnapshot counters_after_binding;
    UInt64 completed_publications = 0;
    UInt64 ordinary_access_checksum = 0;
    UInt64 root_preparation_elapsed_ns = 0;
    UInt64 root_publication_elapsed_ns = 0;
    UInt64 retirement_scan_elapsed_ns = 0;
    std::chrono::steady_clock::time_point campaign_begin;
    std::chrono::steady_clock::time_point campaign_end;
    {
        PublishingTestAuthority authority(
            std::move(initial_root), scaleCapabilities(retained_catalog_definition_count), publication_limits);

        ASTPtr declaration = marker(retained_name);
        const std::array references{
            DeclaredTypeReferenceInput{
                .reference_node = declaration.get(),
                .definition_identity = retained_identity,
                .canonical_arguments = noArguments(),
                .type_argument_lineage = {},
            },
        };
        const auto bound = TypeResolver::resolve(declaration, references, authority, {}, &resolver_statistics);
        ASSERT_TRUE(bound.hasLogicalTree());
        retained_tree = bound.getLogicalTree();
        retained_physical_type = bound.getPhysicalType();
        ASSERT_EQ(retained_tree->getDefinitionHandles().size(), 1);
        ASSERT_EQ(retained_tree->getDescriptors().size(), 1);
        ASSERT_EQ(retained_tree->getDefinitionHandles().front(), initial_definition);
        ASSERT_EQ(retained_tree->getDescriptors().front()->getDefinition(), initial_definition);
        EXPECT_EQ(resolver_statistics.specializer.resolution_sessions, 1);
        EXPECT_EQ(resolver_statistics.specializer.definition_lookups, 1);

        counters_after_binding = authority.counters();
        const auto statistics_after_binding = resolver_statistics;
        definitions.clear();
        definitions.shrink_to_fit();
        initial_definition.reset();

        bool pending_is_b = true;
        campaign_begin = std::chrono::steady_clock::now();
        for (UInt64 publication = 0; publication < retained_publication_count; ++publication)
        {
            const UInt64 generation = checkedAdd(publication, 2, "retained publication generation overflow");
            TypeCatalogRoot::Ptr following;
            if (publication + 1 < retained_publication_count)
            {
                const auto & following_definition = pending_is_b ? variant_c : variant_b;
                const auto prepare_begin = std::chrono::steady_clock::now();
                following = TypeCatalogBuilder::applyMutation(
                    *pending,
                    checkedAdd(generation, 1, "retained publication generation overflow"),
                    TypeCatalogMutation::replace(retained_identity, following_definition),
                    catalog_limits);
                root_preparation_elapsed_ns = checkedAdd(
                    root_preparation_elapsed_ns,
                    elapsedNanoseconds(prepare_begin, std::chrono::steady_clock::now()),
                    "retained root-preparation timing overflow");
            }

            const auto publish_begin = std::chrono::steady_clock::now();
            authority.publish(std::move(pending));
            root_publication_elapsed_ns = checkedAdd(
                root_publication_elapsed_ns,
                elapsedNanoseconds(publish_begin, std::chrono::steady_clock::now()),
                "retained publication timing overflow");
            const auto queued = authority.getRetirementState();
            if (queued.retired_root_count != 1 || queued.active_hazard_slots != 0)
            {
                ADD_FAILURE() << "publication " << publication << " retained an unexpected catalog state";
                break;
            }
            const auto scan_begin = std::chrono::steady_clock::now();
            const auto drained = authority.scanRetired();
            retirement_scan_elapsed_ns = checkedAdd(
                retirement_scan_elapsed_ns,
                elapsedNanoseconds(scan_begin, std::chrono::steady_clock::now()),
                "retirement-scan timing overflow");
            if (drained.retired_root_count != 0 || drained.retired_root_bytes != 0 || drained.active_hazard_slots != 0)
            {
                ADD_FAILURE() << "publication " << publication << " did not reclaim the unpinned old root";
                break;
            }

            const auto & retained_handles = retained_tree->getDefinitionHandles();
            const auto & descriptors = retained_tree->getDescriptors();
            if (retained_handles.size() != 1 || descriptors.size() != 1 || retained_handles.front()->getIdentity() != retained_identity
                || descriptors.front()->getDefinition() != retained_handles.front())
            {
                ADD_FAILURE() << "ordinary bound-result access changed after publication " << publication;
                break;
            }
            ordinary_access_checksum = checkedAdd(
                ordinary_access_checksum,
                checkedAdd(
                    retained_handles.front()->getIdentity().revision,
                    checkedAdd(retained_tree->getNodeCount(), retained_tree->getOccurrenceCount(), "ordinary access checksum overflow"),
                    "ordinary access checksum overflow"),
                "ordinary access checksum overflow");

            ++completed_publications;
            pending = std::move(following);
            pending_is_b = !pending_is_b;
        }
        campaign_end = std::chrono::steady_clock::now();

        EXPECT_EQ(completed_publications, retained_publication_count);
        EXPECT_GT(ordinary_access_checksum, 0);
        EXPECT_EQ(authority.counters(), counters_after_binding);
        EXPECT_EQ(resolver_statistics, statistics_after_binding);
        EXPECT_EQ(authority.findCurrentByIdentity(retained_identity), variant_c);
        EXPECT_FALSE(undemanded_weak.expired());
    }

    ASSERT_TRUE(retained_tree);
    ASSERT_TRUE(retained_physical_type);
    EXPECT_TRUE(undemanded_weak.expired());
    ASSERT_FALSE(retained_weak.expired());
    ASSERT_EQ(retained_tree->getDefinitionHandles().size(), 1);
    ASSERT_EQ(retained_tree->getDescriptors().size(), 1);
    EXPECT_EQ(retained_weak.lock(), retained_tree->getDefinitionHandles().front());
    EXPECT_EQ(retained_tree->getDescriptors().front()->getDefinition(), retained_tree->getDefinitionHandles().front());
    EXPECT_EQ(retained_tree->getPhysicalType(), retained_physical_type);
    EXPECT_EQ(retained_tree->getNodeCount(), 1);
    EXPECT_EQ(retained_tree->getOccurrenceCount(), 1);

    record("catalog_definition_count", retained_catalog_definition_count);
    record("publication_count", retained_publication_count);
    record("initial_root_accounted_bytes", initial_root_bytes);
    record("retained_definition_handles", retained_tree->getDefinitionHandles().size());
    record("retained_descriptors", retained_tree->getDescriptors().size());
    record("ordinary_access_observations", completed_publications);
    record("publication_campaign_elapsed_ns", elapsedNanoseconds(campaign_begin, campaign_end));
    record("root_preparation_elapsed_ns", root_preparation_elapsed_ns);
    record("root_publication_elapsed_ns", root_publication_elapsed_ns);
    record("retirement_scan_elapsed_ns", retirement_scan_elapsed_ns);
    record("binding_checker_charged_work", checker_statistics.charged_work);
    record("binding_resolver_charged_work", resolver_statistics.specializer.charged_work);
}

TEST(UDTConcurrency, ConcurrentBindingAndLookupRemainSnapshotConsistentAcrossPublications)
{
    const auto configuration = concurrencyConfiguration();
    const auto variant_a = checkedVariant(0, "concurrencya");
    const auto variant_b = checkedVariant(0, "concurrencyb");
    const auto variant_c = checkedVariant(0, "concurrencyc");
    ASSERT_TRUE(variant_a->hasSameCheckedSemantics(*variant_b));
    ASSERT_TRUE(variant_a->hasSameCheckedSemantics(*variant_c));
    const auto identity = variant_a->getIdentity();
    const String local_name = variant_a->getNormalizedLocalName();

    const auto build_limits = scaleCatalogLimits(1);
    const std::array initial_definitions{variant_a};
    const std::array probe_definitions{variant_c};
    auto initial_root = TypeCatalogBuilder::build(1, initial_definitions, build_limits);
    auto pending = TypeCatalogBuilder::applyMutation(*initial_root, 2, TypeCatalogMutation::replace(identity, variant_b), build_limits);
    const auto probe_root = TypeCatalogBuilder::build(3, probe_definitions, build_limits);
    const UInt64 maximum_root_bytes
        = std::max({initial_root->getAccountedBytes(), pending->getAccountedBytes(), probe_root->getAccountedBytes()});

    TypeCatalogPublicationLimits publication_limits;
    publication_limits.hazard_slot_count
        = checkedSize(checkedAdd(configuration.readers, 4, "hazard-slot count overflow"), "hazard-slot count does not fit size_t");
    publication_limits.maximum_retired_root_count = configuration.publications;
    publication_limits.maximum_retired_root_bytes
        = checkedMultiply(maximum_root_bytes, configuration.publications, "retirement byte budget overflow");
    PublishingTestAuthority authority(std::move(initial_root), scaleCapabilities(1), publication_limits);

    std::atomic<bool> start{false};
    std::atomic<UInt64> ready{0};
    std::atomic<UInt64> initial_attempts{0};
    std::atomic<UInt64> failure_code{0};
    const auto reader_count = checkedSize(configuration.readers, "reader count does not fit size_t");
    auto latest_generation = std::make_unique<std::atomic<UInt64>[]>(reader_count);
    for (std::size_t reader = 0; reader < reader_count; ++reader)
        latest_generation[reader].store(0, std::memory_order_relaxed);
    std::vector<UInt64> reader_lookup_successes(reader_count, 0);
    std::vector<UInt64> reader_binding_successes(reader_count, 0);
    std::vector<UInt8> reader_generation_masks(reader_count, 0);
    std::vector<ASTPtr> reader_declarations;
    std::vector<DeclaredTypeReferenceInput> reader_references;
    reader_declarations.reserve(reader_count);
    reader_references.reserve(reader_count);
    for (std::size_t reader = 0; reader < reader_count; ++reader)
    {
        ASTPtr declaration = marker("resolver_catalog.ConcurrentLogical");
        reader_references.push_back({
            .reference_node = declaration.get(),
            .definition_identity = identity,
            .canonical_arguments = noArguments(),
            .type_argument_lineage = {},
        });
        reader_declarations.push_back(std::move(declaration));
    }
    const auto expectedForGeneration = [&](UInt64 generation) -> const Definition::Ptr &
    {
        if (generation == 1)
            return variant_a;
        return generation % 2 == 0 ? variant_b : variant_c;
    };
    std::vector<std::jthread> readers;
    readers.reserve(reader_count);

    for (std::size_t reader = 0; reader < reader_count; ++reader)
    {
        readers.emplace_back(
            [&, reader](std::stop_token stop_token)
            {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire) && !stop_token.stop_requested())
                    std::this_thread::yield();
                if (stop_token.stop_requested())
                    return;

                bool first_attempt = true;
                UInt64 lookup_successes = 0;
                UInt64 binding_successes = 0;
                UInt8 generation_mask = 0;
                do
                {
                    try
                    {
                        UInt64 observed_generation = 0;
                        {
                            const auto session = authority.beginResolutionSession();
                            const UInt64 generation = session.getGeneration();
                            observed_generation = generation;
                            const auto by_identity = session.findByIdentity(identity);
                            const auto by_name = session.findByName(local_name);
                            const auto & expected = expectedForGeneration(generation);
                            if (generation == 0 || !by_identity || by_identity != by_name || by_identity != expected
                                || by_identity->getIdentity() != identity)
                            {
                                setFirstFailure(failure_code, 1);
                            }
                            else if (lookup_successes == std::numeric_limits<UInt64>::max())
                            {
                                setFirstFailure(failure_code, 2);
                            }
                            else
                            {
                                ++lookup_successes;
                                generation_mask |= generation == 1 ? UInt8{1} : (generation % 2 == 0 ? UInt8{2} : UInt8{4});
                            }
                        }

                        TypeResolverStatistics statistics;
                        const auto bound = TypeResolver::resolve(
                            reader_declarations[reader],
                            std::span<const DeclaredTypeReferenceInput>(&reader_references[reader], 1),
                            authority,
                            {},
                            &statistics);
                        const auto & logical_tree = bound.getLogicalTree();
                        Definition::Ptr bound_definition;
                        bool binding_is_self_consistent = false;
                        if (bound.hasLogicalTree() && logical_tree && logical_tree->getDefinitionHandles().size() == 1
                            && logical_tree->getDescriptors().size() == 1 && logical_tree->getOccurrenceCount() == 1
                            && logical_tree->getNodeCount() == 1)
                        {
                            const auto & descriptor = logical_tree->getDescriptors().front();
                            bound_definition = logical_tree->getDefinitionHandles().front();
                            const auto & persisted = descriptor->getPersistedDescriptor();
                            binding_is_self_consistent = bound_definition && bound_definition->getIdentity() == identity
                                && descriptor->getDefinition() == bound_definition
                                && descriptor->getPhysicalType() == bound.getPhysicalType()
                                && logical_tree->getPhysicalType() == bound.getPhysicalType()
                                && persisted.getDefinitionIdentity() == bound_definition->getIdentity()
                                && persisted.getDefinitionHash() == bound_definition->getDefinitionHash()
                                && persisted.getLastKnownQualifiedName() == bound_definition->getNormalizedName();
                        }
                        if (!binding_is_self_consistent)
                        {
                            setFirstFailure(failure_code, 3);
                        }
                        else if (bound_definition != variant_a && bound_definition != variant_b && bound_definition != variant_c)
                        {
                            setFirstFailure(failure_code, 4);
                        }
                        else if (
                            statistics.specializer.resolution_sessions != 1 || statistics.specializer.definition_lookups != 1
                            || statistics.physical_factory_calls != 1)
                        {
                            setFirstFailure(failure_code, 5);
                        }
                        else if (binding_successes == std::numeric_limits<UInt64>::max())
                        {
                            setFirstFailure(failure_code, 6);
                        }
                        else
                        {
                            ++binding_successes;
                            latest_generation[reader].store(observed_generation, std::memory_order_release);
                        }
                    }
                    catch (const CatalogError & error)
                    {
                        setFirstFailure(failure_code, checkedAdd(100, static_cast<UInt64>(error.code), "failure code overflow"));
                    }
                    catch (const TypeResolverError & error)
                    {
                        setFirstFailure(failure_code, checkedAdd(200, static_cast<UInt64>(error.code), "failure code overflow"));
                    }
                    catch (...)
                    {
                        setFirstFailure(failure_code, std::numeric_limits<UInt64>::max());
                    }

                    if (first_attempt)
                    {
                        first_attempt = false;
                        initial_attempts.fetch_add(1, std::memory_order_release);
                    }
                } while (!stop_token.stop_requested());
                reader_lookup_successes[reader] = lookup_successes;
                reader_binding_successes[reader] = binding_successes;
                reader_generation_masks[reader] = generation_mask;
            });
    }

    while (ready.load(std::memory_order_acquire) != configuration.readers)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);
    while (initial_attempts.load(std::memory_order_acquire) != configuration.readers)
        std::this_thread::yield();

    const auto allReadersObservedAtLeast = [&](UInt64 generation)
    {
        for (std::size_t reader = 0; reader < reader_count; ++reader)
            if (latest_generation[reader].load(std::memory_order_acquire) < generation)
                return false;
        return true;
    };

    while (!allReadersObservedAtLeast(1) && failure_code.load(std::memory_order_acquire) == 0)
        std::this_thread::yield();

    bool pending_is_b = true;
    UInt64 completed_publications = 0;
    UInt64 root_preparation_elapsed_ns = 0;
    UInt64 root_publication_elapsed_ns = 0;
    const auto campaign_begin = std::chrono::steady_clock::now();
    for (UInt64 publication = 0; publication < configuration.publications && failure_code.load(std::memory_order_acquire) == 0;
         ++publication)
    {
        const UInt64 generation = checkedAdd(publication, 2, "concurrency publication generation overflow");
        TypeCatalogRoot::Ptr following;
        if (publication + 1 < configuration.publications)
        {
            const auto & following_definition = pending_is_b ? variant_c : variant_b;
            const auto prepare_begin = std::chrono::steady_clock::now();
            following = TypeCatalogBuilder::applyMutation(
                *pending,
                checkedAdd(generation, 1, "concurrency publication generation overflow"),
                TypeCatalogMutation::replace(identity, following_definition),
                build_limits);
            root_preparation_elapsed_ns = checkedAdd(
                root_preparation_elapsed_ns,
                elapsedNanoseconds(prepare_begin, std::chrono::steady_clock::now()),
                "concurrency root-preparation timing overflow");
        }
        const auto publish_begin = std::chrono::steady_clock::now();
        authority.publish(std::move(pending));
        root_publication_elapsed_ns = checkedAdd(
            root_publication_elapsed_ns,
            elapsedNanoseconds(publish_begin, std::chrono::steady_clock::now()),
            "concurrency publication timing overflow");
        ++completed_publications;
        pending = std::move(following);
        pending_is_b = !pending_is_b;

        if (generation <= 3)
        {
            while (!allReadersObservedAtLeast(generation) && failure_code.load(std::memory_order_acquire) == 0)
                std::this_thread::yield();
            if (failure_code.load(std::memory_order_acquire) != 0)
                break;
        }
    }
    const UInt64 final_generation = checkedAdd(configuration.publications, 1, "final generation overflow");
    if (completed_publications == configuration.publications && failure_code.load(std::memory_order_acquire) == 0)
    {
        while (!allReadersObservedAtLeast(final_generation) && failure_code.load(std::memory_order_acquire) == 0)
            std::this_thread::yield();
    }
    const auto campaign_end = std::chrono::steady_clock::now();

    for (auto & reader : readers)
        reader.request_stop();
    for (auto & reader : readers)
        reader.join();

    UInt64 total_lookup_successes = 0;
    UInt64 total_binding_successes = 0;
    for (std::size_t reader = 0; reader < reader_count; ++reader)
    {
        EXPECT_GT(reader_lookup_successes[reader], 0);
        EXPECT_GT(reader_binding_successes[reader], 0);
        EXPECT_EQ(reader_generation_masks[reader], UInt8{7});
        EXPECT_EQ(latest_generation[reader].load(std::memory_order_relaxed), final_generation);
        total_lookup_successes = checkedAdd(total_lookup_successes, reader_lookup_successes[reader], "reader lookup count overflow");
        total_binding_successes = checkedAdd(total_binding_successes, reader_binding_successes[reader], "reader binding count overflow");
    }
    EXPECT_EQ(failure_code.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(completed_publications, configuration.publications);
    EXPECT_GE(total_lookup_successes, checkedMultiply(configuration.readers, 3, "minimum lookup count overflow"));
    EXPECT_GE(total_binding_successes, checkedMultiply(configuration.readers, 3, "minimum binding count overflow"));
    EXPECT_EQ(authority.currentGeneration(), final_generation);
    const auto expected_current = configuration.publications % 2 == 1 ? variant_b : variant_c;
    EXPECT_EQ(authority.findCurrentByIdentity(identity), expected_current);
    EXPECT_EQ(authority.findCurrentByName(local_name), expected_current);

    const auto authority_counters = authority.counters();
    EXPECT_EQ(
        authority_counters.session_calls,
        checkedAdd(total_lookup_successes, total_binding_successes, "authority session expectation overflow"));
    EXPECT_EQ(authority_counters.capability_calls, total_binding_successes);
    EXPECT_EQ(authority_counters.database_calls, total_binding_successes);
    EXPECT_EQ(authority_counters.require_calls, total_binding_successes);

    const auto queued = authority.getRetirementState();
    EXPECT_EQ(queued.retired_root_count, configuration.publications);
    EXPECT_GT(queued.retired_root_bytes, 0);
    EXPECT_EQ(queued.active_hazard_slots, 0);
    const auto drained = authority.scanRetired();
    EXPECT_EQ(drained.retired_root_count, 0);
    EXPECT_EQ(drained.retired_root_bytes, 0);
    EXPECT_EQ(drained.active_hazard_slots, 0);

    record("acceptance_mode", configuration.acceptance_mode ? 1 : 0);
    record("reader_threads", configuration.readers);
    record("publication_count", configuration.publications);
    record("reader_successful_lookup_sessions", total_lookup_successes);
    record("reader_successful_bindings", total_binding_successes);
    record("active_swap_reader_handshakes", 2);
    record("final_reader_handshake_generation", final_generation);
    record("publication_campaign_elapsed_ns", elapsedNanoseconds(campaign_begin, campaign_end));
    record("root_preparation_elapsed_ns", root_preparation_elapsed_ns);
    record("root_publication_elapsed_ns", root_publication_elapsed_ns);
    record("maximum_root_accounted_bytes", maximum_root_bytes);
    record("queued_retired_root_bytes", queued.retired_root_bytes);
}

}
