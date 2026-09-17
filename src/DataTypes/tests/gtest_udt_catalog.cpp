#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/Catalog.h>

#include <Core/Field.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Definition::Ptr checkedDefinitionWithAtom(
    std::string normalized_name,
    UInt64 type_id,
    UInt64 database_id,
    UInt64 revision,
    std::string atom,
    std::string normalized_local_name = {})
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = testUUID(0x100, database_id),
        .type_uuid = testUUID(0x200, type_id),
        .revision = revision,
    };
    const auto separator = normalized_name.find('.');
    input.normalized_local_name = normalized_local_name.empty()
        ? (separator == String::npos ? normalized_name : normalized_name.substr(separator + 1))
        : std::move(normalized_local_name);
    input.normalized_name = std::move(normalized_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = std::move(atom);
    input.nodes.push_back(std::move(root));
    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

Definition::Ptr checkedDefinition(std::string normalized_name, UInt64 type_id, UInt64 database_id = 1, UInt64 revision = 1)
{
    return checkedDefinitionWithAtom(std::move(normalized_name), type_id, database_id, revision, "UInt64");
}

Definition::Ptr checkedFieldDefinition(String payload, UInt64 revision = 1)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = testUUID(0x100, 1),
        .type_uuid = testUUID(0x200, 10'001),
        .revision = revision,
    };
    input.normalized_name = "db.FieldPayload";
    input.normalized_local_name = "FieldPayload";

    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "AggregateFunction";
    root.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};

    TemplateNode function;
    function.kind = TemplateNodeKind::AggregateFunction;
    function.text = "udtFieldAccountingFunctionThatMustNotExist";
    function.children = {{.reference = 3, .label = {}}};

    TemplateNode argument_type;
    argument_type.kind = TemplateNodeKind::BuiltIn;
    argument_type.atom = "UInt64";

    TemplateNode field;
    field.kind = TemplateNodeKind::FieldValue;
    field.field_value = CanonicalFieldValue::fromField(Field(std::move(payload)));
    input.nodes = {std::move(root), std::move(function), std::move(argument_type), std::move(field)};

    auto checked = TemplateChecker::checkAll({std::move(input)});
    EXPECT_EQ(checked.size(), 1);
    return checked.front();
}

UInt64 expectedDefinitionOwnedBytes(const Definition & definition)
{
    const auto string_bytes = [](const String & value) { return static_cast<UInt64>(value.capacity()) + 1; };
    const auto vector_bytes = [](const auto & values) { return static_cast<UInt64>(values.capacity() * sizeof(values.front())); };

    UInt64 result = sizeof(Definition) + 2 * sizeof(void *);
    result += string_bytes(definition.getNormalizedName());
    result += string_bytes(definition.getNormalizedLocalName());
    result += vector_bytes(definition.getParameters());
    for (const auto & parameter : definition.getParameters())
        result += string_bytes(parameter.normalized_name);
    result += vector_bytes(definition.getNodes());
    for (const auto & node : definition.getNodes())
    {
        result += string_bytes(node.atom);
        result += string_bytes(node.text);
        result += string_bytes(node.field_value.payload);
        result += string_bytes(node.field_value.name);
        result += vector_bytes(node.enum_entries);
        for (const auto & entry : node.enum_entries)
            result += string_bytes(entry.name);
        result += vector_bytes(node.children);
        for (const auto & child : node.children)
            result += string_bytes(child.label);
    }
    result += vector_bytes(definition.getDependencies());
    result += string_bytes(definition.getCertificate().canonical_template_ir);
    result += string_bytes(definition.getCertificate().encoded_certificate);
    return result;
}

template <typename Function>
void expectCatalogError(CatalogError::Code code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected a user-defined type catalog error";
    }
    catch (const CatalogError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

TypeCatalogBuildLimits fourShardLimits()
{
    TypeCatalogBuildLimits result;
    result.shard_count = 4;
    return result;
}

TEST(UDTCatalog, BuildsIndependentShardedIndexesAndImmutableSnapshots)
{
    const auto alpha = checkedDefinition("db.Alpha", 1);
    const auto beta = checkedDefinition("db.Beta", 2);
    const std::vector first_definitions{alpha, beta};
    const auto limits = fourShardLimits();
    auto first = TypeCatalogBuilder::build(10, first_definitions, limits);

    ASSERT_EQ(first->getGeneration(), 10);
    ASSERT_EQ(first->getDatabaseUUID(), alpha->getIdentity().database_uuid);
    ASSERT_EQ(first->getDefinitionCount(), 2);
    ASSERT_EQ(first->getShardCount(), 4);
    ASSERT_GT(first->getAccountedBytes(), 0);
    EXPECT_EQ(first->findByIdentity(alpha->getIdentity()), alpha);
    EXPECT_EQ(first->findByIdentity(beta->getIdentity()), beta);
    EXPECT_EQ(first->findByName("Alpha"), alpha);
    EXPECT_EQ(first->findByName("Beta"), beta);
    EXPECT_FALSE(first->findByName("Missing"));

    UInt64 identity_entries = 0;
    UInt64 name_entries = 0;
    UInt64 identity_bytes = 0;
    UInt64 name_bytes = 0;
    for (std::size_t shard = 0; shard < first->getShardCount(); ++shard)
    {
        const auto identity = first->getIdentityShardAccounting(shard);
        const auto name = first->getNameShardAccounting(shard);
        identity_entries += identity.entries;
        name_entries += name.entries;
        identity_bytes += identity.accounted_bytes;
        name_bytes += name.accounted_bytes;
    }
    EXPECT_EQ(identity_entries, 2);
    EXPECT_EQ(name_entries, 2);
    EXPECT_GT(identity_bytes, 0);
    EXPECT_GT(name_bytes, 0);

    const auto gamma = checkedDefinition("db.Gamma", 3);
    const std::vector second_definitions{gamma, alpha, beta};
    auto second = TypeCatalogBuilder::build(11, second_definitions, limits);
    EXPECT_EQ(second->getDefinitionCount(), 3);
    EXPECT_EQ(second->findByName("Gamma"), gamma);

    /// Building another root cannot mutate either index in the first snapshot.
    EXPECT_EQ(first->getDefinitionCount(), 2);
    EXPECT_FALSE(first->findByName("Gamma"));
    EXPECT_EQ(first->findByName("Alpha"), alpha);
}

TEST(UDTCatalog, DatabaseAuthorityIsImmutableAcrossBuildMutationAndPublication)
{
    const auto limits = fourShardLimits();
    const UUID first_database = testUUID(0x100, 1);
    const auto first = checkedDefinition("db.First", 1, 1);
    const auto foreign = checkedDefinition("other.Foreign", 2, 2);

    const std::vector mixed{first, foreign};
    expectCatalogError(
        CatalogError::Code::InvalidDefinition, [&] { static_cast<void>(TypeCatalogBuilder::build(1, mixed, limits)); });

    const std::vector<Definition::Ptr> empty;
    auto empty_root = TypeCatalogBuilder::build(first_database, 1, empty, limits);
    EXPECT_EQ(empty_root->getDatabaseUUID(), first_database);
    EXPECT_EQ(empty_root->getDefinitionCount(), 0);
    auto base = TypeCatalogBuilder::applyMutation(*empty_root, 2, TypeCatalogMutation::add(first), limits);
    EXPECT_EQ(base->getDatabaseUUID(), first_database);
    EXPECT_EQ(base->findByName("First"), first);

    expectCatalogError(
        CatalogError::Code::InvalidDefinition,
        [&] { static_cast<void>(TypeCatalogBuilder::applyMutation(*base, 3, TypeCatalogMutation::add(foreign), limits)); });
    expectCatalogError(
        CatalogError::Code::InvalidDefinition,
        [&]
        { static_cast<void>(TypeCatalogBuilder::applyMutation(*base, 3, TypeCatalogMutation::remove(foreign->getIdentity()), limits)); });
    expectCatalogError(
        CatalogError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(
                TypeCatalogBuilder::applyMutation(*base, 3, TypeCatalogMutation::replace(first->getIdentity(), foreign), limits));
        });
    EXPECT_EQ(base->getGeneration(), 2);
    EXPECT_EQ(base->findByName("First"), first);
    EXPECT_FALSE(base->findByName("Foreign"));

    TypeCatalogPublicationLimits publication_limits;
    publication_limits.maximum_retired_root_bytes = 64ULL << 20;
    Catalog catalog(std::move(base), publication_limits);
    const std::vector foreign_set{foreign};
    auto foreign_root = TypeCatalogBuilder::build(3, foreign_set, limits);
    expectCatalogError(CatalogError::Code::InvalidConfiguration, [&] { catalog.publish(std::move(foreign_root)); });
    EXPECT_EQ(catalog.currentGeneration(), 2);
    EXPECT_EQ(catalog.findByName("First"), first);
}

TEST(UDTCatalog, MutationPathCopiesOnlyTouchedShards)
{
    const auto alpha = checkedDefinition("db.Alpha", 1);
    const auto beta = checkedDefinition("db.Beta", 2);
    const auto gamma = checkedDefinition("db.Gamma", 3);
    const std::vector definitions{alpha, beta};
    const auto limits = fourShardLimits();
    auto base = TypeCatalogBuilder::build(10, definitions, limits);
    auto added = TypeCatalogBuilder::applyMutation(*base, 11, TypeCatalogMutation::add(gamma), limits);

    EXPECT_EQ(added->getDefinitionCount(), 3);
    EXPECT_EQ(added->findByName("Gamma"), gamma);
    EXPECT_FALSE(base->findByName("Gamma"));

    std::size_t copied_identity_shards = 0;
    std::size_t copied_name_shards = 0;
    for (std::size_t shard = 0; shard < base->getShardCount(); ++shard)
    {
        copied_identity_shards += base->sharesIdentityShardWith(*added, shard) ? 0 : 1;
        copied_name_shards += base->sharesNameShardWith(*added, shard) ? 0 : 1;
    }
    EXPECT_EQ(copied_identity_shards, 1);
    EXPECT_EQ(copied_name_shards, 1);

    /// A same-identity replacement can rename atomically. It must update one
    /// identity shard and at most the old/new name shards, never all entries.
    const auto renamed_alpha = checkedDefinition("db.RenamedAlpha", 1);
    auto renamed = TypeCatalogBuilder::applyMutation(*added, 12, TypeCatalogMutation::replace(alpha->getIdentity(), renamed_alpha), limits);
    EXPECT_FALSE(renamed->findByName("Alpha"));
    EXPECT_EQ(renamed->findByName("RenamedAlpha"), renamed_alpha);
    EXPECT_EQ(renamed->findByIdentity(alpha->getIdentity()), renamed_alpha);
    EXPECT_EQ(added->findByName("Alpha"), alpha);

    copied_identity_shards = 0;
    copied_name_shards = 0;
    for (std::size_t shard = 0; shard < added->getShardCount(); ++shard)
    {
        copied_identity_shards += added->sharesIdentityShardWith(*renamed, shard) ? 0 : 1;
        copied_name_shards += added->sharesNameShardWith(*renamed, shard) ? 0 : 1;
    }
    EXPECT_EQ(copied_identity_shards, 1);
    EXPECT_GE(copied_name_shards, 1);
    EXPECT_LE(copied_name_shards, 2);

    /// Identity and revision are immutable semantic coordinates. A rename is
    /// permitted, but a different checked body cannot be published under the
    /// same coordinates even if every caller-visible lookup key is valid.
    const auto changed_alpha = checkedDefinitionWithAtom("db.ChangedAlpha", 1, 1, 1, "String");
    expectCatalogError(
        CatalogError::Code::InvalidDefinition,
        [&]
        {
            static_cast<void>(TypeCatalogBuilder::applyMutation(
                *renamed, 13, TypeCatalogMutation::replace(renamed_alpha->getIdentity(), changed_alpha), limits));
        });
    EXPECT_EQ(renamed->getGeneration(), 12);
    EXPECT_EQ(renamed->findByIdentity(alpha->getIdentity()), renamed_alpha);
    EXPECT_EQ(renamed->findByName("RenamedAlpha"), renamed_alpha);
    EXPECT_FALSE(renamed->findByName("ChangedAlpha"));

    auto removed = TypeCatalogBuilder::applyMutation(*renamed, 14, TypeCatalogMutation::remove(beta->getIdentity()), limits);
    EXPECT_EQ(removed->getDefinitionCount(), 2);
    EXPECT_FALSE(removed->findByIdentity(beta->getIdentity()));
    EXPECT_FALSE(removed->findByName("Beta"));
    EXPECT_EQ(renamed->findByName("Beta"), beta);
}

TEST(UDTCatalog, LocalNameIsTheOnlyDatabaseAuthorityIndexKey)
{
    const auto limits = fourShardLimits();
    const auto original = checkedDefinitionWithAtom("db1.Stable", 1, 1, 1, "UInt64", "Stable");
    const auto duplicate_local = checkedDefinitionWithAtom("db2.Stable", 2, 1, 1, "UInt64", "Stable");
    const std::vector duplicates{original, duplicate_local};
    expectCatalogError(
        CatalogError::Code::DuplicateName, [&] { static_cast<void>(TypeCatalogBuilder::build(1, duplicates, limits)); });

    const std::vector one{original};
    auto base = TypeCatalogBuilder::build(1, one, limits);
    EXPECT_EQ(base->findByName("Stable"), original);
    EXPECT_FALSE(base->findByName("db1.Stable"));

    /// Changing only the diagnostic qualifier keeps the same local lookup key
    /// and therefore copies exactly its one identity shard and one name shard.
    const auto diagnostic_rename = checkedDefinitionWithAtom("db2.Stable", 1, 1, 1, "UInt64", "Stable");
    auto diagnostic
        = TypeCatalogBuilder::applyMutation(*base, 2, TypeCatalogMutation::replace(original->getIdentity(), diagnostic_rename), limits);
    EXPECT_EQ(diagnostic->findByName("Stable"), diagnostic_rename);
    EXPECT_EQ(diagnostic->findByName("Stable")->getNormalizedName(), "db2.Stable");
    std::size_t copied_identity_shards = 0;
    std::size_t copied_name_shards = 0;
    for (std::size_t shard = 0; shard < base->getShardCount(); ++shard)
    {
        copied_identity_shards += base->sharesIdentityShardWith(*diagnostic, shard) ? 0 : 1;
        copied_name_shards += base->sharesNameShardWith(*diagnostic, shard) ? 0 : 1;
    }
    EXPECT_EQ(copied_identity_shards, 1);
    EXPECT_EQ(copied_name_shards, 1);

    const auto local_rename = checkedDefinitionWithAtom("db2.Renamed", 1, 1, 1, "UInt64", "Renamed");
    auto renamed = TypeCatalogBuilder::applyMutation(
        *diagnostic, 3, TypeCatalogMutation::replace(diagnostic_rename->getIdentity(), local_rename), limits);
    EXPECT_FALSE(renamed->findByName("Stable"));
    EXPECT_EQ(renamed->findByName("Renamed"), local_rename);
    EXPECT_EQ(diagnostic->findByName("Stable"), diagnostic_rename);
}

TEST(UDTCatalog, BuilderRejectsCollisionsAndEveryProspectiveLimitDeterministically)
{
    const auto limits = fourShardLimits();
    const auto first = checkedDefinition("db.First", 1);
    const auto duplicate_identity = checkedDefinition("db.OtherName", 1);
    const std::vector identity_collision{first, duplicate_identity};
    expectCatalogError(
        CatalogError::Code::DuplicateIdentity,
        [&] { static_cast<void>(TypeCatalogBuilder::build(1, identity_collision, limits)); });

    const auto duplicate_name = checkedDefinition("db.First", 2);
    const std::vector name_collision{first, duplicate_name};
    expectCatalogError(
        CatalogError::Code::DuplicateName, [&] { static_cast<void>(TypeCatalogBuilder::build(1, name_collision, limits)); });

    const std::vector one{first};
    TypeCatalogBuildLimits invalid_shards = limits;
    invalid_shards.shard_count = 3;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, invalid_shards)); });

    TypeCatalogBuildLimits count_limit = limits;
    count_limit.maximum_definitions = 0;
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, count_limit)); });

    TypeCatalogBuildLimits name_limit = limits;
    name_limit.maximum_normalized_name_bytes = first->getNormalizedLocalName().size() - 1;
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, name_limit)); });

    TypeCatalogBuildLimits name_length_limit = limits;
    name_length_limit.maximum_normalized_name_length = first->getNormalizedLocalName().size() - 1;
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, name_length_limit)); });

    auto bounded_lookup = TypeCatalogBuilder::build(1, one, limits);
    const String overlong_lookup(static_cast<size_t>(limits.maximum_normalized_name_length + 1), 'x');
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(bounded_lookup->findByName(overlong_lookup)); });

    const auto measured = TypeCatalogBuilder::build(1, one, limits)->getAccountedBytes();
    ASSERT_GT(measured, 0);
    TypeCatalogBuildLimits byte_limit = limits;
    byte_limit.maximum_root_accounted_bytes = measured - 1;
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, byte_limit)); });

    const std::vector<Definition::Ptr> null_definition{nullptr};
    expectCatalogError(
        CatalogError::Code::InvalidDefinition,
        [&] { static_cast<void>(TypeCatalogBuilder::build(1, null_definition, limits)); });

    expectCatalogError(
        CatalogError::Code::DuplicateIdentity,
        [&]
        {
            static_cast<void>(
                TypeCatalogBuilder::applyMutation(*TypeCatalogBuilder::build(1, one, limits), 2, TypeCatalogMutation::add(first), limits));
        });

    const auto missing = checkedDefinition("db.Missing", 99)->getIdentity();
    auto mutation_base = TypeCatalogBuilder::build(1, one, limits);
    expectCatalogError(
        CatalogError::Code::MissingIdentity,
        [&] { static_cast<void>(TypeCatalogBuilder::applyMutation(*mutation_base, 2, TypeCatalogMutation::remove(missing), limits)); });

    TypeCatalogBuildLimits mutation_count_limit = limits;
    mutation_count_limit.maximum_definitions = 1;
    const auto another = checkedDefinition("db.Another", 100);
    expectCatalogError(
        CatalogError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                TypeCatalogBuilder::applyMutation(*mutation_base, 2, TypeCatalogMutation::add(another), mutation_count_limit));
        });
}

TEST(UDTCatalog, FieldPayloadBytesAreRejectedProspectivelyBeforeMutationPublication)
{
    const auto small = checkedFieldDefinition("x");
    /// Replacing checked semantics under one immutable revision is invalid
    /// independently of quotas. Use a legitimate revision rollover so this
    /// test reaches and isolates prospective retained-payload accounting.
    const auto large = checkedFieldDefinition(String(8ULL << 10, 'x'), 2);
    const auto generous_limits = fourShardLimits();
    const std::vector small_set{small};
    const std::vector large_set{large};
    auto base = TypeCatalogBuilder::build(1, small_set, generous_limits);
    auto large_root = TypeCatalogBuilder::build(2, large_set, generous_limits);

    const UInt64 small_definition_bytes = expectedDefinitionOwnedBytes(*small);
    const UInt64 large_definition_bytes = expectedDefinitionOwnedBytes(*large);
    ASSERT_EQ(small->getIdentity().database_uuid, large->getIdentity().database_uuid);
    ASSERT_EQ(small->getIdentity().type_uuid, large->getIdentity().type_uuid);
    ASSERT_LT(small->getIdentity().revision, large->getIdentity().revision);
    ASSERT_GT(large_definition_bytes, small_definition_bytes);
    const UInt64 definition_delta = large_definition_bytes - small_definition_bytes;
    ASSERT_EQ(large_root->getAccountedBytes() - base->getAccountedBytes(), definition_delta);

    TypeCatalogBuildLimits lowered = generous_limits;
    lowered.maximum_root_accounted_bytes = base->getAccountedBytes() + definition_delta - 1;
    expectCatalogError(
        CatalogError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                TypeCatalogBuilder::applyMutation(*base, 2, TypeCatalogMutation::replace(small->getIdentity(), large), lowered));
        });

    EXPECT_EQ(base->getGeneration(), 1);
    EXPECT_EQ(base->getDefinitionCount(), 1);
    EXPECT_EQ(base->findByIdentity(small->getIdentity()), small);
    EXPECT_EQ(base->findByName(small->getNormalizedLocalName()), small);
}

TEST(UDTCatalog, PerShardBoundsPreventCatalogSizedPathCopies)
{
    TypeCatalogBuildLimits one_shard_limits;
    one_shard_limits.shard_count = 1;
    one_shard_limits.maximum_identity_shard_entries = 1;
    one_shard_limits.maximum_name_shard_entries = 1;

    const auto first = checkedDefinition("db.First", 1);
    const auto second = checkedDefinition("db.Second", 2);
    const std::vector one{first};
    const std::vector two{first, second};
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, two, one_shard_limits)); });

    auto base = TypeCatalogBuilder::build(1, one, one_shard_limits);
    expectCatalogError(
        CatalogError::Code::LimitExceeded,
        [&] { static_cast<void>(TypeCatalogBuilder::applyMutation(*base, 2, TypeCatalogMutation::add(second), one_shard_limits)); });

    TypeCatalogBuildLimits widened = one_shard_limits;
    widened.maximum_identity_shard_entries = 2;
    widened.maximum_name_shard_entries = 2;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { static_cast<void>(TypeCatalogBuilder::applyMutation(*base, 2, TypeCatalogMutation::add(second), widened)); });

    TypeCatalogBuildLimits byte_limited = one_shard_limits;
    byte_limited.maximum_shard_accounted_bytes = 1;
    expectCatalogError(
        CatalogError::Code::LimitExceeded, [&] { static_cast<void>(TypeCatalogBuilder::build(1, one, byte_limited)); });
}

TEST(UDTCatalog, PublicationBackpressurePrecedesExchangeAndRetirementIsExplicit)
{
    const auto limits = fourShardLimits();
    const auto first = checkedDefinition("db.First", 1);
    const auto second = checkedDefinition("db.Second", 2);
    const auto third = checkedDefinition("db.Third", 3);
    const std::vector first_set{first};
    const std::vector second_set{second};
    const std::vector third_set{third};

    auto initial = TypeCatalogBuilder::build(1, first_set, limits);
    const UInt64 retirement_bytes = initial->getAccountedBytes() * 4;
    TypeCatalogPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 8;
    publication_limits.maximum_retired_root_count = 1;
    publication_limits.maximum_retired_root_bytes = retirement_bytes;
    Catalog catalog(std::move(initial), publication_limits);

    const auto retained_first = catalog.findByName("First");
    ASSERT_EQ(retained_first, first);
    catalog.publish(TypeCatalogBuilder::build(2, second_set, limits));
    EXPECT_EQ(catalog.currentGeneration(), 2);
    EXPECT_FALSE(catalog.findByName("First"));
    EXPECT_EQ(catalog.findByName("Second"), second);

    const auto queued = catalog.getRetirementState();
    EXPECT_EQ(queued.retired_root_count, 1);
    EXPECT_GT(queued.retired_root_bytes, 0);

    auto rejected = TypeCatalogBuilder::build(3, third_set, limits);
    expectCatalogError(CatalogError::Code::LimitExceeded, [&] { catalog.publish(std::move(rejected)); });
    EXPECT_EQ(catalog.currentGeneration(), 2);
    EXPECT_EQ(catalog.findByName("Second"), second);

    const auto drained = catalog.scanRetired();
    EXPECT_EQ(drained.retired_root_count, 0);
    EXPECT_EQ(drained.retired_root_bytes, 0);
    /// The copied definition handle remains valid independently of its O(N)
    /// root after writer-side retirement.
    EXPECT_EQ(retained_first->getNormalizedName(), "db.First");

    catalog.publish(TypeCatalogBuilder::build(3, third_set, limits));
    EXPECT_EQ(catalog.currentGeneration(), 3);
    EXPECT_EQ(catalog.findByName("Third"), third);
}

TEST(UDTCatalog, ResolutionSessionPinsOneSnapshotAndOnlyWriterReclaimsIt)
{
    const auto first = checkedDefinition("db.First", 1);
    const auto second = checkedDefinition("db.Second", 2);
    const std::vector first_set{first};
    const std::vector second_set{second};
    const auto build_limits = fourShardLimits();
    TypeCatalogPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 1;
    publication_limits.maximum_retired_root_count = 1;
    publication_limits.maximum_retired_root_bytes = 64ULL << 20;
    Catalog catalog(TypeCatalogBuilder::build(1, first_set, build_limits), publication_limits);

    {
        auto session = catalog.beginResolutionSession();
        EXPECT_EQ(session.getGeneration(), 1);
        EXPECT_EQ(session.findByName("First"), first);
        EXPECT_EQ(session.findByIdentity(first->getIdentity()), first);

        catalog.publish(TypeCatalogBuilder::build(2, second_set, build_limits));
        EXPECT_EQ(catalog.currentGeneration(), 2);
        EXPECT_EQ(session.getGeneration(), 1);

        /// Both lookups remain on the root pinned at session creation, even
        /// though publication has replaced the catalog's live root.
        EXPECT_EQ(session.findByName("First"), first);
        EXPECT_EQ(session.findByIdentity(first->getIdentity()), first);
        EXPECT_FALSE(session.findByName("Second"));

        /// This catalog has one slot and the session owns it until scope exit.
        expectCatalogError(CatalogError::Code::HazardSlotsExhausted, [&] { catalog.findByName("Second"); });
        const auto still_pinned = catalog.scanRetired();
        EXPECT_EQ(still_pinned.retired_root_count, 1);
        EXPECT_EQ(still_pinned.active_hazard_slots, 1);
    }

    /// Session teardown only clears its hazard; it cannot reclaim an O(N)
    /// root on this reader path.
    const auto released_but_not_reclaimed = catalog.getRetirementState();
    EXPECT_EQ(released_but_not_reclaimed.retired_root_count, 1);
    EXPECT_EQ(released_but_not_reclaimed.active_hazard_slots, 0);
    EXPECT_EQ(catalog.findByName("Second"), second);

    const auto explicitly_reclaimed = catalog.scanRetired();
    EXPECT_EQ(explicitly_reclaimed.retired_root_count, 0);
    EXPECT_EQ(explicitly_reclaimed.retired_root_bytes, 0);
}

TEST(UDTCatalog, PublicationRejectsShardGenerationAndHazardConfigurationDrift)
{
    const auto definition = checkedDefinition("db.Value", 1);
    const std::vector definitions{definition};
    auto limits = fourShardLimits();
    auto initial = TypeCatalogBuilder::build(5, definitions, limits);

    TypeCatalogPublicationLimits no_slots;
    no_slots.hazard_slot_count = 0;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { Catalog invalid(TypeCatalogBuilder::build(5, definitions, limits), no_slots); });

    TypeCatalogPublicationLimits too_few_bytes;
    too_few_bytes.maximum_retired_root_bytes = initial->getAccountedBytes() - 1;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { Catalog invalid(TypeCatalogBuilder::build(5, definitions, limits), too_few_bytes); });

    Catalog catalog(std::move(initial));
    expectCatalogError(
        CatalogError::Code::GenerationMismatch, [&] { catalog.publish(TypeCatalogBuilder::build(5, definitions, limits)); });
    EXPECT_EQ(catalog.currentGeneration(), 5);

    limits.shard_count = 8;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { catalog.publish(TypeCatalogBuilder::build(6, definitions, limits)); });
    EXPECT_EQ(catalog.currentGeneration(), 5);

    limits = fourShardLimits();
    ++limits.maximum_identity_shard_entries;
    expectCatalogError(
        CatalogError::Code::InvalidConfiguration,
        [&] { catalog.publish(TypeCatalogBuilder::build(6, definitions, limits)); });
    EXPECT_EQ(catalog.currentGeneration(), 5);
}

TEST(UDTCatalog, LockFreeReadersRemainSafeAcrossPublicationAndWriterOnlyReclamation)
{
    const auto common = checkedDefinition("db.Common", 1);
    const std::vector definitions{common};
    const auto build_limits = fourShardLimits();
    TypeCatalogPublicationLimits publication_limits;
    publication_limits.hazard_slot_count = 16;
    publication_limits.maximum_retired_root_count = 16;
    publication_limits.maximum_retired_root_bytes = 64ULL << 20;
    Catalog catalog(TypeCatalogBuilder::build(1, definitions, build_limits), publication_limits);

    std::atomic<bool> stop{false};
    std::atomic<UInt64> failures{0};
    std::vector<std::thread> readers;
    for (std::size_t index = 0; index < 8; ++index)
    {
        readers.emplace_back(
            [&]
            {
                while (!stop.load(std::memory_order_acquire))
                {
                    try
                    {
                        const auto found = catalog.findByName("Common");
                        if (!found || found->getIdentity() != common->getIdentity())
                            failures.fetch_add(1, std::memory_order_relaxed);
                    }
                    catch (const CatalogError &)
                    {
                        failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
    }

    for (UInt64 generation = 2; generation <= 200; ++generation)
    {
        for (;;)
        {
            try
            {
                catalog.publish(TypeCatalogBuilder::build(generation, definitions, build_limits));
                break;
            }
            catch (const CatalogError & error)
            {
                if (error.code != CatalogError::Code::LimitExceeded)
                {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                static_cast<void>(catalog.scanRetired());
                std::this_thread::yield();
            }
        }
        static_cast<void>(catalog.scanRetired());
    }

    stop.store(true, std::memory_order_release);
    for (auto & reader : readers)
        reader.join();
    EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(catalog.currentGeneration(), 200);
    const auto drained = catalog.scanRetired();
    EXPECT_EQ(drained.retired_root_count, 0);
    EXPECT_EQ(drained.active_hazard_slots, 0);
}

TEST(UDTCatalog, ShutdownDrainsTheLocalDomainAndRejectsNewReaders)
{
    const auto definition = checkedDefinition("db.Value", 1);
    const std::vector definitions{definition};
    Catalog catalog(TypeCatalogBuilder::build(1, definitions, fourShardLimits()));

    EXPECT_EQ(catalog.findByIdentity(definition->getIdentity()), definition);
    catalog.shutdownAndDrain();
    EXPECT_TRUE(catalog.isShutdown());
    EXPECT_EQ(catalog.currentGeneration(), 0);
    EXPECT_EQ(catalog.getRetirementState().retired_root_count, 0);
    expectCatalogError(CatalogError::Code::Shutdown, [&] { catalog.findByName("Value"); });
    expectCatalogError(
        CatalogError::Code::Shutdown,
        [&] { catalog.publish(TypeCatalogBuilder::build(2, definitions, fourShardLimits())); });
    /// Idempotent explicit detach and destructor drain.
    EXPECT_NO_THROW(catalog.shutdownAndDrain());
}

}
}
