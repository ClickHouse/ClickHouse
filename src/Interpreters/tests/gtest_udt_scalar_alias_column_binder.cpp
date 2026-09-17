#include <Interpreters/UDTScalarAliasColumnBinder.h>

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/User.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TypeResolver.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

UUID databaseUUID()
{
    return uuid(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
}

SchemaObjectID tableObject()
{
    return {
        .kind = SchemaObjectKind::Table,
        .database_uuid = databaseUUID(),
        .object_uuid = uuid(0x123456789abcdef0ULL, 0x0102030405060708ULL),
    };
}

TypeAuthorityCapabilities capabilities()
{
    return {
        .adapter_abi = 1,
        .mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates),
        .limits = {
            .maximum_definitions = 32,
            .maximum_definition_bytes = 1ULL << 20,
            .maximum_template_nodes = 4'096,
            .maximum_direct_dependencies = 256,
            .maximum_transitive_dependencies = 32,
            .maximum_checker_work = 65'536,
        },
    };
}

Definition::Ptr checkedAlias(String name, String physical_type = "UInt64", bool parameterized = false)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = databaseUUID(),
        .type_uuid = uuid(0xabcdef0123456789ULL, parameterized ? 2 : 1),
        .revision = 1,
    };
    input.normalized_name = "app." + name;
    input.normalized_local_name = std::move(name);
    if (parameterized)
        input.parameters.push_back({.normalized_name = "T", .kind = ParameterKind::Type});
    TemplateNode root;
    if (parameterized)
    {
        root.kind = TemplateNodeKind::TypeParameter;
        root.parameter = 0;
    }
    else
    {
        root.kind = TemplateNodeKind::BuiltIn;
        root.atom = std::move(physical_type);
    }
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

ASTPtr parseCreate(const String & sql)
{
    ParserCreateQuery parser;
    return parseQuery(parser, sql, "UDT scalar-alias column binder test", 0, 256, 100'000);
}

ASTCreateQuery & asCreate(const ASTPtr & ast)
{
    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw std::logic_error("test input did not parse as CREATE");
    return *create;
}

ASTColumnDeclaration & column(ASTCreateQuery & create, size_t index)
{
    if (!create.columns_list || !create.columns_list->columns || index >= create.columns_list->columns->children.size())
        throw std::logic_error("test CREATE has no requested column");
    auto * declaration = create.columns_list->columns->children[index]->as<ASTColumnDeclaration>();
    if (!declaration)
        throw std::logic_error("test CREATE child is not a column declaration");
    return *declaration;
}

template <typename Callback>
void expectBinderError(ScalarAliasColumnBinderError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a scalar-alias column binder error";
    }
    catch (const ScalarAliasColumnBinderError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

class CountingAuthority final : public IAuthorityAdapter
{
public:
    explicit CountingAuthority(AuthorityAdapterPtr backend_)
        : backend(std::move(backend_))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override
    {
        ++capability_calls;
        return backend->getCapabilities();
    }

    UUID getDatabaseUUID() const noexcept override
    {
        ++database_calls;
        return backend->getDatabaseUUID();
    }

    ResolutionSession beginResolutionSession() const override
    {
        ++session_calls;
        return backend->beginResolutionSession();
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        ++require_calls;
        backend->requireCapabilities(required, operation);
    }

    mutable size_t capability_calls = 0;
    mutable size_t database_calls = 0;
    mutable size_t session_calls = 0;
    mutable size_t require_calls = 0;

private:
    AuthorityAdapterPtr backend;
};

class FailIfTouchedAuthority final : public IAuthorityAdapter
{
public:
    const TypeAuthorityCapabilities & getCapabilities() const noexcept override
    {
        touched = true;
        return stored_capabilities;
    }

    UUID getDatabaseUUID() const noexcept override
    {
        touched = true;
        return UUIDHelpers::Nil;
    }

    ResolutionSession beginResolutionSession() const override
    {
        touched = true;
        throw std::logic_error("physical-only path touched its UDT authority");
    }

    void requireCapabilities(TypeAuthorityCapabilityMask, std::string_view) const override
    {
        touched = true;
        throw std::logic_error("physical-only path required UDT capabilities");
    }

    mutable bool touched = false;

private:
    TypeAuthorityCapabilities stored_capabilities;
};

class DeniedUsageContext final
{
public:
    DeniedUsageContext()
    {
        auto & access_control = getMutableContext().context->getAccessControl();
        storage = std::make_shared<MemoryAccessStorage>(
            "udt_scalar_alias_column_binder_denied_access", access_control.getChangesNotifier(), false);
        access_control.addStorage(storage);
        auto user = std::make_shared<User>();
        user->setName("udt_scalar_alias_column_binder_denied_user");
        if (!storage->insert(userUUID(), user, false, true))
            throw std::logic_error("failed to install denied UDT binder test user");

        context = Context::createCopy(getContext().context);
        context->setUser(userUUID());
    }

    DeniedUsageContext(const DeniedUsageContext &) = delete;
    DeniedUsageContext & operator=(const DeniedUsageContext &) = delete;

    ~DeniedUsageContext()
    {
        context.reset();
        auto & access_control = getMutableContext().context->getAccessControl();
        if (storage)
        {
            static_cast<void>(storage->remove(userUUID(), false));
            access_control.removeStorage(storage);
        }
    }

    ContextPtr get() const noexcept { return context; }

private:
    static UUID userUUID() { return uuid(0x9000000000000000ULL, 0x501); }

    std::shared_ptr<MemoryAccessStorage> storage;
    ContextMutablePtr context;
};

}

TEST(UDTScalarAliasColumnBinder, PhysicalOnlyRoutingIsCompletelyInert)
{
    ASTPtr ast = parseCreate("CREATE TABLE app.events (id UInt64, label String) ENGINE = Memory");
    auto & create = asCreate(ast);
    FailIfTouchedAuthority authority;
    ContextPtr no_context;

    EXPECT_FALSE(hasReferencesInCreateTableColumns(create));
    const auto prepared = prepareScalarAliasColumns(create, {}, no_context, authority);
    EXPECT_FALSE(prepared);
    EXPECT_FALSE(authority.touched);
    EXPECT_EQ(column(create, 0).getType()->formatWithSecretsOneLine(), "UInt64");
}

TEST(UDTScalarAliasColumnBinder, RootAliasesArePhysicalizedOnlyAfterCompletePreparationAndFinishCanonically)
{
    ASTPtr ast = parseCreate("CREATE TABLE app.events (id app.UserId, repeated app.UserId, label String) ENGINE = Memory");
    auto & create = asCreate(ast);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));
    const auto context = Context::createCopy(getContext().context);

    auto prepared = prepareScalarAliasColumns(create, "app", context, authority);
    ASSERT_TRUE(prepared);
    EXPECT_NE(column(create, 0).getType()->as<ASTUDTReference>(), nullptr);
    EXPECT_NE(column(create, 1).getType()->as<ASTUDTReference>(), nullptr);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 3u);
    auto expected = prepared->getExpectedPhysicalColumns().begin();
    EXPECT_EQ(expected->name, "id");
    EXPECT_EQ(expected->type->getName(), "UInt64");
    ++expected;
    EXPECT_EQ(expected->name, "repeated");
    EXPECT_EQ(expected->type->getName(), "UInt64");
    ++expected;
    EXPECT_EQ(expected->name, "label");
    EXPECT_EQ(expected->type->getName(), "String");

    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(column(create, 0).getType()->as<ASTDataType>()->name, "UInt64");
    EXPECT_EQ(column(create, 1).getType()->as<ASTDataType>()->name, "UInt64");
    EXPECT_EQ(column(create, 2).getType()->as<ASTDataType>()->name, "String");

    NamesAndTypesList normalized = prepared->getExpectedPhysicalColumns();
    auto bindings = std::move(*prepared).finish(tableObject(), 1, normalized);
    ASSERT_TRUE(bindings.persisted_references);
    ASSERT_TRUE(bindings.sidecar_expectation);
    EXPECT_EQ(bindings.physical_columns, normalized);
    EXPECT_EQ(bindings.persisted_references->occurrence_paths.size(), 2u);
    EXPECT_EQ(bindings.persisted_references->descriptors.size(), 1u);
    ASSERT_EQ(bindings.dependency_edges.size(), 1u);
    EXPECT_EQ(bindings.dependency_edges.front().dependent, tableObject());
    EXPECT_EQ(bindings.dependency_edges.front().dependency.object_uuid, definition->getIdentity().type_uuid);
}

TEST(UDTScalarAliasColumnBinder, BatchedUsageCheckRunsAfterOneLookupSessionAndBeforeTypeResolver)
{
    ASTPtr ast = parseCreate("CREATE TABLE app.events (id app.UserId) ENGINE = Memory");
    auto & create = asCreate(ast);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));
    DeniedUsageContext denied;

    EXPECT_THROW(static_cast<void>(prepareScalarAliasColumns(create, "app", denied.get(), authority)), Exception);
    EXPECT_EQ(authority.session_calls, 1u);
    EXPECT_EQ(authority.require_calls, 0u);
    EXPECT_NE(column(create, 0).getType()->as<ASTUDTReference>(), nullptr);
}

TEST(UDTScalarAliasColumnBinder, NestedReferencesArePhysicalizedWithStableChildLineage)
{
    const auto context = Context::createCopy(getContext().context);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));

    ASTPtr nested_ast = parseCreate("CREATE TABLE app.events (ids Array(app.UserId)) ENGINE = Memory");
    auto & nested = asCreate(nested_ast);
    EXPECT_TRUE(hasReferencesInCreateTableColumns(nested));
    auto prepared = prepareScalarAliasColumns(nested, "app", context, authority);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 1u);
    EXPECT_EQ(prepared->getExpectedPhysicalColumns().front().type->getName(), "Array(UInt64)");

    NamesAndTypesList normalized = prepared->getExpectedPhysicalColumns();
    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(column(nested, 0).getType()->formatWithSecretsOneLine(), "Array(UInt64)");

    auto bindings = std::move(*prepared).finish(tableObject(), 1, normalized);
    ASSERT_TRUE(bindings.persisted_references);
    const auto & references = *bindings.persisted_references;
    ASSERT_EQ(references.descriptors.size(), 1u);
    ASSERT_EQ(references.occurrence_paths.size(), 1u);
    ASSERT_EQ(references.uses.size(), 1u);
    EXPECT_EQ(references.occurrence_paths[0].object_ordinal, 0u);
    EXPECT_EQ(references.occurrence_paths[0].occurrence_ordinal, 0u);
    EXPECT_EQ(references.occurrence_paths[0].type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(references.uses[0].path_id, 0u);
    ASSERT_LT(references.uses[0].descriptor_id, references.descriptors.size());
    EXPECT_EQ(
        references.descriptors[references.uses[0].descriptor_id].getDefinitionIdentity().type_uuid, definition->getIdentity().type_uuid);
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, RootNestedReferencesRebaseAcrossFlattenNested)
{
    const auto context = Context::createCopy(getContext().context);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));

    ASTPtr ast = parseCreate("CREATE TABLE app.events (payload Nested(id app.UserId, label String)) ENGINE = Memory");
    auto & create = asCreate(ast);
    auto prepared = prepareScalarAliasColumns(create, "app", context, authority);
    ASSERT_TRUE(prepared);
    prepared->applyPhysicalTypeASTs();

    NamesAndTypesList normalized;
    normalized.emplace_back("payload.id", DataTypeFactory::instance().get("Array(UInt64)"));
    normalized.emplace_back("payload.label", DataTypeFactory::instance().get("Array(String)"));
    auto bindings = std::move(*prepared).finish(tableObject(), 1, normalized);

    EXPECT_EQ(bindings.physical_columns, normalized);
    ASSERT_TRUE(bindings.persisted_references);
    ASSERT_TRUE(bindings.bound_physical_schema);
    ASSERT_TRUE(bindings.sidecar_expectation);
    ASSERT_EQ(bindings.persisted_references->occurrence_paths.size(), 1u);
    EXPECT_EQ(bindings.persisted_references->occurrence_paths[0].object_ordinal, 0u);
    EXPECT_EQ(bindings.persisted_references->occurrence_paths[0].type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(bindings.physical_schema_fingerprint, bindings.persisted_references->physical_schema_fingerprint);
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, JSONTypedPathReferencesPreserveOwnedChild)
{
    const auto context = Context::createCopy(getContext().context);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));

    ASTPtr ast = parseCreate("CREATE TABLE app.events (payload JSON(user_id app.UserId)) ENGINE = Memory");
    auto & create = asCreate(ast);
    auto prepared = prepareScalarAliasColumns(create, "app", context, authority);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 1u);
    EXPECT_EQ(prepared->getExpectedPhysicalColumns().front().type->getName(), "JSON(user_id UInt64)");

    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(column(create, 0).getType()->formatWithSecretsOneLine(), "JSON(user_id UInt64)");
    NamesAndTypesList normalized = prepared->getExpectedPhysicalColumns();
    auto bindings = std::move(*prepared).finish(tableObject(), 1, normalized);

    ASSERT_TRUE(bindings.persisted_references);
    ASSERT_EQ(bindings.persisted_references->occurrence_paths.size(), 1u);
    EXPECT_EQ(bindings.persisted_references->occurrence_paths[0].type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, CrossDatabaseReferencesFailBeforeAuthorityLookup)
{
    FailIfTouchedAuthority authority;
    const auto context = Context::createCopy(getContext().context);

    ASTPtr foreign_ast = parseCreate("CREATE TABLE app.events (id other.UserId) ENGINE = Memory");
    auto & foreign = asCreate(foreign_ast);
    expectBinderError(
        ScalarAliasColumnBinderError::Code::CrossDatabaseReference,
        [&] { static_cast<void>(prepareScalarAliasColumns(foreign, "app", context, authority)); });
    EXPECT_FALSE(authority.touched);
}

TEST(UDTScalarAliasColumnBinder, SyntacticPreflightBoundsTheWholeDeclarationBatchBeforeAuthorityLookup)
{
    FailIfTouchedAuthority authority;
    const auto context = Context::createCopy(getContext().context);
    ASTPtr ast = parseCreate("CREATE TABLE app.events (id app.UserId) ENGINE = Memory");
    auto & create = asCreate(ast);

    std::vector<ASTColumnDeclaration *> declarations(
        static_cast<size_t>(TypeResolverLimits{}.maximum_input_references + 1), &column(create, 0));
    try
    {
        static_cast<void>(prepareScalarAliasAlterColumns(declarations, "app", context, authority));
        FAIL() << "expected an aggregate syntactic preflight limit failure";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
    EXPECT_FALSE(authority.touched);
}

TEST(UDTScalarAliasColumnBinder, ParameterizedReferenceRetainsNestedTypeArgumentLineage)
{
    const auto context = Context::createCopy(getContext().context);
    const auto box = checkedAlias("Box", "UInt64", true);
    const auto user_id = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {box, user_id}));

    ASTPtr parenthesized_ast = parseCreate("CREATE TABLE app.events (value app.Box(Array(app.UserId))) ENGINE = Memory");
    auto & parenthesized = asCreate(parenthesized_ast);
    auto prepared = prepareScalarAliasColumns(parenthesized, "app", context, authority);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 1u);
    EXPECT_EQ(prepared->getExpectedPhysicalColumns().front().type->getName(), "Array(UInt64)");

    NamesAndTypesList normalized = prepared->getExpectedPhysicalColumns();
    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(column(parenthesized, 0).getType()->formatWithSecretsOneLine(), "Array(UInt64)");

    auto bindings = std::move(*prepared).finish(tableObject(), 1, normalized);
    ASSERT_TRUE(bindings.persisted_references);
    const auto & references = *bindings.persisted_references;
    ASSERT_EQ(references.descriptors.size(), 2u);
    ASSERT_EQ(references.occurrence_paths.size(), 2u);
    ASSERT_EQ(references.uses.size(), 2u);
    EXPECT_TRUE(references.occurrence_paths[0].type_child_ordinals.empty());
    EXPECT_EQ(references.occurrence_paths[1].type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(references.uses[0].path_id, 0u);
    EXPECT_EQ(references.uses[1].path_id, 1u);
    ASSERT_LT(references.uses[0].descriptor_id, references.descriptors.size());
    ASSERT_LT(references.uses[1].descriptor_id, references.descriptors.size());
    const auto & outer = references.descriptors[references.uses[0].descriptor_id];
    const auto & nested = references.descriptors[references.uses[1].descriptor_id];
    EXPECT_EQ(outer.getDefinitionIdentity().type_uuid, box->getIdentity().type_uuid);
    EXPECT_FALSE(outer.getCanonicalArgumentsEncoding().empty());
    EXPECT_EQ(nested.getDefinitionIdentity().type_uuid, user_id->getIdentity().type_uuid);
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, ParameterizedReferenceRetainsEveryBranchedTypeArgumentOccurrence)
{
    const auto context = Context::createCopy(getContext().context);
    const auto box = checkedAlias("Box", "UInt64", true);
    const auto user_id = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {box, user_id}));

    ASTPtr ast = parseCreate(
        "CREATE TABLE app.events (value app.Box(Tuple("
        "owner app.UserId, ids Array(app.UserId), lookup Map(String, app.UserId)))) ENGINE = Memory");
    auto & create = asCreate(ast);
    auto prepared = prepareScalarAliasColumns(create, "app", context, authority);
    ASSERT_TRUE(prepared);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 1u);
    EXPECT_EQ(
        prepared->getExpectedPhysicalColumns().front().type->getName(),
        "Tuple(owner UInt64, ids Array(UInt64), lookup Map(String, UInt64))");

    NamesAndTypesList normalized = prepared->getExpectedPhysicalColumns();
    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(
        column(create, 0).getType()->formatWithSecretsOneLine(), "Tuple(owner UInt64, ids Array(UInt64), lookup Map(String, UInt64))");
    auto bindings = std::move(*prepared).finish(tableObject(), 3, normalized);

    ASSERT_TRUE(bindings.persisted_references);
    const auto & references = *bindings.persisted_references;
    ASSERT_EQ(references.descriptors.size(), 2u);
    ASSERT_EQ(references.occurrence_paths.size(), 4u);
    ASSERT_EQ(references.uses.size(), 4u);

    std::vector<std::vector<UInt64>> actual_paths;
    actual_paths.reserve(references.occurrence_paths.size());
    for (const auto & path : references.occurrence_paths)
    {
        EXPECT_EQ(path.object_ordinal, 0u);
        actual_paths.push_back(path.type_child_ordinals);
    }
    std::sort(actual_paths.begin(), actual_paths.end());
    const std::vector<std::vector<UInt64>> expected_paths{{}, {0}, {1, 0}, {2, 1}};
    EXPECT_EQ(actual_paths, expected_paths);

    size_t box_uses = 0;
    size_t user_id_uses = 0;
    for (const auto & use : references.uses)
    {
        ASSERT_LT(use.path_id, references.occurrence_paths.size());
        ASSERT_LT(use.descriptor_id, references.descriptors.size());
        const auto & identity = references.descriptors[use.descriptor_id].getDefinitionIdentity();
        if (identity.type_uuid == box->getIdentity().type_uuid)
            ++box_uses;
        else if (identity.type_uuid == user_id->getIdentity().type_uuid)
            ++user_id_uses;
        else
            ADD_FAILURE() << "unexpected descriptor identity";
    }
    EXPECT_EQ(box_uses, 1u);
    EXPECT_EQ(user_id_uses, 3u);
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, ApplyPreflightIsAllOrNothingAndRetryableAfterASTDrift)
{
    const auto context = Context::createCopy(getContext().context);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));

    ASTPtr ast = parseCreate("CREATE TABLE app.events (first app.UserId, second app.UserId) ENGINE = Memory");
    auto & create = asCreate(ast);
    auto prepared = prepareScalarAliasColumns(create, "app", context, authority);
    ASSERT_TRUE(prepared);

    const ASTPtr original_first = column(create, 0).getType();
    ASTPtr original_second = column(create, 1).getType();
    ASTPtr changed_second = original_second->clone();
    const auto * changed_second_ptr = changed_second.get();
    column(create, 1).setType(std::move(changed_second));
    expectBinderError(ScalarAliasColumnBinderError::Code::QueryChanged, [&] { prepared->applyPhysicalTypeASTs(); });

    EXPECT_EQ(column(create, 0).getType().get(), original_first.get());
    EXPECT_NE(column(create, 0).getType()->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(column(create, 1).getType().get(), changed_second_ptr);
    EXPECT_NE(column(create, 1).getType()->as<ASTUDTReference>(), nullptr);

    column(create, 1).setType(std::move(original_second));
    EXPECT_NO_THROW(prepared->applyPhysicalTypeASTs());
    EXPECT_EQ(column(create, 0).getType()->formatWithSecretsOneLine(), "UInt64");
    EXPECT_EQ(column(create, 1).getType()->formatWithSecretsOneLine(), "UInt64");
}

TEST(UDTScalarAliasColumnBinder, AggregateAlterUsesOneSnapshotAndReturnsAlignedColumnFragments)
{
    const auto context = Context::createCopy(getContext().context);
    const auto definition = checkedAlias("UserId");
    CountingAuthority authority(makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition}));

    ASTPtr ast = parseCreate("CREATE TABLE app.events (added app.UserId, physical String, modified Array(app.UserId)) ENGINE = Memory");
    auto & create = asCreate(ast);
    std::vector<ASTColumnDeclaration *> declarations{
        &column(create, 0),
        &column(create, 1),
        &column(create, 2),
    };
    auto prepared = prepareScalarAliasAlterColumns(declarations, "app", context, authority);
    ASSERT_TRUE(prepared);
    EXPECT_EQ(authority.session_calls, 1u);
    ASSERT_EQ(prepared->getExpectedPhysicalColumns().size(), 3u);

    prepared->applyPhysicalTypeASTs();
    EXPECT_EQ(column(create, 0).getType()->formatWithSecretsOneLine(), "UInt64");
    EXPECT_EQ(column(create, 1).getType()->formatWithSecretsOneLine(), "String");
    EXPECT_EQ(column(create, 2).getType()->formatWithSecretsOneLine(), "Array(UInt64)");

    auto fragments = std::move(*prepared).finishIndividualColumns(tableObject(), 9);
    ASSERT_EQ(fragments.size(), declarations.size());
    ASSERT_TRUE(fragments[0]);
    EXPECT_FALSE(fragments[1]);
    ASSERT_TRUE(fragments[2]);
    EXPECT_EQ(fragments[0]->object, tableObject());
    EXPECT_EQ(fragments[0]->object_schema_revision, 9u);
    ASSERT_EQ(fragments[0]->occurrence_paths.size(), 1u);
    EXPECT_TRUE(fragments[0]->occurrence_paths.front().type_child_ordinals.empty());
    EXPECT_EQ(fragments[2]->object, tableObject());
    EXPECT_EQ(fragments[2]->object_schema_revision, 9u);
    ASSERT_EQ(fragments[2]->occurrence_paths.size(), 1u);
    EXPECT_EQ(fragments[2]->occurrence_paths.front().type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(authority.session_calls, 1u);
}

TEST(UDTScalarAliasColumnBinder, FinishRejectsAnyNormalizedPhysicalSchemaDrift)
{
    ASTPtr ast = parseCreate("CREATE TABLE app.events (id app.UserId) ENGINE = Memory");
    auto & create = asCreate(ast);
    const auto definition = checkedAlias("UserId");
    const auto authority = makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition});
    const auto context = Context::createCopy(getContext().context);

    auto prepared = prepareScalarAliasColumns(create, "app", context, *authority);
    ASSERT_TRUE(prepared);
    prepared->applyPhysicalTypeASTs();
    NamesAndTypesList drifted = prepared->getExpectedPhysicalColumns();
    drifted.front().name = "renamed";
    expectBinderError(
        ScalarAliasColumnBinderError::Code::NormalizedSchemaMismatch,
        [&] { static_cast<void>(std::move(*prepared).finish(tableObject(), 1, drifted)); });
}

}
