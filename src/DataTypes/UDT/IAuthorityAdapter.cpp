#include <DataTypes/UDT/IAuthorityAdapter.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::UDT
{

IAuthorityAdapter::ResolutionSession::ResolutionSession(
    std::shared_ptr<const void> lifetime_token_, Catalog::ResolutionSession catalog_session_)
    : backend(std::in_place_index<0>, std::move(lifetime_token_), std::move(catalog_session_))
{
}

IAuthorityAdapter::ResolutionSession::CatalogBackend::CatalogBackend(
    std::shared_ptr<const void> lifetime_token_, Catalog::ResolutionSession catalog_session_)
    : lifetime_token(std::move(lifetime_token_))
    , catalog_session(std::move(catalog_session_))
{
}

IAuthorityAdapter::ResolutionSession::SnapshotBackend::SnapshotBackend(
    const void * view_,
    SnapshotResolutionOperations operations_,
    void * release_context_,
    std::size_t release_token_,
    SnapshotRelease release_) noexcept
    : view(view_)
    , operations(operations_)
    , release_context(release_context_)
    , release_token(release_token_)
    , release(release_)
{
}

IAuthorityAdapter::ResolutionSession::SnapshotBackend::SnapshotBackend(SnapshotBackend && other) noexcept
    : view(std::exchange(other.view, nullptr))
    , operations(other.operations)
    , release_context(std::exchange(other.release_context, nullptr))
    , release_token(other.release_token)
    , release(std::exchange(other.release, nullptr))
{
}

IAuthorityAdapter::ResolutionSession::SnapshotBackend::~SnapshotBackend()
{
    if (release)
        release(release_context, release_token);
}

IAuthorityAdapter::ResolutionSession::ResolutionSession(
    const void * view_,
    SnapshotResolutionOperations operations_,
    void * release_context_,
    std::size_t release_token_,
    SnapshotRelease release_) noexcept
    : backend(std::in_place_index<1>, view_, operations_, release_context_, release_token_, release_)
{
}

Definition::Ptr
IAuthorityAdapter::ResolutionSession::findByIdentity(const DefinitionIdentity & identity) const
{
    if (const auto * catalog_session = std::get_if<0>(&backend))
        return catalog_session->catalog_session.findByIdentity(identity);
    const auto & snapshot = std::get<1>(backend);
    return snapshot.operations.find_by_identity(snapshot.view, identity);
}

Definition::Ptr IAuthorityAdapter::ResolutionSession::findByName(std::string_view normalized_local_name) const
{
    if (const auto * catalog_session = std::get_if<0>(&backend))
        return catalog_session->catalog_session.findByName(normalized_local_name);
    const auto & snapshot = std::get<1>(backend);
    return snapshot.operations.find_by_name(snapshot.view, normalized_local_name);
}

UInt64 IAuthorityAdapter::ResolutionSession::getGeneration() const noexcept
{
    if (const auto * catalog_session = std::get_if<0>(&backend))
        return catalog_session->catalog_session.getGeneration();
    const auto & snapshot = std::get<1>(backend);
    return snapshot.operations.get_generation(snapshot.view);
}

const EffectiveResourceLimits * IAuthorityAdapter::ResolutionSession::getEffectiveResourceLimits() const noexcept
{
    if (std::holds_alternative<CatalogBackend>(backend))
        return nullptr;
    const auto & snapshot = std::get<SnapshotBackend>(backend);
    return snapshot.operations.get_effective_resource_limits ? snapshot.operations.get_effective_resource_limits(snapshot.view) : nullptr;
}

IAuthorityAdapter::ResolutionSession IAuthorityAdapter::makeResolutionSession(
    std::shared_ptr<const void> lifetime_token, Catalog::ResolutionSession catalog_session)
{
    return ResolutionSession(std::move(lifetime_token), std::move(catalog_session));
}

IAuthorityAdapter::ResolutionSession IAuthorityAdapter::makeSnapshotResolutionSession(
    const void * view,
    SnapshotResolutionOperations operations,
    void * release_context,
    std::size_t release_token,
    SnapshotRelease release) noexcept
{
    return ResolutionSession(view, operations, release_context, release_token, release);
}

namespace
{

constexpr TypeAuthorityCapabilityMask transient_allowed_capabilities = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
    | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates)
    | typeAuthorityCapabilityBit(TypeAuthorityCapability::DecreasingRecursion);

constexpr TypeAuthorityCapabilityMask transientRequiredCapabilities
    = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
    | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);

[[noreturn]] void invalidAuthority(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid transient user-defined type authority: {}", message);
}

TypeCatalogRoot::Ptr validateAndBuildTransientRoot(
    UUID database_uuid, const TypeAuthorityCapabilities & capabilities, std::span<const Definition::Ptr> definitions)
{
    if (database_uuid == UUIDHelpers::Nil)
        invalidAuthority("database UUID is nil");
    if (capabilities.adapter_abi != 1)
        invalidAuthority("adapter ABI is unsupported");
    if ((capabilities.mask & ~transient_allowed_capabilities) != 0)
        invalidAuthority("a transient authority cannot advertise durable or unknown capabilities");
    if (!capabilities.containsAll(transientRequiredCapabilities))
        invalidAuthority("required transient-authority capabilities are absent");

    const auto & authority_limits = capabilities.limits;
    if (authority_limits.maximum_definitions == 0 || authority_limits.maximum_definition_bytes == 0
        || authority_limits.maximum_template_nodes == 0 || authority_limits.maximum_direct_dependencies == 0
        || authority_limits.maximum_transitive_dependencies == 0 || authority_limits.maximum_checker_work == 0)
    {
        invalidAuthority("every advertised limit must be nonzero");
    }
    if (definitions.size() > authority_limits.maximum_definitions)
        invalidAuthority("definition count exceeds its limit");
    for (const auto & definition : definitions)
    {
        if (!definition || definition->getIdentity().database_uuid != database_uuid)
            invalidAuthority("a definition is null or belongs to another database");
        if (definition->getNodes().size() > authority_limits.maximum_template_nodes)
            invalidAuthority("a definition exceeds the template-node limit");
        if (definition->getDependencies().size() > authority_limits.maximum_direct_dependencies)
            invalidAuthority("a definition exceeds the direct-dependency limit");
        if (definition->getCertificate().transitive_dependency_count > authority_limits.maximum_transitive_dependencies)
            invalidAuthority("a definition exceeds the transitive-dependency limit");
        if (definition->getCertificate().charged_work > authority_limits.maximum_checker_work)
            invalidAuthority("a definition exceeds the checker-work limit");
        if (!tryCountLogicalRetainedDefinitionBytes(*definition, authority_limits.maximum_definition_bytes))
            invalidAuthority("definition retained bytes exceed their limit");
    }

    TypeCatalogBuildLimits catalog_limits;
    catalog_limits.maximum_definitions = authority_limits.maximum_definitions;
    const UInt64 aggregate_definition_bytes
        = authority_limits.maximum_definition_bytes > std::numeric_limits<UInt64>::max() / authority_limits.maximum_definitions
        ? std::numeric_limits<UInt64>::max()
        : authority_limits.maximum_definition_bytes * authority_limits.maximum_definitions;
    catalog_limits.maximum_root_accounted_bytes = std::min(catalog_limits.maximum_root_accounted_bytes, aggregate_definition_bytes);
    return TypeCatalogBuilder::build(database_uuid, 1, definitions, catalog_limits);
}

class TransientAuthorityState final
{
public:
    explicit TransientAuthorityState(TypeCatalogRoot::Ptr root)
        : catalog(std::move(root))
    {
    }

    Catalog catalog;
};

class UnsupportedAuthorityAdapter final : public IAuthorityAdapter
{
public:
    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return UUIDHelpers::Nil; }
    ResolutionSession beginResolutionSession() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "User-defined type authority does not support binding");
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        if (required != 0)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "User-defined type authority does not support {}", operation);
    }

private:
    const TypeAuthorityCapabilities capabilities{};
};

class TransientAuthorityAdapter final : public IAuthorityAdapter
{
public:
    TransientAuthorityAdapter(
        UUID database_uuid_, TypeAuthorityCapabilities capabilities_, std::vector<Definition::Ptr> definitions_)
        : database_uuid(database_uuid_)
        , capabilities(std::move(capabilities_))
        , state(std::make_shared<TransientAuthorityState>(validateAndBuildTransientRoot(database_uuid, capabilities, definitions_)))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return database_uuid; }
    ResolutionSession beginResolutionSession() const override
    {
        auto catalog_session = state->catalog.beginResolutionSession();
        return makeResolutionSession(state, std::move(catalog_session));
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        if (!capabilities.containsAll(required))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "User-defined type authority lacks capabilities required for {}", operation);
    }

private:
    const UUID database_uuid;
    const TypeAuthorityCapabilities capabilities;
    std::shared_ptr<const TransientAuthorityState> state;
};

}

const IAuthorityAdapter & getUnsupportedAuthorityAdapter() noexcept
{
    static const UnsupportedAuthorityAdapter adapter;
    return adapter;
}

AuthorityAdapterPtr makeUnsupportedAuthorityAdapter() noexcept
{
    return AuthorityAdapterPtr(AuthorityAdapterPtr{}, &getUnsupportedAuthorityAdapter());
}

AuthorityAdapterPtr makeTransientAuthorityAdapter(
    UUID database_uuid, TypeAuthorityCapabilities capabilities, std::vector<Definition::Ptr> definitions)
{
    return std::make_shared<TransientAuthorityAdapter>(database_uuid, std::move(capabilities), std::move(definitions));
}

}
