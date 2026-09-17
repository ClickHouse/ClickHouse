#pragma once

#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>

#include <cstddef>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class CatalogError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidDefinition,
        DuplicateIdentity,
        DuplicateName,
        MissingIdentity,
        LimitExceeded,
        HazardSlotsExhausted,
        GenerationMismatch,
        Shutdown,
    };

    CatalogError(Code code_, std::string_view message);

    Code code;
};

/// Builder limits are checked prospectively before constructing either index.
/// The byte limit uses a deterministic conservative ownership model, including
/// immutable definition payloads retained by the root and both flat indexes.
struct TypeCatalogBuildLimits
{
    /// Fixed production layout selected by local high-scale characterization:
    /// 1/10k/100k catalogs retained one-shard mutation and bounded lookup work
    /// with 64 shards. The count is part of an immutable root's layout and is
    /// revalidated on every successor; it is not adapted from live data.
    std::size_t shard_count = 64;
    UInt64 maximum_definitions = 100'000;
    UInt64 maximum_normalized_name_length = 4ULL << 10;
    UInt64 maximum_normalized_name_bytes = 64ULL << 20;
    UInt64 maximum_root_accounted_bytes = 512ULL << 20;
    /// These bounds make one immutable-shard path copy independent of total
    /// catalog size, including under adversarial shard placement. They are
    /// checked before any map allocation or publication.
    UInt64 maximum_identity_shard_entries = 4'096;
    UInt64 maximum_name_shard_entries = 4'096;
    UInt64 maximum_shard_accounted_bytes = 32ULL << 20;
    /// Explicit per-authority keys keep shard placement reproducible without a
    /// process-global seed. Deployments may provide independently generated
    /// keys when adversarially chosen names are in scope.
    UInt64 shard_hash_key0 = 0xa0761d6478bd642fULL;
    UInt64 shard_hash_key1 = 0xe7037ed1a0b428dbULL;
};

struct TypeCatalogPublicationLimits
{
    std::size_t hazard_slot_count = 256;
    UInt64 maximum_retired_root_count = 8;
    UInt64 maximum_retired_root_bytes = 1ULL << 30;
};

struct TypeCatalogShardAccounting
{
    UInt64 entries = 0;
    UInt64 accounted_bytes = 0;

    bool operator==(const TypeCatalogShardAccounting &) const = default;
};

struct TypeCatalogRetirementState
{
    UInt64 retired_root_count = 0;
    UInt64 retired_root_bytes = 0;
    UInt64 active_hazard_slots = 0;
    bool shutdown = false;

    bool operator==(const TypeCatalogRetirementState &) const = default;
};

/// A complete immutable authority snapshot. It owns two independently sharded
/// absl::flat_hash_map indexes. No method exposes a map, shard, root pointer, or
/// generation lease; every successful lookup copies the immutable definition
/// handle while its caller already owns or hazards this root.
class TypeCatalogRoot final
{
public:
    using Ptr = std::unique_ptr<const TypeCatalogRoot>;

    TypeCatalogRoot(const TypeCatalogRoot &) = delete;
    TypeCatalogRoot & operator=(const TypeCatalogRoot &) = delete;
    TypeCatalogRoot(TypeCatalogRoot &&) = delete;
    TypeCatalogRoot & operator=(TypeCatalogRoot &&) = delete;
    ~TypeCatalogRoot();

    const UUID & getDatabaseUUID() const noexcept;
    UInt64 getGeneration() const noexcept;
    UInt64 getDefinitionCount() const noexcept;
    UInt64 getAccountedBytes() const noexcept;
    std::size_t getShardCount() const noexcept;
    UInt64 getShardHashKey0() const noexcept;
    UInt64 getShardHashKey1() const noexcept;
    UInt64 getMaximumIdentityShardEntries() const noexcept;
    UInt64 getMaximumNameShardEntries() const noexcept;
    UInt64 getMaximumShardAccountedBytes() const noexcept;
    UInt64 getMaximumNormalizedNameLength() const noexcept;

    TypeCatalogShardAccounting getIdentityShardAccounting(std::size_t shard) const;
    TypeCatalogShardAccounting getNameShardAccounting(std::size_t shard) const;
    bool sharesIdentityShardWith(const TypeCatalogRoot & other, std::size_t shard) const;
    bool sharesNameShardWith(const TypeCatalogRoot & other, std::size_t shard) const;

    /// Offline root lookups are safe while the caller owns the root. Live
    /// publication users must call Catalog instead.
    Definition::Ptr findByIdentity(const DefinitionIdentity & identity) const;
    /// Looks up one database-local normalized identifier. Database
    /// qualification is resolved before entering this per-authority root.
    Definition::Ptr findByName(std::string_view normalized_local_name) const;

private:
    class Impl;

    explicit TypeCatalogRoot(std::unique_ptr<const Impl> impl_);

    friend class TypeCatalogBuilder;
    std::unique_ptr<const Impl> impl;
};

/// One bounded immutable-root edit. Replace is an atomic remove-plus-add and
/// therefore covers a same-identity diagnostic rename as well as a
/// revision/identity rollover. A same-identity replacement must preserve the
/// complete checked semantics. Construction helpers prevent ambiguous
/// inactive fields.
class TypeCatalogMutation final
{
public:
    enum class Kind : UInt8
    {
        Add,
        Replace,
        Remove,
    };

    static TypeCatalogMutation add(Definition::Ptr definition);
    static TypeCatalogMutation replace(DefinitionIdentity old_identity, Definition::Ptr definition);
    static TypeCatalogMutation remove(DefinitionIdentity identity);

    Kind getKind() const noexcept { return kind; }

private:
    TypeCatalogMutation(Kind kind_, DefinitionIdentity old_identity_, Definition::Ptr definition_);

    friend class TypeCatalogBuilder;
    Kind kind;
    DefinitionIdentity old_identity;
    Definition::Ptr definition;
};

class TypeCatalogBuilder final
{
public:
    /// Builds a root for an explicit database authority. This overload is
    /// required for an empty initial authority snapshot.
    [[nodiscard]] static TypeCatalogRoot::Ptr build(
        UUID database_uuid,
        UInt64 generation,
        std::span<const Definition::Ptr> definitions,
        const TypeCatalogBuildLimits & limits = {});

    /// Convenience for a non-empty definition set; derives the authority UUID
    /// from its first definition and rejects an empty set.
    [[nodiscard]] static TypeCatalogRoot::Ptr
    build(UInt64 generation, std::span<const Definition::Ptr> definitions, const TypeCatalogBuildLimits & limits = {});

    /// Path-copies only the affected identity/name shards and shares every
    /// untouched shard with base. All collision/accounting/quota checks finish
    /// before allocating a replacement root or a cloned shard.
    [[nodiscard]] static TypeCatalogRoot::Ptr applyMutation(
        const TypeCatalogRoot & base, UInt64 generation, const TypeCatalogMutation & mutation, const TypeCatalogBuildLimits & limits = {});

    /// One-generation batch removal used by bounded schema transactions.
    /// Identities must be strictly sorted. Only affected immutable shards are
    /// copied; untouched shards are shared with `base`.
    [[nodiscard]] static TypeCatalogRoot::Ptr removeDefinitions(
        const TypeCatalogRoot & base,
        UInt64 generation,
        std::span<const DefinitionIdentity> sorted_identities,
        const TypeCatalogBuildLimits & limits = {});

private:
    TypeCatalogBuilder() = delete;
};

/// Per-authority immutable-root publication. Readers perform one acquire root
/// load, publish a seq_cst hazard, recheck the root, copy one definition Ptr,
/// and clear the hazard in O(1). They never lock, scan retirement state, or
/// destroy a root. The authority's writer/admin explicitly calls scanRetired;
/// no process-global registry or worker is created.
class Catalog final
{
public:
    /// Opaque snapshot-consistent multi-lookup scope. It pins exactly one root
    /// with one hazard slot, never exposes that root, and clears the slot in
    /// O(1) without performing reclamation.
    class ResolutionSession final
    {
    public:
        ResolutionSession(const ResolutionSession &) = delete;
        ResolutionSession & operator=(const ResolutionSession &) = delete;
        ResolutionSession(ResolutionSession &&) noexcept;
        ResolutionSession & operator=(ResolutionSession &&) = delete;
        ~ResolutionSession();

        Definition::Ptr findByIdentity(const DefinitionIdentity & identity) const;
        Definition::Ptr findByName(std::string_view normalized_local_name) const;
        UInt64 getGeneration() const noexcept;

    private:
        explicit ResolutionSession(void * hazard_domain_, std::size_t hazard_slot_, const TypeCatalogRoot * root_) noexcept;

        friend class Catalog;
        void * hazard_domain = nullptr;
        std::size_t hazard_slot = 0;
        const TypeCatalogRoot * root = nullptr;
    };

    explicit Catalog(TypeCatalogRoot::Ptr initial_root, const TypeCatalogPublicationLimits & limits = {});
    ~Catalog();

    Catalog(const Catalog &) = delete;
    Catalog & operator=(const Catalog &) = delete;
    Catalog(Catalog &&) = delete;
    Catalog & operator=(Catalog &&) = delete;

    Definition::Ptr findByIdentity(const DefinitionIdentity & identity) const;
    Definition::Ptr findByName(std::string_view normalized_local_name) const;
    [[nodiscard]] ResolutionSession beginResolutionSession() const;

    /// All checks, retired-backlog admission, and vector capacity reservation
    /// finish before the acq_rel exchange. After that exchange, publication is
    /// a no-throw ownership transfer.
    void publish(TypeCatalogRoot::Ptr next_root);

    /// Writer/admin-thread reclamation adapter. Potentially large root
    /// destruction happens after releasing the writer mutex.
    TypeCatalogRetirementState scanRetired();
    TypeCatalogRetirementState getRetirementState() const;

    /// Stops new leases, removes the active root, and explicitly scans until
    /// all bounded in-flight leases have cleared. The destructor invokes the
    /// same drain; callers should call it at authority detach for diagnostics.
    void shutdownAndDrain();
    bool isShutdown() const noexcept;
    UInt64 currentGeneration() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
