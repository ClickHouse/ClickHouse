#pragma once

#include <Core/Types.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>

#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace DB::UDT
{

/// DDL-local proof retained at the stable analyzer generation, before logical
/// roles are erased from the executable query tree.  Physical outputs carry no
/// source pointers.  Exact outputs carry either an explicit declared-type tree
/// (possibly sliced by an approved static child) or one immutable prebound
/// object snapshot plus its exact declaration owner/path prefix.
struct SelectedOutputTypeBinding final
{
    String output_name;
    DataTypePtr physical_type;
    BoundDeclaredTypeTree::Ptr explicit_logical_tree;
    std::vector<UInt64> explicit_type_child_prefix;
    BoundObjectTypeReferences::Ptr prebound_references;
    PersistedTypePathSection prebound_section = PersistedTypePathSection::ColumnType;
    String prebound_runtime_owner_key;
    std::vector<UInt64> prebound_type_child_prefix;

    bool isPhysicalOnly() const noexcept { return !explicit_logical_tree && !prebound_references; }

    bool isValid() const noexcept
    {
        const bool has_explicit = static_cast<bool>(explicit_logical_tree);
        const bool has_prebound = static_cast<bool>(prebound_references);
        return !output_name.empty() && physical_type && !(has_explicit && has_prebound)
            && (!has_explicit || prebound_runtime_owner_key.empty())
            && (!has_prebound || (!prebound_runtime_owner_key.empty() && explicit_type_child_prefix.empty()))
            && (has_explicit || explicit_type_child_prefix.empty())
            && (has_prebound || (prebound_runtime_owner_key.empty() && prebound_type_child_prefix.empty()));
    }
};

using SelectedOutputTypeBindings = std::vector<SelectedOutputTypeBinding>;

enum class SelectedOutputTypeBindingCollectionKind : UInt8
{
    /// The analyzer saw neither an explicit logical type state nor bound UDT
    /// references on any already-resolved source snapshot.  No per-output
    /// Physical vector or provenance index was allocated.
    NoLogicalSourceFastPath,
    CompleteBindings,
};

struct SelectedOutputTypeBindingCollection final
{
    SelectedOutputTypeBindingCollectionKind kind = SelectedOutputTypeBindingCollectionKind::NoLogicalSourceFastPath;
    SelectedOutputTypeBindings bindings;
};

/// Shared only across Context copies participating in one DDL analysis.  One
/// stable QueryAnalysisPass publishes exactly once and the DDL owner consumes
/// exactly once; a second publisher/consumer fails closed.
class SelectedOutputTypeBindingCollector final
{
public:
    explicit SelectedOutputTypeBindingCollector(bool complete_bindings_required_ = false)
        : complete_bindings_required(complete_bindings_required_)
    {
    }

    bool requiresCompleteBindings() const noexcept { return complete_bindings_required; }

    bool tryClaimPublisher(const void * owner)
    {
        if (!owner)
            return false;
        std::lock_guard lock(mutex);
        if (failed || published || consumed)
            return false;
        if (!publisher_owner)
            publisher_owner = owner;
        return publisher_owner == owner;
    }

    /// Commits publication only after the complete analyzer pass, including
    /// its final semantic/lifetime barriers, returned successfully.
    bool markPublisherComplete(const void * owner) noexcept
    {
        std::lock_guard lock(mutex);
        if (failed || consumed || publisher_owner != owner || !published || !result)
            return false;
        publication_complete = true;
        publisher_owner = nullptr;
        return true;
    }

    /// Terminates a claimed collection when analyzer resolution unwinds. A
    /// result published before a later finalization failure is discarded too;
    /// another analyzer generation must not replace it.
    void abandonPublisher(const void * owner) noexcept
    {
        if (!owner)
            return;
        std::lock_guard lock(mutex);
        if (!publication_complete && !consumed && publisher_owner == owner)
        {
            result.reset();
            publisher_owner = nullptr;
            failed = true;
        }
    }

    bool publishNoLogicalSourceFastPath()
    {
        return publishResult({
            .kind = SelectedOutputTypeBindingCollectionKind::NoLogicalSourceFastPath,
            .bindings = {},
        });
    }

    bool publish(SelectedOutputTypeBindings value)
    {
        return publishResult({
            .kind = SelectedOutputTypeBindingCollectionKind::CompleteBindings,
            .bindings = std::move(value),
        });
    }

    std::optional<SelectedOutputTypeBindingCollection> take()
    {
        std::lock_guard lock(mutex);
        if (!publication_complete || consumed || !result)
            return std::nullopt;
        consumed = true;
        auto value = std::move(result);
        result.reset();
        return value;
    }

private:
    bool publishResult(SelectedOutputTypeBindingCollection value)
    {
        std::lock_guard lock(mutex);
        if (failed || published || consumed || !publisher_owner)
            return false;
        result.emplace(std::move(value));
        published = true;
        return true;
    }

    std::mutex mutex;
    std::optional<SelectedOutputTypeBindingCollection> result;
    const void * publisher_owner = nullptr;
    bool failed = false;
    bool published = false;
    bool publication_complete = false;
    bool consumed = false;
    const bool complete_bindings_required = false;
};

}
