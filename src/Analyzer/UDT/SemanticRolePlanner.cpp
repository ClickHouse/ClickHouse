#include <Analyzer/UDT/QueryTreeSemanticRoleGraph.h>
#include <Analyzer/UDT/SemanticRolePlanner.h>

#include <Common/ProfileEvents.h>
#include <Common/StringHashForHeterogeneousLookup.h>

#include <Core/UUID.h>

#include <DataTypes/UDT/ResourceAccounting.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event UDTQueryDefinitionHandles;
extern const Event UDTSemanticAnalysisActivations;
extern const Event UDTSemanticAnalysisNodesVisited;
extern const Event UDTSemanticAnalysisEdgesVisited;
extern const Event UDTSemanticRolesInterned;
extern const Event UDTSemanticRoleConflicts;
extern const Event UDTSchemaBoundRoleUses;
}

namespace DB::UDT
{
namespace
{

using Error = SemanticRolePlannerError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

constexpr UInt64 hash_node_overhead = 3 * sizeof(void *);

void requireAdmission(ProspectiveResourceBudget & budget, ResourceDelta delta, UInt64 scratch_bytes)
{
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, scratch_bytes);
    const auto admission = budget.charge(delta);
    if (!admission.isAccepted())
        fail(Error::Code::LimitExceeded, formatResourceAdmissionFailure(admission));
}

constexpr SemanticRolePlannerLimits implementation_maxima{
    .maximum_sinks = 65'536,
    .maximum_demanded_states = 1ULL << 20,
    .maximum_inspected_edges = 1ULL << 22,
    .maximum_logical_paths = 1ULL << 20,
    .maximum_shapes = 1ULL << 20,
    .maximum_roles = 1ULL << 20,
    .maximum_owned_definition_handles = 65'536,
    .maximum_active_depth = 65'536,
    .maximum_shape_bytes = 1ULL << 30,
    .maximum_single_shape_bytes = 1ULL << 20,
    .maximum_role_argument_bytes = 1ULL << 30,
    .maximum_single_role_argument_bytes = 1ULL << 20,
    .maximum_combined_scratch_bytes = 1ULL << 30,
    .maximum_literal_bytes = 1ULL << 30,
    .maximum_conflicts = 65'536,
};

void validateLimits(const SemanticRolePlannerLimits & limits)
{
    const std::array<UInt64, 15> configured{
        limits.maximum_sinks,
        limits.maximum_demanded_states,
        limits.maximum_inspected_edges,
        limits.maximum_logical_paths,
        limits.maximum_shapes,
        limits.maximum_roles,
        limits.maximum_owned_definition_handles,
        limits.maximum_active_depth,
        limits.maximum_shape_bytes,
        limits.maximum_single_shape_bytes,
        limits.maximum_role_argument_bytes,
        limits.maximum_single_role_argument_bytes,
        limits.maximum_combined_scratch_bytes,
        limits.maximum_literal_bytes,
        limits.maximum_conflicts,
    };
    const std::array<UInt64, 15> maxima{
        implementation_maxima.maximum_sinks,
        implementation_maxima.maximum_demanded_states,
        implementation_maxima.maximum_inspected_edges,
        implementation_maxima.maximum_logical_paths,
        implementation_maxima.maximum_shapes,
        implementation_maxima.maximum_roles,
        implementation_maxima.maximum_owned_definition_handles,
        implementation_maxima.maximum_active_depth,
        implementation_maxima.maximum_shape_bytes,
        implementation_maxima.maximum_single_shape_bytes,
        implementation_maxima.maximum_role_argument_bytes,
        implementation_maxima.maximum_single_role_argument_bytes,
        implementation_maxima.maximum_combined_scratch_bytes,
        implementation_maxima.maximum_literal_bytes,
        implementation_maxima.maximum_conflicts,
    };

    bool has_zero = false;
    bool exceeds_maximum = false;
    for (std::size_t index = 0; index < configured.size(); ++index)
    {
        has_zero = has_zero || configured[index] == 0;
        exceeds_maximum = exceeds_maximum || configured[index] > maxima[index];
    }

    if (has_zero)
        fail(Error::Code::InvalidConfiguration, "every semantic-role planner limit must be nonzero");
    if (exceeds_maximum)
        fail(Error::Code::InvalidConfiguration, "a semantic-role planner limit exceeds its implementation maximum");
    if (limits.maximum_active_depth > limits.maximum_demanded_states)
        fail(Error::Code::InvalidConfiguration, "semantic-role active depth exceeds the demanded-state limit");
    if (limits.maximum_single_shape_bytes > limits.maximum_shape_bytes
        || limits.maximum_single_role_argument_bytes > limits.maximum_role_argument_bytes)
        fail(Error::Code::InvalidConfiguration, "a semantic-role item byte limit exceeds its aggregate limit");
    if (limits.maximum_shape_bytes > limits.maximum_combined_scratch_bytes
        || limits.maximum_role_argument_bytes > limits.maximum_combined_scratch_bytes)
        fail(Error::Code::InvalidConfiguration, "a semantic-role component byte limit exceeds the combined scratch limit");
}

UInt64 mix(UInt64 value) noexcept
{
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

UInt64 hashBytes(std::span<const CanonicalByte> bytes) noexcept
{
    UInt64 result = 0x9e3779b97f4a7c15ULL;
    for (const auto byte : bytes)
        result = mix(result ^ byte);
    return result;
}

UInt64 hashDefinitionIdentity(const DefinitionIdentity & identity) noexcept
{
    UInt64 result = mix(UUIDHelpers::getHighBytes(identity.database_uuid));
    result = mix(result ^ UUIDHelpers::getLowBytes(identity.database_uuid));
    result = mix(result ^ UUIDHelpers::getHighBytes(identity.type_uuid));
    result = mix(result ^ UUIDHelpers::getLowBytes(identity.type_uuid));
    return mix(result ^ identity.revision);
}

UInt64 hashInstantiation(
    const DefinitionIdentity & identity,
    const Digest & definition_hash,
    std::string_view arguments,
    const Digest & instantiation_hash) noexcept
{
    UInt64 result = hashDefinitionIdentity(identity);
    result = mix(result ^ hashBytes(definition_hash));
    result = mix(result ^ std::hash<std::string_view>{}(arguments));
    return mix(result ^ hashBytes(instantiation_hash));
}

struct SemanticNodePathHash
{
    std::size_t operator()(const SemanticNodePath & state) const noexcept
    {
        return static_cast<std::size_t>(mix(state.node ^ (static_cast<UInt64>(state.path) << 32)));
    }
};

struct LogicalRoleView
{
    const PersistedTypeDescriptor & descriptor;
    LogicalShapeID shape;
};

struct ReshapedLogicalRoleView
{
    const InternedLogicalRole & source;
    LogicalShapeID shape;
};

struct LogicalRoleHash
{
    using is_transparent = void;

    std::size_t operator()(const InternedLogicalRole & role) const noexcept
    {
        return static_cast<std::size_t>(
            mix(hashInstantiation(
                    role.definition_identity, role.definition_hash, role.canonical_arguments_encoding, role.instantiation_semantic_hash)
                ^ role.shape));
    }

    std::size_t operator()(const LogicalRoleView & role) const noexcept
    {
        return static_cast<std::size_t>(
            mix(hashInstantiation(
                    role.descriptor.getDefinitionIdentity(),
                    role.descriptor.getDefinitionHash(),
                    role.descriptor.getCanonicalArgumentsEncoding(),
                    role.descriptor.getInstantiationSemanticHash())
                ^ role.shape));
    }

    std::size_t operator()(const ReshapedLogicalRoleView & role) const noexcept
    {
        return static_cast<std::size_t>(
            mix(hashInstantiation(
                    role.source.definition_identity,
                    role.source.definition_hash,
                    role.source.canonical_arguments_encoding,
                    role.source.instantiation_semantic_hash)
                ^ role.shape));
    }
};

struct LogicalRoleEqual
{
    using is_transparent = void;

    bool operator()(const InternedLogicalRole & lhs, const InternedLogicalRole & rhs) const noexcept { return lhs == rhs; }

    bool operator()(const InternedLogicalRole & lhs, const LogicalRoleView & rhs) const noexcept
    {
        return lhs.definition_identity == rhs.descriptor.getDefinitionIdentity()
            && lhs.definition_hash == rhs.descriptor.getDefinitionHash()
            && lhs.canonical_arguments_encoding == rhs.descriptor.getCanonicalArgumentsEncoding()
            && lhs.instantiation_semantic_hash == rhs.descriptor.getInstantiationSemanticHash() && lhs.shape == rhs.shape;
    }

    bool operator()(const InternedLogicalRole & lhs, const ReshapedLogicalRoleView & rhs) const noexcept
    {
        return lhs.definition_identity == rhs.source.definition_identity && lhs.definition_hash == rhs.source.definition_hash
            && lhs.canonical_arguments_encoding == rhs.source.canonical_arguments_encoding
            && lhs.instantiation_semantic_hash == rhs.source.instantiation_semantic_hash && lhs.shape == rhs.shape;
    }
};

struct DigestHash
{
    std::size_t operator()(const Digest & digest) const noexcept { return static_cast<std::size_t>(hashBytes(digest)); }
};

enum class MemoState : UInt8
{
    Visiting,
    Done,
};

enum class PlannerLifecycle : UInt8
{
    Open,
    Sealed,
};

struct MemoEntry
{
    MemoState state = MemoState::Visiting;
    RoleProof proof = RoleProof::noRole();
};

struct StoredSink
{
    SemanticNodePath source;
    bool observes_identity = false;
    LogicalRoleID expected_role = invalid_logical_role_id;
    QueryDefinitionHandleID retained_definition_handle = invalid_query_definition_handle_id;
};

struct ProofFrame
{
    SemanticNodePath state;
    SemanticRoleNode node;
    const SemanticTransferDescriptor * transfer = nullptr;
    UInt32 next_input = 0;
    SemanticNodePath pending_child;
    bool awaiting_child = false;
    bool has_aggregate = false;
    RoleProof aggregate = RoleProof::noRole();
};

}

struct SemanticRolePlanner::Impl
{
    Impl(
        const QueryTreeSemanticRoleGraph & graph_,
        ProspectiveResourceBudget & query_resource_budget_,
        SemanticRolePlannerLimits limits_,
        UInt64 admitted_base_scratch_bytes)
        : generation(graph_.getGeneration())
        , graph(graph_)
        , query_resource_budget(query_resource_budget_)
        , limits(limits_)
    {
        statistics.semantic_scratch_bytes = admitted_base_scratch_bytes;
    }

    void charge(ResourceDelta delta, UInt64 scratch_bytes)
    {
        if (statistics.semantic_scratch_bytes > limits.maximum_combined_scratch_bytes
            || scratch_bytes > limits.maximum_combined_scratch_bytes - statistics.semantic_scratch_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role scratch bytes exceed their local implementation limit");
        requireAdmission(query_resource_budget, std::move(delta), scratch_bytes);
        statistics.semantic_scratch_bytes += scratch_bytes;
    }

    void chargeDistinctDescriptor(const PersistedTypeDescriptor & descriptor)
    {
        const auto admission = query_resource_budget.chargeDistinctDescriptor(
            descriptor.getDefinitionIdentity(), descriptor.getCanonicalArgumentsEncoding());
        if (!admission.isAccepted())
            fail(Error::Code::LimitExceeded, formatResourceAdmissionFailure(admission));
    }

    void ensureProofStackCapacity(std::vector<ProofFrame> & stack, std::size_t required_capacity)
    {
        if (required_capacity <= stack.capacity())
            return;

        const auto maximum_capacity = static_cast<std::size_t>(limits.maximum_active_depth);
        if (required_capacity > maximum_capacity)
            fail(Error::Code::LimitExceeded, "semantic-role active proof depth exceeds its limit");

        auto next_capacity = stack.capacity() ? std::min(maximum_capacity, stack.capacity() * 2) : std::size_t{1};
        next_capacity = std::max(next_capacity, required_capacity);
        const UInt64 scratch_bytes = static_cast<UInt64>(next_capacity - stack.capacity()) * sizeof(ProofFrame);
        charge({}, scratch_bytes);
        stack.reserve(next_capacity);
    }

    void chargeAndEnsureSinkCapacity(ResourceDelta delta, std::size_t required_capacity)
    {
        const auto maximum_capacity = static_cast<std::size_t>(limits.maximum_sinks);
        if (required_capacity > maximum_capacity)
            fail(Error::Code::LimitExceeded, "semantic-role eligible sinks exceed their limit");

        if (required_capacity <= sinks.capacity())
        {
            charge(std::move(delta), 0);
            return;
        }

        const std::size_t doubled_capacity
            = sinks.capacity() > maximum_capacity / 2 ? maximum_capacity : std::max<std::size_t>(1, sinks.capacity() * 2);
        const std::size_t next_capacity = std::max(required_capacity, doubled_capacity);
        const UInt64 scratch_bytes = static_cast<UInt64>(next_capacity - sinks.capacity()) * sizeof(StoredSink);
        charge(std::move(delta), scratch_bytes);
        sinks.reserve(next_capacity);
    }

    void checkGeneration() const
    {
        if (graph.getGeneration() != generation)
            fail(Error::Code::StaleGeneration, "semantic-role query graph generation changed while planner state was live");
        if (!graph.isSealed())
            fail(Error::Code::InvalidGraph, "semantic-role query graph became open while planner state was live");
    }

    void checkUsable() const
    {
        if (poisoned)
            fail(Error::Code::InvalidGraph, "semantic-role planner is unusable after a failed proof attempt");
        checkGeneration();
    }

    void checkOpen() const
    {
        checkUsable();
        if (lifecycle == PlannerLifecycle::Sealed)
            fail(Error::Code::MutableAfterSeal, "semantic-role planner cannot be changed or execute new proofs after seal");
    }

    void checkSealed() const
    {
        checkUsable();
        if (lifecycle != PlannerLifecycle::Sealed)
            fail(Error::Code::NotSealed, "semantic-role planner result requested before seal");
        graph.validateSealed();
    }

    void checkReadable() const
    {
        if (lifecycle == PlannerLifecycle::Sealed)
            checkSealed();
        else
            checkUsable();
    }

    LogicalShapeID findShape(std::string_view shape) const noexcept
    {
        const auto found = shapes.find(shape);
        return found == shapes.end() ? invalid_logical_shape_id : found->second;
    }

    LogicalShapeID insertShape(std::string_view shape)
    {
        if (shape.empty())
            fail(Error::Code::InvalidGraph, "semantic-role logical shape is empty");
        if (shape.size() > limits.maximum_single_shape_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role logical shape exceeds its item byte limit");

        if (const auto found = findShape(shape); found != invalid_logical_shape_id)
            return found;
        if (shapes_by_id.size() >= limits.maximum_shapes)
            fail(Error::Code::LimitExceeded, "semantic-role logical shape count exceeds its limit");
        if (shape.size() > limits.maximum_shape_bytes - statistics.shape_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role logical shape bytes exceed their limit");

        ResourceDelta delta;
        delta.add(ResourceLimit::InternedLogicalShapesPerQuery, 1);
        const UInt64 scratch_bytes = sizeof(String) + sizeof(LogicalShapeID) + sizeof(const String *) + hash_node_overhead + shape.size();
        charge(std::move(delta), scratch_bytes);

        const auto id = static_cast<LogicalShapeID>(shapes_by_id.size());
        auto [inserted, was_inserted] = shapes.emplace(String(shape), id);
        if (!was_inserted)
            return inserted->second;
        try
        {
            shapes_by_id.push_back(std::addressof(inserted->first));
        }
        catch (...)
        {
            shapes.erase(inserted);
            throw;
        }

        statistics.shape_bytes += shape.size();
        statistics.shapes_interned = shapes_by_id.size();
        return id;
    }

    LogicalRoleID internRole(const LogicalRoleInput & input)
    {
        if (!input.isValid())
            fail(Error::Code::InvalidGraph, "semantic-role exact source has no checked descriptor or logical shape");

        const auto & descriptor = *input.descriptor;
        const auto arguments = std::string_view(descriptor.getCanonicalArgumentsEncoding());
        if (arguments.size() > limits.maximum_single_role_argument_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role canonical arguments exceed their item byte limit");

        auto shape = findShape(input.shape.canonical_encoding);
        const bool insert_shape = shape == invalid_logical_shape_id;
        if (insert_shape)
        {
            if (input.shape.canonical_encoding.empty())
                fail(Error::Code::InvalidGraph, "semantic-role logical shape is empty");
            if (input.shape.canonical_encoding.size() > limits.maximum_single_shape_bytes
                || input.shape.canonical_encoding.size() > limits.maximum_shape_bytes - statistics.shape_bytes)
                fail(Error::Code::LimitExceeded, "semantic-role logical shape bytes exceed their limit");
            if (shapes_by_id.size() >= limits.maximum_shapes)
                fail(Error::Code::LimitExceeded, "semantic-role logical shape count exceeds its limit");
            shape = static_cast<LogicalShapeID>(shapes_by_id.size());
        }

        const LogicalRoleView view{descriptor, shape};
        if (const auto found = roles.find(view); found != roles.end())
            return found->second;
        if (roles_by_id.size() >= limits.maximum_roles)
            fail(Error::Code::LimitExceeded, "semantic-role exact role count exceeds its limit");
        if (arguments.size() > limits.maximum_role_argument_bytes - statistics.role_argument_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role canonical argument bytes exceed their limit");

        chargeDistinctDescriptor(descriptor);

        if (insert_shape)
            shape = insertShape(input.shape.canonical_encoding);

        const auto id = static_cast<LogicalRoleID>(roles_by_id.size());
        ResourceDelta delta;
        delta.add(ResourceLimit::DenseRoleRecordsPerQuery, 1);
        const UInt64 scratch_bytes = sizeof(InternedLogicalRole) + sizeof(LogicalRoleID) + sizeof(const InternedLogicalRole *)
            + hash_node_overhead + arguments.size();
        charge(std::move(delta), scratch_bytes);
        InternedLogicalRole role{
            .definition_identity = descriptor.getDefinitionIdentity(),
            .definition_hash = descriptor.getDefinitionHash(),
            .canonical_arguments_encoding = descriptor.getCanonicalArgumentsEncoding(),
            .instantiation_semantic_hash = descriptor.getInstantiationSemanticHash(),
            .shape = shape,
        };
        auto [inserted, was_inserted] = roles.emplace(std::move(role), id);
        if (!was_inserted)
            return inserted->second;
        try
        {
            roles_by_id.push_back(std::addressof(inserted->first));
        }
        catch (...)
        {
            roles.erase(inserted);
            throw;
        }

        statistics.role_argument_bytes += arguments.size();
        statistics.roles_interned = roles_by_id.size();
        ProfileEvents::increment(ProfileEvents::UDTSemanticRolesInterned);
        return id;
    }

    LogicalRoleID reshapeRole(LogicalRoleID role_id, const LogicalShapeInput & shape_input)
    {
        if (role_id >= roles_by_id.size() || !shape_input.isValid())
            fail(Error::Code::InvalidGraph, "semantic-role reshape references an invalid role or shape");

        const auto & source = *roles_by_id[role_id];
        auto shape = findShape(shape_input.canonical_encoding);
        const bool insert_shape = shape == invalid_logical_shape_id;
        if (insert_shape)
        {
            if (shape_input.canonical_encoding.size() > limits.maximum_single_shape_bytes
                || shape_input.canonical_encoding.size() > limits.maximum_shape_bytes - statistics.shape_bytes)
                fail(Error::Code::LimitExceeded, "semantic-role reshape shape bytes exceed their limit");
            if (shapes_by_id.size() >= limits.maximum_shapes)
                fail(Error::Code::LimitExceeded, "semantic-role logical shape count exceeds its limit");
            shape = static_cast<LogicalShapeID>(shapes_by_id.size());
        }

        if (const auto found = roles.find(ReshapedLogicalRoleView{source, shape}); found != roles.end())
            return found->second;
        if (roles_by_id.size() >= limits.maximum_roles)
            fail(Error::Code::LimitExceeded, "semantic-role exact role count exceeds its limit");
        if (source.canonical_arguments_encoding.size() > limits.maximum_role_argument_bytes - statistics.role_argument_bytes)
            fail(Error::Code::LimitExceeded, "semantic-role canonical argument bytes exceed their limit");

        if (insert_shape)
            shape = insertShape(shape_input.canonical_encoding);

        const auto id = static_cast<LogicalRoleID>(roles_by_id.size());
        ResourceDelta delta;
        delta.add(ResourceLimit::DenseRoleRecordsPerQuery, 1);
        const UInt64 scratch_bytes = sizeof(InternedLogicalRole) + sizeof(LogicalRoleID) + sizeof(const InternedLogicalRole *)
            + hash_node_overhead + source.canonical_arguments_encoding.size();
        charge(std::move(delta), scratch_bytes);
        InternedLogicalRole prospective{
            .definition_identity = source.definition_identity,
            .definition_hash = source.definition_hash,
            .canonical_arguments_encoding = source.canonical_arguments_encoding,
            .instantiation_semantic_hash = source.instantiation_semantic_hash,
            .shape = shape,
        };
        auto [inserted, was_inserted] = roles.emplace(std::move(prospective), id);
        if (!was_inserted)
            return inserted->second;
        try
        {
            roles_by_id.push_back(std::addressof(inserted->first));
        }
        catch (...)
        {
            roles.erase(inserted);
            throw;
        }

        statistics.role_argument_bytes += source.canonical_arguments_encoding.size();
        statistics.roles_interned = roles_by_id.size();
        ProfileEvents::increment(ProfileEvents::UDTSemanticRolesInterned);
        return id;
    }

    bool haveSameInstantiation(LogicalRoleID lhs_id, LogicalRoleID rhs_id) const
    {
        if (lhs_id >= roles_by_id.size() || rhs_id >= roles_by_id.size())
            fail(Error::Code::InvalidGraph, "semantic-role proof references an invalid role");
        const auto & lhs = *roles_by_id[lhs_id];
        const auto & rhs = *roles_by_id[rhs_id];
        return lhs.definition_identity == rhs.definition_identity && lhs.definition_hash == rhs.definition_hash
            && lhs.canonical_arguments_encoding == rhs.canonical_arguments_encoding
            && lhs.instantiation_semantic_hash == rhs.instantiation_semantic_hash;
    }

    QueryDefinitionHandleID retainDefinitionHandle(const InstantiatedTypeDescriptor::Ptr & descriptor)
    {
        if (!descriptor)
            return invalid_query_definition_handle_id;

        const auto & persisted = descriptor->getPersistedDescriptor();
        const auto & hash = persisted.getInstantiationSemanticHash();
        if (const auto found = definition_handles_by_hash.find(hash); found != definition_handles_by_hash.end())
        {
            for (const auto handle : found->second)
            {
                if (owned_definition_handles[handle]->getPersistedDescriptor().hasSameInstantiation(persisted))
                    return handle;
            }
        }

        if (owned_definition_handles.size() >= limits.maximum_owned_definition_handles)
            fail(Error::Code::LimitExceeded, "semantic-role owned definition handles exceed their limit");

        const UInt64 scratch_bytes = sizeof(InstantiatedTypeDescriptor::Ptr) + sizeof(Digest) + sizeof(std::vector<QueryDefinitionHandleID>)
            + sizeof(QueryDefinitionHandleID) + hash_node_overhead;
        charge({}, scratch_bytes);

        const auto id = static_cast<QueryDefinitionHandleID>(owned_definition_handles.size());
        auto [handles_for_hash, inserted_hash] = definition_handles_by_hash.try_emplace(hash);
        try
        {
            owned_definition_handles.push_back(descriptor);
            try
            {
                handles_for_hash->second.push_back(id);
            }
            catch (...)
            {
                owned_definition_handles.pop_back();
                throw;
            }
        }
        catch (...)
        {
            if (inserted_hash && handles_for_hash->second.empty())
                definition_handles_by_hash.erase(handles_for_hash);
            throw;
        }
        statistics.owned_definition_handles = owned_definition_handles.size();
        ProfileEvents::increment(ProfileEvents::UDTQueryDefinitionHandles);
        return id;
    }

    SemanticConflictID addConflict(SemanticRoleConflictKind kind, const SemanticNodePath & state, LogicalRoleID lhs, LogicalRoleID rhs)
    {
        if (conflicts.size() >= limits.maximum_conflicts)
            fail(Error::Code::LimitExceeded, "semantic-role conflict diagnostics exceed their limit");
        ResourceDelta delta;
        delta.add(ResourceLimit::ConflictSamplesPerQuery, 1);
        charge(std::move(delta), sizeof(SemanticRoleConflict));
        const auto id = static_cast<SemanticConflictID>(conflicts.size());
        conflicts.push_back({kind, state, lhs, rhs});
        statistics.conflicts = conflicts.size();
        ProfileEvents::increment(ProfileEvents::UDTSemanticRoleConflicts);
        return id;
    }

    void validateNodeDescription(const SemanticRoleNode & node) const
    {
        switch (node.source.kind)
        {
            case SemanticRoleSourceKind::None:
            case SemanticRoleSourceKind::NullOnly:
            case SemanticRoleSourceKind::Exact:
            case SemanticRoleSourceKind::PreboundExact: break;
        }
        if (static_cast<UInt8>(node.source.kind) > static_cast<UInt8>(SemanticRoleSourceKind::PreboundExact))
            fail(Error::Code::InvalidGraph, "semantic-role graph returned an unknown source kind");

        const bool has_exact_source_payload = node.source.exact_role.descriptor || !node.source.exact_role.shape.canonical_encoding.empty();
        const bool expects_exact_source_payload
            = node.source.kind == SemanticRoleSourceKind::Exact || node.source.kind == SemanticRoleSourceKind::PreboundExact;
        if (expects_exact_source_payload != has_exact_source_payload)
            fail(Error::Code::InvalidGraph, "semantic-role graph source payload disagrees with its source kind");

        if (node.source.kind != SemanticRoleSourceKind::None)
        {
            if (node.input_count != 0 || node.transfer != SemanticTransferKind::Unregistered || node.result_shape || node.exact_target)
                fail(Error::Code::InvalidGraph, "semantic-role source state also declares transfer inputs or metadata");
            if ((node.source.kind == SemanticRoleSourceKind::Exact || node.source.kind == SemanticRoleSourceKind::PreboundExact)
                && !node.source.exact_role.isValid())
                fail(Error::Code::InvalidGraph, "semantic-role exact source has no checked descriptor or logical shape");
            return;
        }

        const auto * transfer = SemanticTransferRegistry::find(node.transfer);
        if (!transfer)
        {
            if (node.transfer != SemanticTransferKind::Unregistered)
                fail(Error::Code::InvalidGraph, "semantic-role graph returned an unknown transfer kind");
            if (node.input_count != 0 || node.result_shape || node.exact_target)
                fail(Error::Code::InvalidGraph, "unregistered semantic-role barrier carries transfer inputs or metadata");
            return;
        }
        if (node.input_count < transfer->minimum_inputs || node.input_count > transfer->maximum_inputs)
            fail(Error::Code::InvalidGraph, "semantic-role transfer input count is outside its closed descriptor");
        if (transfer->requires_result_shape && !node.result_shape)
            fail(Error::Code::InvalidGraph, "semantic-role transfer result shape disagrees with its closed descriptor");
        if (node.result_shape && !transfer->allows_result_shape)
            fail(Error::Code::InvalidGraph, "semantic-role transfer result shape disagrees with its closed descriptor");
        if (node.result_shape && !node.result_shape->isValid())
            fail(Error::Code::InvalidGraph, "semantic-role transfer result shape is empty");
        if (transfer->requires_exact_target != node.exact_target.has_value())
            fail(Error::Code::InvalidGraph, "semantic-role transfer exact target disagrees with its closed descriptor");
        if (node.exact_target && !node.exact_target->isValid())
            fail(Error::Code::InvalidGraph, "semantic-role transfer exact target is invalid");
    }

    ProofFrame beginState(const SemanticNodePath & state)
    {
        if (!state.isValid())
            fail(Error::Code::InvalidGraph, "semantic-role graph returned an invalid node/path state");
        if (memo.size() >= limits.maximum_demanded_states)
            fail(Error::Code::LimitExceeded, "semantic-role demanded states exceed their limit");

        const bool new_path = !logical_paths.contains(state.path);
        if (new_path && logical_paths.size() >= limits.maximum_logical_paths)
            fail(Error::Code::LimitExceeded, "semantic-role logical paths exceed their limit");

        checkGeneration();
        auto node = graph.describe(state);
        checkGeneration();
        validateNodeDescription(node);

        ResourceDelta delta;
        delta.add(ResourceLimit::NodePathStatesPerQuery, 1);
        UInt64 scratch_bytes = sizeof(SemanticNodePath) + sizeof(MemoEntry) + hash_node_overhead;
        if (new_path)
            scratch_bytes += sizeof(LogicalPathID) + hash_node_overhead;
        charge(std::move(delta), scratch_bytes);

        auto [memo_entry, inserted] = memo.emplace(state, MemoEntry{});
        if (!inserted)
            fail(Error::Code::InvalidGraph, "semantic-role attempted to begin an already memoized state");
        try
        {
            if (new_path)
                logical_paths.insert(state.path);
        }
        catch (...)
        {
            memo.erase(memo_entry);
            throw;
        }

        statistics.demanded_states = memo.size();
        statistics.logical_paths = logical_paths.size();
        ProfileEvents::increment(ProfileEvents::UDTSemanticAnalysisNodesVisited);
        const auto * transfer = SemanticTransferRegistry::find(node.transfer);
        return {
            .state = state,
            .node = std::move(node),
            .transfer = transfer,
            .next_input = 0,
            .pending_child = {},
            .awaiting_child = false,
            .has_aggregate = false,
            .aggregate = RoleProof::noRole(),
        };
    }

    void accumulate(ProofFrame & frame, const RoleProof & child)
    {
        if (!frame.transfer)
            fail(Error::Code::InvalidGraph, "unregistered semantic-role transfer attempted to inspect an input");

        if (!frame.has_aggregate)
        {
            frame.aggregate = child;
            frame.has_aggregate = true;
            return;
        }

        if (frame.transfer->policy != SemanticTransferPolicy::MeetUnanimous)
            fail(Error::Code::InvalidGraph, "unary semantic-role transfer produced multiple input proofs");

        if (frame.aggregate.getKind() == RoleProof::Kind::NoRole || child.getKind() == RoleProof::Kind::NoRole)
        {
            frame.aggregate = RoleProof::noRole();
            return;
        }
        if (frame.aggregate.isConflict())
            return;
        if (child.isConflict())
        {
            frame.aggregate = child;
            return;
        }
        if (frame.aggregate.getKind() == RoleProof::Kind::NullOnly)
        {
            frame.aggregate = child;
            return;
        }
        if (child.getKind() == RoleProof::Kind::NullOnly)
            return;

        const auto lhs = frame.aggregate.getExactRole();
        const auto rhs = child.getExactRole();
        if (lhs != rhs)
            frame.aggregate = RoleProof::conflict(addConflict(SemanticRoleConflictKind::DistinctExactRoles, frame.state, lhs, rhs));
    }

    RoleProof finishFrame(ProofFrame & frame)
    {
        if (frame.node.source.kind == SemanticRoleSourceKind::NullOnly)
            return RoleProof::nullOnly();
        if (frame.node.source.kind == SemanticRoleSourceKind::Exact || frame.node.source.kind == SemanticRoleSourceKind::PreboundExact)
        {
            const auto role = internRole(frame.node.source.exact_role);
            if (frame.node.source.kind == SemanticRoleSourceKind::PreboundExact)
            {
                ++statistics.prebound_schema_role_uses;
                ProfileEvents::increment(ProfileEvents::UDTSchemaBoundRoleUses);
            }
            return RoleProof::exact(role);
        }
        if (!frame.transfer)
            return RoleProof::noRole();
        if (!frame.has_aggregate)
            fail(Error::Code::InvalidGraph, "registered semantic-role transfer has no evaluated input");

        RoleProof result = frame.aggregate;
        switch (frame.transfer->policy)
        {
            case SemanticTransferPolicy::PreserveUnary: break;
            case SemanticTransferPolicy::ReshapeUnary:
                if (result.isExact())
                    result = RoleProof::exact(reshapeRole(result.getExactRole(), *frame.node.result_shape));
                break;
            case SemanticTransferPolicy::MeetUnanimous:
                /// A NULL-only arm does not necessarily add a wrapper: the
                /// unanimous exact arm can already have the same Nullable
                /// result shape. The closed graph adapter supplies a shape
                /// exactly when the physical result differs from an input.
                if (result.isExact() && frame.node.result_shape)
                    result = RoleProof::exact(reshapeRole(result.getExactRole(), *frame.node.result_shape));
                break;
            case SemanticTransferPolicy::PreserveExactInstantiation:
                if (result.isExact())
                {
                    const auto target = internRole(*frame.node.exact_target);
                    result = haveSameInstantiation(result.getExactRole(), target) ? RoleProof::exact(target) : RoleProof::noRole();
                }
                break;
        }
        return result;
    }

    const UInt64 generation;
    const QueryTreeSemanticRoleGraph & graph;
    ProspectiveResourceBudget & query_resource_budget;
    const SemanticRolePlannerLimits limits;
    SemanticRolePlannerStatistics statistics;
    bool poisoned = false;
    PlannerLifecycle lifecycle = PlannerLifecycle::Open;

    std::unordered_map<String, LogicalShapeID, StringHashForHeterogeneousLookup, std::equal_to<>> shapes;
    std::vector<const String *> shapes_by_id;
    std::unordered_map<InternedLogicalRole, LogicalRoleID, LogicalRoleHash, LogicalRoleEqual> roles;
    std::vector<const InternedLogicalRole *> roles_by_id;

    std::unordered_map<Digest, std::vector<QueryDefinitionHandleID>, DigestHash> definition_handles_by_hash;
    std::vector<InstantiatedTypeDescriptor::Ptr> owned_definition_handles;
    std::vector<StoredSink> sinks;
    std::vector<PlannedBoundary> planned_boundaries;
    std::optional<SemanticCacheDependencyDigest> cache_dependency_digest;
    std::vector<SemanticRoleConflict> conflicts;

    std::unordered_map<SemanticNodePath, MemoEntry, SemanticNodePathHash> memo;
    std::unordered_set<LogicalPathID> logical_paths;
};

SemanticRolePlannerError::SemanticRolePlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

SemanticRolePlanner::SemanticRolePlanner(
    const QueryTreeSemanticRoleGraph & graph,
    ProspectiveResourceBudget & query_resource_budget,
    const SemanticRolePlannerLimits & limits,
    UInt64 admitted_base_scratch_bytes)
    : impl(std::make_unique<Impl>(graph, query_resource_budget, limits, admitted_base_scratch_bytes))
{
    ProfileEvents::increment(ProfileEvents::UDTSemanticAnalysisActivations);
}

SemanticRolePlanner::~SemanticRolePlanner() = default;

SemanticRolePlanner::Ptr SemanticRolePlanner::create(
    const QueryTreeSemanticRoleGraph & graph, ProspectiveResourceBudget & query_resource_budget, const SemanticRolePlannerLimits & limits)
{
    validateLimits(limits);
    if (!graph.isSealed())
        fail(Error::Code::InvalidGraph, "semantic-role planner requires a sealed QueryTree graph");
    graph.validateSealed();
    if (graph.getSinkCount() > limits.maximum_sinks)
        fail(Error::Code::LimitExceeded, "sealed QueryTree semantic-role sinks exceed the planner limit");
    constexpr UInt64 base_scratch_bytes = sizeof(SemanticRolePlanner) + sizeof(Impl);
    if (base_scratch_bytes > limits.maximum_combined_scratch_bytes)
        fail(Error::Code::LimitExceeded, "semantic-role planner base state exceeds its local scratch limit");
    requireAdmission(query_resource_budget, {}, base_scratch_bytes);
    auto planner = Ptr(new SemanticRolePlanner(graph, query_resource_budget, limits, base_scratch_bytes));
    for (UInt64 sink = 0; sink < graph.getSinkCount(); ++sink)
    {
        const auto imported = planner->registerSink(graph.getSink(static_cast<SemanticSinkID>(sink)));
        if (!imported || *imported != sink)
            fail(Error::Code::InvalidGraph, "sealed QueryTree semantic-role sink enumeration changed during planner import");
    }
    return planner;
}

std::optional<SemanticSinkID> SemanticRolePlanner::registerSink(const SemanticSink & sink)
{
    impl->checkOpen();
    if (!SemanticSinkRegistry::isEligible(sink))
        return std::nullopt;
    if (impl->sinks.size() >= impl->limits.maximum_sinks)
        fail(Error::Code::LimitExceeded, "semantic-role eligible sinks exceed their limit");

    impl->chargeAndEnsureSinkCapacity({}, impl->sinks.size() + 1);

    StoredSink stored{
        .source = sink.source,
        .observes_identity = sink.observes_identity,
    };
    if (sink.expected_role)
    {
        stored.expected_role = impl->internRole(sink.expected_role->role);
        stored.retained_definition_handle = impl->retainDefinitionHandle(sink.expected_role->retained_descriptor);
        if (!sink.expected_role->retained_descriptor)
        {
            ++impl->statistics.prebound_schema_role_uses;
            ProfileEvents::increment(ProfileEvents::UDTSchemaBoundRoleUses);
        }
    }

    const auto id = static_cast<SemanticSinkID>(impl->sinks.size());
    impl->sinks.push_back(stored);
    impl->statistics.eligible_sinks = impl->sinks.size();
    return id;
}

void SemanticRolePlanner::chargeLiteralBytes(UInt64 bytes)
{
    impl->checkOpen();
    if (bytes > impl->limits.maximum_literal_bytes - impl->statistics.literal_bytes)
        fail(Error::Code::LimitExceeded, "semantic-role expected-destination literal bytes exceed their limit");
    ResourceDelta delta;
    delta.add(ResourceLimit::ContextualLiteralBytesPerQuery, bytes);
    impl->charge(std::move(delta), bytes);
    impl->statistics.literal_bytes += bytes;
}

RoleProof SemanticRolePlanner::prove(SemanticNodeID source, LogicalPathID path)
{
    impl->checkOpen();
    const SemanticNodePath root{source, path};
    if (!root.isValid())
        fail(Error::Code::InvalidGraph, "semantic-role proof requested an invalid node/path state");

    if (const auto found = impl->memo.find(root); found != impl->memo.end())
    {
        if (found->second.state == MemoState::Done)
            return found->second.proof;
        impl->poisoned = true;
        fail(Error::Code::InvalidGraph, "semantic-role proof reentered an active state");
    }

    struct PoisonOnFailure
    {
        bool & poisoned;
        bool completed = false;

        ~PoisonOnFailure()
        {
            if (!completed)
                poisoned = true;
        }
    } proof_attempt{impl->poisoned};

    std::vector<ProofFrame> stack;
    impl->ensureProofStackCapacity(stack, 1);
    stack.push_back(impl->beginState(root));
    while (!stack.empty())
    {
        impl->checkGeneration();
        auto & frame = stack.back();

        if (frame.awaiting_child)
        {
            const auto child = impl->memo.find(frame.pending_child);
            if (child == impl->memo.end() || child->second.state != MemoState::Done)
                fail(Error::Code::InvalidGraph, "semantic-role child did not complete before its parent resumed");
            impl->accumulate(frame, child->second.proof);
            frame.awaiting_child = false;
            continue;
        }

        /// NoRole is the absorbing element of a unanimous meet: an opaque
        /// branch erases provenance even if earlier exact branches conflict.
        /// Conflict cannot short-circuit because a later NoRole must still be
        /// allowed to erase it, independently of branch order.
        const bool short_circuit_meet = frame.transfer && frame.transfer->policy == SemanticTransferPolicy::MeetUnanimous
            && frame.has_aggregate && frame.aggregate.getKind() == RoleProof::Kind::NoRole;
        if (frame.transfer && frame.next_input < frame.node.input_count && !short_circuit_meet)
        {
            if (impl->statistics.inspected_edges >= impl->limits.maximum_inspected_edges)
                fail(Error::Code::LimitExceeded, "semantic-role inspected edges exceed their limit");

            ResourceDelta delta;
            delta.add(ResourceLimit::InspectedEdgesPerQuery, 1);
            impl->charge(std::move(delta), 0);

            const auto input_index = frame.next_input++;
            impl->checkGeneration();
            const auto child_state = impl->graph.getInput(frame.state, input_index);
            impl->checkGeneration();
            if (!child_state.isValid())
                fail(Error::Code::InvalidGraph, "semantic-role graph returned an invalid input state");
            ++impl->statistics.inspected_edges;
            ProfileEvents::increment(ProfileEvents::UDTSemanticAnalysisEdgesVisited);

            const auto child = impl->memo.find(child_state);
            if (child == impl->memo.end())
            {
                if (stack.size() >= impl->limits.maximum_active_depth)
                    fail(Error::Code::LimitExceeded, "semantic-role active proof depth exceeds its limit");
                impl->ensureProofStackCapacity(stack, stack.size() + 1);
                auto child_frame = impl->beginState(child_state);
                stack.push_back(std::move(child_frame));
                auto & parent = stack[stack.size() - 2];
                parent.pending_child = child_state;
                parent.awaiting_child = true;
                continue;
            }
            if (child->second.state == MemoState::Visiting)
            {
                const auto conflict
                    = impl->addConflict(SemanticRoleConflictKind::Cycle, frame.state, invalid_logical_role_id, invalid_logical_role_id);
                impl->accumulate(frame, RoleProof::conflict(conflict));
                continue;
            }
            impl->accumulate(frame, child->second.proof);
            continue;
        }

        const auto result = impl->finishFrame(frame);
        const auto memo_entry = impl->memo.find(frame.state);
        if (memo_entry == impl->memo.end() || memo_entry->second.state != MemoState::Visiting)
            fail(Error::Code::InvalidGraph, "semantic-role memo state disappeared before completion");
        memo_entry->second.state = MemoState::Done;
        memo_entry->second.proof = result;
        stack.pop_back();
    }

    const auto proof = impl->memo.at(root).proof;
    proof_attempt.completed = true;
    return proof;
}

PlannedBoundary SemanticRolePlanner::satisfy(SemanticSinkID sink, const RoleProof & proof) const
{
    impl->checkOpen();
    if (sink >= impl->sinks.size())
        fail(Error::Code::InvalidSink, "semantic-role boundary references an unknown sink");
    if (proof.isExact() && proof.getExactRole() >= impl->roles_by_id.size())
        fail(Error::Code::InvalidGraph, "semantic-role boundary references an unknown exact role");
    if (proof.isConflict() && proof.getConflict() >= impl->conflicts.size())
        fail(Error::Code::InvalidGraph, "semantic-role boundary references an unknown conflict");

    const auto & stored = impl->sinks[sink];
    PlannedBoundary result{
        .sink = sink,
        .source_proof = proof,
        .expected_role = stored.expected_role,
        .retained_definition_handle = stored.retained_definition_handle,
    };
    if (proof.isConflict())
        result.kind = PlannedBoundaryKind::Conflict;
    else if (stored.expected_role != invalid_logical_role_id)
        result.kind = proof.isExact() && proof.getExactRole() == stored.expected_role ? PlannedBoundaryKind::PreserveSourceRole
                                                                                      : PlannedBoundaryKind::ApplyExpectedRole;
    else if (stored.observes_identity && proof.isExact())
        result.kind = PlannedBoundaryKind::ObserveSourceRole;
    else if (proof.isExact())
        result.kind = PlannedBoundaryKind::PreserveSourceRole;
    else
        result.kind = PlannedBoundaryKind::PhysicalOnly;
    return result;
}

PlannedBoundary SemanticRolePlanner::planSink(SemanticSinkID sink)
{
    impl->checkOpen();
    if (sink >= impl->sinks.size())
        fail(Error::Code::InvalidSink, "semantic-role planning references an unknown sink");
    return satisfy(sink, prove(impl->sinks[sink].source));
}

void SemanticRolePlanner::seal(const SemanticCacheDependencyDigestLimits & digest_limits)
{
    impl->checkOpen();

    struct PoisonOnFailure
    {
        bool & poisoned;
        bool completed = false;

        ~PoisonOnFailure()
        {
            if (!completed)
                poisoned = true;
        }
    } seal_attempt{impl->poisoned};

    const UInt64 planned_boundary_bytes = static_cast<UInt64>(impl->sinks.size()) * sizeof(PlannedBoundary);
    impl->charge({}, planned_boundary_bytes);
    impl->planned_boundaries.reserve(impl->sinks.size());
    for (SemanticSinkID sink = 0; sink < impl->sinks.size(); ++sink)
    {
        auto boundary = planSink(sink);
        if (boundary.kind == PlannedBoundaryKind::PhysicalOnly)
            fail(Error::Code::InvalidGraph, "eligible semantic-role sink resolved to an uncacheable physical-only boundary");
        impl->planned_boundaries.push_back(std::move(boundary));
    }
    if (impl->planned_boundaries.size() != impl->sinks.size())
        fail(Error::Code::InvalidGraph, "semantic-role planner did not produce one boundary for every sealed sink");

    if (!impl->sinks.empty() && impl->roles_by_id.empty())
        fail(Error::Code::InvalidGraph, "eligible semantic-role sinks produced no cacheable expected or proven role");

    const UInt64 digest_scratch_bytes = static_cast<UInt64>(impl->roles_by_id.size()) * 2 * sizeof(const void *);
    impl->charge({}, digest_scratch_bytes);
    auto digest = SemanticCacheDependencyDigest::fromSealedPlanner(impl->roles_by_id, impl->shapes_by_id, digest_limits);
    impl->cache_dependency_digest.emplace(std::move(digest));
    impl->graph.validateSealed();
    impl->lifecycle = PlannerLifecycle::Sealed;
    seal_attempt.completed = true;
}

bool SemanticRolePlanner::isSealed() const noexcept
{
    return impl->lifecycle == PlannerLifecycle::Sealed;
}

const PlannedBoundary & SemanticRolePlanner::getPlannedBoundary(SemanticSinkID sink) const
{
    impl->checkSealed();
    if (sink >= impl->planned_boundaries.size())
        fail(Error::Code::InvalidSink, "semantic-role sealed boundary references an unknown sink");
    return impl->planned_boundaries[sink];
}

std::span<const PlannedBoundary> SemanticRolePlanner::getPlannedBoundaries() const
{
    impl->checkSealed();
    if (impl->planned_boundaries.size() != impl->sinks.size())
        fail(Error::Code::InvalidGraph, "sealed semantic-role planner lost part of its boundary enumeration");
    return std::span<const PlannedBoundary>(impl->planned_boundaries);
}

const SemanticCacheDependencyDigest & SemanticRolePlanner::getCacheDependencyDigest() const
{
    impl->checkSealed();
    if (!impl->cache_dependency_digest)
        fail(Error::Code::InvalidGraph, "sealed semantic-role planner lost its cache dependency digest");
    return *impl->cache_dependency_digest;
}

UInt64 SemanticRolePlanner::getGeneration() const noexcept
{
    return impl->generation;
}

const SemanticRolePlannerStatistics & SemanticRolePlanner::getStatistics() const noexcept
{
    return impl->statistics;
}

const InternedLogicalRole & SemanticRolePlanner::getRole(LogicalRoleID role) const
{
    impl->checkReadable();
    if (role >= impl->roles_by_id.size())
        fail(Error::Code::InvalidGraph, "semantic-role ID is outside the role interner");
    return *impl->roles_by_id[role];
}

std::string_view SemanticRolePlanner::getShape(LogicalShapeID shape) const
{
    impl->checkReadable();
    if (shape >= impl->shapes_by_id.size())
        fail(Error::Code::InvalidGraph, "semantic-role shape ID is outside the shape interner");
    return *impl->shapes_by_id[shape];
}

const SemanticRoleConflict & SemanticRolePlanner::getConflict(SemanticConflictID conflict) const
{
    impl->checkReadable();
    if (conflict >= impl->conflicts.size())
        fail(Error::Code::InvalidGraph, "semantic-role conflict ID is outside the bounded diagnostic table");
    return impl->conflicts[conflict];
}

const InstantiatedTypeDescriptor::Ptr & SemanticRolePlanner::getOwnedDefinitionHandle(QueryDefinitionHandleID handle) const
{
    impl->checkReadable();
    if (handle >= impl->owned_definition_handles.size())
        fail(Error::Code::InvalidGraph, "semantic-role definition handle ID is outside the owned handle table");
    return impl->owned_definition_handles[handle];
}

}
