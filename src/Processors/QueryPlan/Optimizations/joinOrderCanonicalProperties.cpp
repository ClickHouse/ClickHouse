#include <Processors/QueryPlan/Optimizations/joinOrderCanonicalProperties.h>

#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <base/scope_guard.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <limits>
#include <numeric>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

size_t hashCombine(size_t seed, size_t value)
{
    return seed ^ (value + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

template <typename T>
size_t hashSet(std::span<const T> values)
{
    size_t hash = 0;
    for (const auto & value : values)
    {
        if constexpr (requires { value.value; })
            hash = hashCombine(hash, std::hash<UInt32>{}(value.value));
        else
            hash = hashCombine(hash, std::hash<T>{}(value));
    }
    return hash;
}

template <typename T, bool normalize>
class FlatArenaStore
{
public:
    FlatArenaStore() { ranges.emplace_back(); }

    UInt32 intern(std::vector<T> & values)
    {
        if constexpr (normalize)
        {
            std::ranges::sort(values);
            values.erase(std::unique(values.begin(), values.end()), values.end());
        }

        const size_t hash = hashSet<T>(values);
        auto [begin, end] = ids.equal_range(hash);
        for (auto it = begin; it != end; ++it)
        {
            const UInt32 id = it->second;
            if (std::ranges::equal(get(id), values))
                return id;
        }

        const UInt32 id = checkedJoinOrderUInt32(ranges.size(), "interned sequences");
        const UInt32 offset = checkedJoinOrderUInt32(arena.size(), "interned sequence members");
        const UInt32 size = checkedJoinOrderUInt32(values.size(), "interned sequence members");
        arena.insert(arena.end(), values.begin(), values.end());
        ranges.push_back({offset, size});
        ids.emplace(hash, id);
        return id;
    }

    std::span<const T> get(UInt32 id) const
    {
        if (id == 0 || id >= ranges.size())
            return {};
        const auto [offset, size] = ranges[id];
        return std::span<const T>(arena).subspan(offset, size);
    }

    bool contains(UInt32 id) const { return id != 0 && id < ranges.size(); }
    size_t size() const { return ranges.size() - 1; }
    size_t memberCount() const { return arena.size(); }
    size_t retainedBytes() const { return arena.size() * sizeof(T); }

private:
    struct Range
    {
        UInt32 offset = 0;
        UInt32 size = 0;
    };

    std::vector<T> arena;
    std::vector<Range> ranges;
    std::unordered_multimap<size_t, UInt32> ids;
};

using FlatUInt32SetStore = FlatArenaStore<UInt32, true>;
using FlatColumnSetStore = FlatArenaStore<JoinOrderColumnId, true>;
using FlatUInt64SequenceStore = FlatArenaStore<UInt64, false>;

std::atomic<UInt64> next_provider_id{1};

}

struct JoinOrderCanonicalProperties::Impl
{
    struct EqualityClass
    {
        std::vector<JoinOrderColumnId> members;
        UInt32 closure_atom_id = 0;
        /// Some member pair cannot be physically equated; links of this class may be used
        /// only when justified by intra-group predicates (no synthesis required).
        bool incomparable = false;
        /// One bit per member relation below 32, so `getEqualityCut` can skip classes that
        /// do not touch both groups without walking members (native-subset regions only).
        UInt32 native_relation_mask = 0;
    };

    struct Group
    {
        UInt32 native_subset = 0;
        UInt32 generic_subset = 0;
        JoinOrderPredicateClosureId closure;
        JoinOrderOutputContractId output_contract;
        /// Lazily computed by `baseEqualityClasses`: classes whose in-group members are
        /// mutually connected by predicates applicable inside the group (one entry per class
        /// index). The links of such classes are enforced by the group's own subtree, so
        /// proofs may use them without obligations.
        std::optional<std::vector<bool>> base_equality_classes;
    };

    struct Cut
    {
        JoinOrderLogicalGroupId left;
        JoinOrderLogicalGroupId right;
        JoinOrderColumnSetId left_columns;
        JoinOrderColumnSetId right_columns;
        std::optional<JoinOrderPropertyProofId> cardinality_proof;
    };

    struct UsableKey
    {
        UInt32 relation = 0;
        std::vector<JoinOrderColumnId> columns;
        JoinOrderUniqueKeyId catalog_id;
    };

    struct CacheKey
    {
        UInt32 group = 0;
        UInt32 demand = 0;
        bool operator==(const CacheKey &) const = default;
    };

    struct CacheKeyHash
    {
        size_t operator()(const CacheKey & key) const
        {
            return hashCombine(std::hash<UInt32>{}(key.group), std::hash<UInt32>{}(key.demand));
        }
    };

    std::shared_ptr<const JoinOrderDataPropertyCatalog> catalog;
    const size_t relation_count;
    const UInt64 provider_id;
    std::vector<JoinOrderCanonicalPredicate> predicates;
    std::optional<JoinOrderPropertyUnsupportedReason> region_rejection;

    JoinOrderSemanticRegionId region;

    std::vector<EqualityClass> equality_classes;
    std::vector<std::optional<UInt32>> column_to_equality_class;
    std::vector<UsableKey> usable_keys;
    std::vector<std::vector<UInt32>> column_to_keys;
    std::vector<bool> relation_has_nullable_trusted_key;
    std::vector<bool> relation_has_usable_key;
    size_t equality_member_count = 0;

    mutable FlatUInt32SetStore relation_subsets;
    mutable FlatUInt32SetStore predicate_closures;
    mutable FlatUInt64SequenceStore output_contracts;
    mutable FlatColumnSetStore column_sets;
    mutable std::vector<Group> groups{1};
    mutable std::unordered_map<UInt32, UInt32> native_group_ids;
    mutable std::unordered_map<UInt32, UInt32> generic_group_ids;
    mutable std::vector<Cut> cuts{1};
    /// A zero value is the cached `NoEqualityCut` sentinel.
    mutable std::unordered_map<UInt64, UInt32> cut_ids;
    mutable std::unordered_map<CacheKey, JoinOrderUniquenessResult, CacheKeyHash> uniqueness_cache;

    mutable std::vector<UInt32> generic_subset_scratch;

    mutable bool cut_scratch_initialized = false;
    mutable std::vector<JoinOrderColumnId> cut_left_columns;
    mutable std::vector<JoinOrderColumnId> cut_right_columns;

    mutable bool uniqueness_scratch_initialized = false;
    mutable UInt32 uniqueness_generation = 0;
    mutable std::vector<UInt32> reached_generation;
    mutable std::vector<UInt32> equality_fired_generation;
    mutable std::vector<UInt32> key_remaining_generation;
    mutable std::vector<UInt32> key_remaining;
    mutable std::vector<UInt32> relation_determined_generation;
    mutable std::vector<JoinOrderColumnId> uniqueness_queue;

    mutable UInt32 next_proof_handle = 1;
    mutable JoinOrderCanonicalMetrics metrics;

    Impl(
        std::shared_ptr<const JoinOrderDataPropertyCatalog> catalog_,
        size_t relation_count_,
        std::vector<JoinOrderCanonicalPredicate> predicates_,
        std::optional<JoinOrderPropertyUnsupportedReason> region_rejection_)
        : catalog(std::move(catalog_))
        , relation_count(relation_count_)
        , provider_id(next_provider_id.fetch_add(1, std::memory_order_relaxed))
        , predicates(std::move(predicates_))
        , region_rejection(region_rejection_)
        , region{1, provider_id}
    {
        /// A pre-rejected region answers every request with the caller's reason; keep that
        /// reason and skip class/key construction that nothing could ever consume.
        if (region_rejection)
            return;

        if (!catalog || catalog->relationCount() != relation_count)
        {
            region_rejection = JoinOrderPropertyUnsupportedReason::MissingCatalog;
            return;
        }

        /// A source-qualified contract does not use display names as identity, but
        /// the current candidate-header boundary binds columns by name. Reject a
        /// duplicate anywhere in the region rather than allowing two source-qualified
        /// columns to collapse to one candidate-header name.
        std::unordered_set<String> output_names;
        for (UInt32 relation = 0; relation < relation_count; ++relation)
        {
            const auto relation_columns = catalog->columnsForRelation(relation);
            for (const auto column_id : relation_columns)
            {
                const auto & column = catalog->column(column_id);
                if (column.relation != relation || column.leaf_output_position >= relation_columns.size()
                    || !output_names.insert(catalog->name(column.display_name)).second || catalog->typeName(column_id).empty())
                {
                    region_rejection = JoinOrderPropertyUnsupportedReason::AmbiguousOutputContract;
                    return;
                }
            }
        }

        const size_t column_count = catalog->columnCount();
        std::vector<UInt32> parent(column_count);
        std::iota(parent.begin(), parent.end(), 0);
        auto find = [&](UInt32 value)
        {
            UInt32 root = value;
            while (parent[root] != root)
                root = parent[root];
            while (parent[value] != value)
            {
                const UInt32 next = parent[value];
                parent[value] = root;
                value = next;
            }
            return root;
        };
        auto unite = [&](UInt32 lhs, UInt32 rhs)
        {
            lhs = find(lhs);
            rhs = find(rhs);
            if (lhs != rhs)
                parent[rhs] = lhs;
        };

        std::ranges::sort(predicates, {}, &JoinOrderCanonicalPredicate::stable_id);
        for (size_t index = 0; index < predicates.size(); ++index)
        {
            const auto & predicate = predicates[index];
            if (index && predicates[index - 1].stable_id == predicate.stable_id)
            {
                region_rejection = JoinOrderPropertyUnsupportedReason::AmbiguousEqualityBinding;
                return;
            }
            for (const auto relation : predicate.applicability)
            {
                if (relation >= relation_count)
                {
                    region_rejection = JoinOrderPropertyUnsupportedReason::InvalidSubset;
                    return;
                }
            }
            if (!predicate.deterministic)
            {
                region_rejection = JoinOrderPropertyUnsupportedReason::NonDeterministicPredicate;
                return;
            }
            if (const auto * unsupported_binding = getUnsupportedReason(predicate.binding))
            {
                region_rejection = *unsupported_binding;
                return;
            }
            const auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&predicate.binding);
            if (!equality)
                continue;
            if (equality->lhs.value >= column_count || equality->rhs.value >= column_count)
            {
                region_rejection = JoinOrderPropertyUnsupportedReason::AmbiguousEqualityBinding;
                return;
            }
            if (!catalog->isIntrinsicNonNull(equality->lhs) || !catalog->isIntrinsicNonNull(equality->rhs))
            {
                region_rejection = JoinOrderPropertyUnsupportedReason::NullableEqualityColumn;
                return;
            }
            unite(equality->lhs.value, equality->rhs.value);
        }

        std::unordered_map<UInt32, std::vector<JoinOrderColumnId>> members_by_root;
        for (const auto & predicate : predicates)
        {
            const auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&predicate.binding);
            if (!equality)
                continue;
            members_by_root[find(equality->lhs.value)].push_back(equality->lhs);
            members_by_root[find(equality->rhs.value)].push_back(equality->rhs);
        }

        std::vector<std::vector<JoinOrderColumnId>> classes;
        classes.reserve(members_by_root.size());
        for (auto & [_, members] : members_by_root)
        {
            std::ranges::sort(members);
            members.erase(std::unique(members.begin(), members.end()), members.end());
            if (members.size() > 1)
                classes.push_back(std::exchange(members, {}));
        }
        std::ranges::sort(classes, [](const auto & lhs, const auto & rhs) { return lhs.front() < rhs.front(); });

        column_to_equality_class.resize(column_count);
        equality_classes.reserve(classes.size());
        for (size_t index = 0; index < classes.size(); ++index)
        {
            if (index > std::numeric_limits<UInt32>::max() - 0x80000000U)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many canonical equality classes");
            const UInt32 class_id = checkedJoinOrderUInt32(equality_classes.size(), "equality classes");
            UInt32 native_relation_mask = 0;
            for (const auto column : classes[index])
            {
                column_to_equality_class[column.value] = class_id;
                if (const UInt32 relation = catalog->column(column).relation; relation < 32)
                    native_relation_mask |= UInt32{1} << relation;
            }
            equality_member_count += classes[index].size();
            equality_classes.push_back({std::exchange(classes[index], {}), 0x80000000U + class_id, false, native_relation_mask});
        }

        for (const auto & predicate : predicates)
        {
            const auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&predicate.binding);
            if (!equality || !equality->members_incomparable)
                continue;
            if (const auto class_index = column_to_equality_class[equality->lhs.value])
                equality_classes[*class_index].incomparable = true;
        }

        column_to_keys.resize(column_count);
        relation_has_nullable_trusted_key.assign(relation_count, false);
        relation_has_usable_key.assign(relation_count, false);
        for (UInt32 relation = 0; relation < relation_count; ++relation)
        {
            for (const auto key_id : catalog->uniqueKeysForRelation(relation))
            {
                if (!catalog->isTrustedUniqueKey(key_id))
                    continue;
                const auto columns = catalog->columns(catalog->uniqueKey(key_id).columns);
                if (columns.empty())
                    continue;
                const bool usable = std::ranges::all_of(
                    columns,
                    [&](JoinOrderColumnId column)
                    {
                        return column.value < column_count && catalog->column(column).relation == relation
                            && catalog->isIntrinsicNonNull(column);
                    });
                if (!usable)
                {
                    relation_has_nullable_trusted_key[relation] = true;
                    continue;
                }
                const UInt32 internal_id = checkedJoinOrderUInt32(usable_keys.size(), "usable keys");
                usable_keys.push_back({relation, {columns.begin(), columns.end()}, key_id});
                relation_has_usable_key[relation] = true;
                for (const auto column : columns)
                    column_to_keys[column.value].push_back(internal_id);
            }
        }
    }

    bool owns(UInt64 token) const { return token == provider_id; }
    bool usesNativeSubsets() const { return relation_count <= 32; }

    template <typename Function>
    void forEachGroupRelation(const Group & group, Function && function) const
    {
        if (usesNativeSubsets())
        {
            UInt32 subset = group.native_subset;
            while (subset)
            {
                const UInt32 relation = std::countr_zero(subset);
                function(relation);
                subset &= subset - 1;
            }
            return;
        }

        for (const UInt32 relation : relation_subsets.get(group.generic_subset))
            function(relation);
    }

    bool groupContains(const Group & group, UInt32 relation) const
    {
        if (usesNativeSubsets())
            return relation < 32 && (group.native_subset & (UInt32{1} << relation));
        const auto relations = relation_subsets.get(group.generic_subset);
        return std::ranges::binary_search(relations, relation);
    }

    bool groupsIntersect(const Group & lhs, const Group & rhs) const
    {
        if (usesNativeSubsets())
            return lhs.native_subset & rhs.native_subset;

        const auto lhs_relations = relation_subsets.get(lhs.generic_subset);
        const auto rhs_relations = relation_subsets.get(rhs.generic_subset);
        size_t lhs_index = 0;
        size_t rhs_index = 0;
        while (lhs_index < lhs_relations.size() && rhs_index < rhs_relations.size())
        {
            if (lhs_relations[lhs_index] == rhs_relations[rhs_index])
                return true;
            if (lhs_relations[lhs_index] < rhs_relations[rhs_index])
                ++lhs_index;
            else
                ++rhs_index;
        }
        return false;
    }

    bool predicateApplies(const JoinOrderCanonicalPredicate & predicate, const Group & group) const
    {
        for (const auto relation : predicate.applicability)
            if (relation >= relation_count || !groupContains(group, checkedJoinOrderUInt32(relation, "relations")))
                return false;
        return true;
    }

    const std::vector<bool> & baseEqualityClasses(Group & group) const
    {
        if (group.base_equality_classes)
            return *group.base_equality_classes;

        /// Union-find over columns using only the equality predicates applicable inside the
        /// group: exactly the links the group's subtree enforces on its own.
        std::vector<UInt32> parent(catalog->columnCount());
        for (UInt32 column = 0; column < parent.size(); ++column)
            parent[column] = column;
        auto find = [&](UInt32 column)
        {
            while (parent[column] != column)
                column = parent[column] = parent[parent[column]];
            return column;
        };
        for (const auto & predicate : predicates)
        {
            const auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&predicate.binding);
            if (!equality || !predicateApplies(predicate, group))
                continue;
            parent[find(equality->lhs.value)] = find(equality->rhs.value);
        }

        std::vector<bool> connected_classes(equality_classes.size());
        for (size_t index = 0; index < equality_classes.size(); ++index)
        {
            std::optional<UInt32> root;
            bool connected = true;
            for (const auto member : equality_classes[index].members)
            {
                if (!groupContains(group, catalog->column(member).relation))
                    continue;
                const UInt32 member_root = find(member.value);
                if (root && *root != member_root)
                {
                    connected = false;
                    break;
                }
                root = member_root;
            }
            connected_classes[index] = connected;
        }
        group.base_equality_classes = std::move(connected_classes);
        return *group.base_equality_classes;
    }

    BitSet materializeSubset(const Group & group) const
    {
        BitSet result;
        forEachGroupRelation(group, [&](UInt32 relation) { result.set(relation); });
        return result;
    }

    std::vector<UInt32> & beginGenericSubsetScratch() const
    {
        generic_subset_scratch.clear();
        const size_t previous_capacity = generic_subset_scratch.capacity();
        generic_subset_scratch.reserve(relation_count);
        metrics.generic_subset_scratch_capacity_changes += previous_capacity != generic_subset_scratch.capacity();
        ++metrics.generic_subset_scratch_uses;
        return generic_subset_scratch;
    }

    JoinOrderGroupLookupResult mintNativeGroup(UInt32 subset) const
    {
        if (auto it = native_group_ids.find(subset); it != native_group_ids.end())
            return JoinOrderLogicalGroupId{it->second, provider_id};

        const UInt32 group_id = checkedJoinOrderUInt32(groups.size(), "logical groups");
        groups.push_back({subset, 0, {}, {}, {}});
        native_group_ids.emplace(subset, group_id);
        ++metrics.groups;
        ++metrics.retained_subset_payload_members;
        metrics.retained_subset_payload_bytes += sizeof(UInt32);
        return JoinOrderLogicalGroupId{group_id, provider_id};
    }

    JoinOrderGroupLookupResult mintGenericGroup(std::vector<UInt32> & relations) const
    {
        const UInt32 subset_id = relation_subsets.intern(relations);
        if (auto it = generic_group_ids.find(subset_id); it != generic_group_ids.end())
            return JoinOrderLogicalGroupId{it->second, provider_id};

        const UInt32 group_id = checkedJoinOrderUInt32(groups.size(), "logical groups");
        groups.push_back({0, subset_id, {}, {}, {}});
        generic_group_ids.emplace(subset_id, group_id);
        ++metrics.groups;
        metrics.retained_subset_payload_members = relation_subsets.memberCount();
        metrics.retained_subset_payload_bytes = relation_subsets.retainedBytes();
        return JoinOrderLogicalGroupId{group_id, provider_id};
    }

    JoinOrderColumnSetLookupResult internColumnSetInPlace(std::vector<JoinOrderColumnId> & columns) const
    {
        if (columns.empty())
            return std::unexpected(JoinOrderPropertyUnsupportedReason::EmptyDemand);
        for (const auto column : columns)
            if (!catalog || column.value >= catalog->columnCount())
                return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidColumnId);

        const UInt32 id = column_sets.intern(columns);
        metrics.demands = column_sets.size();
        return JoinOrderColumnSetId{id, provider_id};
    }

    void materializeGroupDiagnostics(Group & group) const
    {
        if (!group.closure.value)
        {
            std::vector<UInt32> closure_atoms;
            closure_atoms.reserve(predicates.size() + equality_classes.size());
            for (const auto & predicate : predicates)
                if (predicateApplies(predicate, group))
                    closure_atoms.push_back(predicate.stable_id);

            for (const auto & equality_class : equality_classes)
            {
                size_t included = 0;
                for (const auto column : equality_class.members)
                    included += groupContains(group, catalog->column(column).relation);
                if (included > 1)
                    closure_atoms.push_back(equality_class.closure_atom_id);
            }

            const UInt32 closure_id = predicate_closures.intern(closure_atoms);
            group.closure = {closure_id, provider_id};
            metrics.retained_expanded_predicate_closure_members = predicate_closures.memberCount();
        }

        if (!group.output_contract.value)
        {
            /// The source-qualified column id fixes relation and output position. Pair it
            /// with the interned type id so display names remain validation-only metadata.
            size_t output_member_count = 0;
            forEachGroupRelation(group, [&](UInt32 relation) { output_member_count += catalog->columnsForRelation(relation).size(); });

            std::vector<UInt64> output_members;
            output_members.reserve(output_member_count);
            forEachGroupRelation(
                group,
                [&](UInt32 relation)
                {
                    for (const auto column : catalog->columnsForRelation(relation))
                    {
                        const auto & definition = catalog->column(column);
                        output_members.push_back((static_cast<UInt64>(column.value) << 32) | definition.type_name.value);
                    }
                });
            const UInt32 output_contract_id = output_contracts.intern(output_members);
            group.output_contract = {output_contract_id, provider_id};
            metrics.retained_expanded_output_contract_members = output_contracts.memberCount();
        }
    }

    void beginCutScratch() const
    {
        if (!cut_scratch_initialized)
        {
            cut_scratch_initialized = true;
            ++metrics.cut_scratch_initializations;
            const size_t previous_left_capacity = cut_left_columns.capacity();
            const size_t previous_right_capacity = cut_right_columns.capacity();
            cut_left_columns.reserve(equality_member_count);
            cut_right_columns.reserve(equality_member_count);
            metrics.cut_scratch_capacity_changes += previous_left_capacity != cut_left_columns.capacity();
            metrics.cut_scratch_capacity_changes += previous_right_capacity != cut_right_columns.capacity();
        }
        cut_left_columns.clear();
        cut_right_columns.clear();
        ++metrics.cut_scratch_uses;
    }

    void beginUniquenessScratch() const
    {
        if (!uniqueness_scratch_initialized)
        {
            uniqueness_scratch_initialized = true;
            ++metrics.uniqueness_scratch_initializations;
            const size_t column_count = catalog->columnCount();
            const size_t previous_reached_capacity = reached_generation.capacity();
            const size_t previous_equality_capacity = equality_fired_generation.capacity();
            const size_t previous_key_generation_capacity = key_remaining_generation.capacity();
            const size_t previous_key_remaining_capacity = key_remaining.capacity();
            const size_t previous_relation_capacity = relation_determined_generation.capacity();
            const size_t previous_queue_capacity = uniqueness_queue.capacity();
            reached_generation.resize(column_count);
            equality_fired_generation.resize(equality_classes.size());
            key_remaining_generation.resize(usable_keys.size());
            key_remaining.resize(usable_keys.size());
            relation_determined_generation.resize(relation_count);
            uniqueness_queue.reserve(column_count);
            metrics.uniqueness_scratch_capacity_changes += previous_reached_capacity != reached_generation.capacity();
            metrics.uniqueness_scratch_capacity_changes += previous_equality_capacity != equality_fired_generation.capacity();
            metrics.uniqueness_scratch_capacity_changes += previous_key_generation_capacity != key_remaining_generation.capacity();
            metrics.uniqueness_scratch_capacity_changes += previous_key_remaining_capacity != key_remaining.capacity();
            metrics.uniqueness_scratch_capacity_changes += previous_relation_capacity != relation_determined_generation.capacity();
            metrics.uniqueness_scratch_capacity_changes += previous_queue_capacity != uniqueness_queue.capacity();
        }

        if (uniqueness_generation == std::numeric_limits<UInt32>::max())
        {
            std::ranges::fill(reached_generation, 0);
            std::ranges::fill(equality_fired_generation, 0);
            std::ranges::fill(key_remaining_generation, 0);
            std::ranges::fill(relation_determined_generation, 0);
            uniqueness_generation = 1;
        }
        else
            ++uniqueness_generation;

        uniqueness_queue.clear();
        ++metrics.uniqueness_scratch_uses;
    }

    JoinOrderUniquenessResult unsupported(JoinOrderPropertyUnsupportedReason reason) const { return reason; }

    JoinOrderPropertyProofId mintProofHandle() const
    {
        if (next_proof_handle == std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many proof handles for canonical join-order properties");
        const JoinOrderPropertyProofId id{next_proof_handle++, provider_id};
        ++metrics.proofs;
        return id;
    }

    const Group * getGroupDefinition(JoinOrderLogicalGroupId id) const
    {
        if (!owns(id.provider) || id.value == 0 || id.value >= groups.size())
            return nullptr;
        return &groups[id.value];
    }

    Group * getMutableGroupDefinition(JoinOrderLogicalGroupId id) const
    {
        if (!owns(id.provider) || id.value == 0 || id.value >= groups.size())
            return nullptr;
        return &groups[id.value];
    }

    const Cut * getCutDefinition(JoinOrderEqualityCutId id) const
    {
        if (!owns(id.provider) || id.value == 0 || id.value >= cuts.size())
            return nullptr;
        return &cuts[id.value];
    }

    Cut * getMutableCutDefinition(JoinOrderEqualityCutId id) const
    {
        if (!owns(id.provider) || id.value == 0 || id.value >= cuts.size())
            return nullptr;
        return &cuts[id.value];
    }
};

JoinOrderCanonicalProperties::JoinOrderCanonicalProperties(
    std::shared_ptr<const JoinOrderDataPropertyCatalog> catalog,
    size_t relation_count,
    std::vector<JoinOrderCanonicalPredicate> predicates,
    std::optional<JoinOrderPropertyUnsupportedReason> region_rejection)
    : impl(std::make_unique<Impl>(std::move(catalog), relation_count, std::move(predicates), region_rejection))
{
}

JoinOrderCanonicalProperties::~JoinOrderCanonicalProperties() = default;
JoinOrderCanonicalProperties::JoinOrderCanonicalProperties(JoinOrderCanonicalProperties &&) noexcept = default;
JoinOrderCanonicalProperties & JoinOrderCanonicalProperties::operator=(JoinOrderCanonicalProperties &&) noexcept = default;

JoinOrderGroupLookupResult JoinOrderCanonicalProperties::getGroup(const BitSet & subset) const
{
    if (impl->region_rejection)
        return std::unexpected(*impl->region_rejection);
    if (!subset)
        return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidSubset);

    if (impl->usesNativeSubsets())
    {
        UInt32 native_subset = 0;
        for (const auto relation : subset)
        {
            if (relation >= impl->relation_count)
                return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidSubset);
            native_subset |= UInt32{1} << relation;
        }
        return impl->mintNativeGroup(native_subset);
    }

    for (const auto relation : subset)
        if (relation >= impl->relation_count)
            return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidSubset);

    auto & relations = impl->beginGenericSubsetScratch();
    SCOPE_EXIT(relations.clear());
    for (const auto relation : subset)
        relations.push_back(checkedJoinOrderUInt32(relation, "relations"));
    return impl->mintGenericGroup(relations);
}

JoinOrderGroupLookupResult JoinOrderCanonicalProperties::getGroup(UInt32 native_subset) const
{
    if (impl->region_rejection)
        return std::unexpected(*impl->region_rejection);
    if (!native_subset)
        return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidSubset);
    if (impl->relation_count < 32)
    {
        const UInt32 valid_mask = impl->relation_count ? (UInt32{1} << impl->relation_count) - 1 : 0;
        if (native_subset & ~valid_mask)
            return std::unexpected(JoinOrderPropertyUnsupportedReason::InvalidSubset);
    }
    if (impl->usesNativeSubsets())
        return impl->mintNativeGroup(native_subset);

    auto & relations = impl->beginGenericSubsetScratch();
    SCOPE_EXIT(relations.clear());
    while (native_subset)
    {
        const UInt32 relation = std::countr_zero(native_subset);
        relations.push_back(relation);
        native_subset &= native_subset - 1;
    }
    return impl->mintGenericGroup(relations);
}

JoinOrderColumnSetLookupResult JoinOrderCanonicalProperties::internColumnSet(std::span<const JoinOrderColumnId> columns) const
{
    std::vector<JoinOrderColumnId> normalized(columns.begin(), columns.end());
    return impl->internColumnSetInPlace(normalized);
}

JoinOrderEqualityCutResult JoinOrderCanonicalProperties::getEqualityCut(JoinOrderLogicalGroupId left, JoinOrderLogicalGroupId right) const
{
    if (!impl->owns(left.provider) || !impl->owns(right.provider))
        return JoinOrderPropertyUnsupportedReason::ProviderMismatch;
    const auto * left_group = impl->getGroupDefinition(left);
    const auto * right_group = impl->getGroupDefinition(right);
    if (!left_group || !right_group || impl->groupsIntersect(*left_group, *right_group))
        return JoinOrderPropertyUnsupportedReason::InvalidGroup;

    const UInt64 cache_key = (static_cast<UInt64>(left.value) << 32) | right.value;
    if (auto it = impl->cut_ids.find(cache_key); it != impl->cut_ids.end())
    {
        ++impl->metrics.cut_cache_hits;
        if (!it->second)
        {
            ++impl->metrics.negative_cut_cache_hits;
            return JoinOrderNoEqualityCut{};
        }
        return JoinOrderEqualityCutId{it->second, impl->provider_id};
    }
    ++impl->metrics.cut_cache_misses;

    impl->beginCutScratch();
    SCOPE_EXIT(impl->cut_left_columns.clear(); impl->cut_right_columns.clear());
    const bool native_groups = impl->usesNativeSubsets();
    for (const auto & equality_class : impl->equality_classes)
    {
        /// Most classes touch neither side of the cut; skip them without visiting members.
        /// A skipped class cannot intersect both groups, so the `incomparable` fail-closed
        /// check below is unaffected.
        if (native_groups
            && (!(equality_class.native_relation_mask & left_group->native_subset)
                || !(equality_class.native_relation_mask & right_group->native_subset)))
            continue;

        const size_t left_begin = impl->cut_left_columns.size();
        const size_t right_begin = impl->cut_right_columns.size();
        for (const auto column : equality_class.members)
        {
            const UInt32 relation = impl->catalog->column(column).relation;
            if (impl->groupContains(*left_group, relation))
                impl->cut_left_columns.push_back(column);
            if (impl->groupContains(*right_group, relation))
                impl->cut_right_columns.push_back(column);
        }
        if (impl->cut_left_columns.size() == left_begin || impl->cut_right_columns.size() == right_begin)
        {
            impl->cut_left_columns.resize(left_begin);
            impl->cut_right_columns.resize(right_begin);
        }
        else if (equality_class.incomparable)
        {
            /// The cut would rely on equating members of a class containing a pair that
            /// `equals` cannot express, so the physical join could not enforce the full cut.
            /// Fail closed for this cut only; unrelated cuts keep their caps.
            return JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType;
        }
    }

    if (impl->cut_left_columns.empty() || impl->cut_right_columns.empty())
    {
        const UInt64 reverse_cache_key = (static_cast<UInt64>(right.value) << 32) | left.value;
        impl->cut_ids.emplace(cache_key, 0);
        impl->cut_ids.emplace(reverse_cache_key, 0);
        return JoinOrderNoEqualityCut{};
    }

    const auto left_set = impl->internColumnSetInPlace(impl->cut_left_columns);
    const auto right_set = impl->internColumnSetInPlace(impl->cut_right_columns);
    if (!left_set)
        return left_set.error();
    if (!right_set)
        return right_set.error();

    const UInt32 cut_id = checkedJoinOrderUInt32(impl->cuts.size(), "equality cuts");
    impl->cuts.push_back({left, right, *left_set, *right_set, {}});
    impl->cut_ids.emplace(cache_key, cut_id);
    ++impl->metrics.cuts;
    return JoinOrderEqualityCutId{cut_id, impl->provider_id};
}

JoinOrderUniquenessResult JoinOrderCanonicalProperties::isUniqueOn(JoinOrderLogicalGroupId group_id, JoinOrderColumnSetId columns_id) const
{
    if (!impl->owns(group_id.provider) || !impl->owns(columns_id.provider))
        return impl->unsupported(JoinOrderPropertyUnsupportedReason::ProviderMismatch);
    auto * group = impl->getMutableGroupDefinition(group_id);
    if (!group)
        return impl->unsupported(JoinOrderPropertyUnsupportedReason::InvalidGroup);
    if (!impl->column_sets.contains(columns_id.value))
        return impl->unsupported(JoinOrderPropertyUnsupportedReason::InvalidColumnId);
    const auto demand = impl->column_sets.get(columns_id.value);
    if (demand.empty())
        return impl->unsupported(JoinOrderPropertyUnsupportedReason::EmptyDemand);

    const Impl::CacheKey cache_key{group_id.value, columns_id.value};
    if (auto it = impl->uniqueness_cache.find(cache_key); it != impl->uniqueness_cache.end())
    {
        ++impl->metrics.cache_hits;
        return it->second;
    }
    ++impl->metrics.cache_misses;

    for (const auto column : demand)
    {
        if (column.value >= impl->catalog->columnCount())
        {
            auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::InvalidColumnId);
            impl->uniqueness_cache.emplace(cache_key, result);
            return result;
        }
        const auto & definition = impl->catalog->column(column);
        if (!impl->groupContains(*group, definition.relation))
        {
            auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::ColumnOutsideGroup);
            impl->uniqueness_cache.emplace(cache_key, result);
            return result;
        }
        if (!definition.intrinsic_non_null)
        {
            auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::NullableDemandColumn);
            impl->uniqueness_cache.emplace(cache_key, result);
            return result;
        }
    }

    bool has_unusable_nullable_key = false;
    impl->forEachGroupRelation(
        *group,
        [&](UInt32 relation)
        { has_unusable_nullable_key |= impl->relation_has_nullable_trusted_key[relation] && !impl->relation_has_usable_key[relation]; });
    if (has_unusable_nullable_key)
    {
        auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::NullableKeyColumn);
        impl->uniqueness_cache.emplace(cache_key, result);
        return result;
    }

    impl->beginUniquenessScratch();
    SCOPE_EXIT(impl->uniqueness_queue.clear());
    const UInt32 generation = impl->uniqueness_generation;
    auto reach = [&](JoinOrderColumnId column)
    {
        if (impl->reached_generation[column.value] != generation)
        {
            impl->reached_generation[column.value] = generation;
            impl->uniqueness_queue.push_back(column);
            return true;
        }
        return false;
    };
    for (const auto column : demand)
        reach(column);

    UInt64 obligation_classes = 0;

    size_t queue_index = 0;
    while (queue_index < impl->uniqueness_queue.size())
    {
        const JoinOrderColumnId column = impl->uniqueness_queue[queue_index++];

        if (const auto class_index = impl->column_to_equality_class[column.value];
            class_index && impl->equality_fired_generation[*class_index] != generation)
        {
            impl->equality_fired_generation[*class_index] = generation;
            bool reached_new_member = false;
            for (const auto member : impl->equality_classes[*class_index].members)
            {
                const auto & definition = impl->catalog->column(member);
                if (!impl->groupContains(*group, definition.relation))
                    continue;
                ++impl->metrics.equality_members_visited;
                if (!definition.intrinsic_non_null)
                {
                    auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::NullableEqualityColumn);
                    impl->uniqueness_cache.emplace(cache_key, result);
                    return result;
                }
                reached_new_member |= reach(member);
            }
            /// The fire was load-bearing only if it reached a new in-group member. A class
            /// whose in-group members are not connected by intra-group predicates is then an
            /// obligation of this proof: the assumed equality holds only if the selected
            /// plan synthesizes it at or below the group's join. A link of an incomparable
            /// class cannot be synthesized at all, so such a proof fails closed.
            if (reached_new_member && !impl->baseEqualityClasses(*group)[*class_index])
            {
                if (impl->equality_classes[*class_index].incomparable)
                {
                    auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType);
                    impl->uniqueness_cache.emplace(cache_key, result);
                    return result;
                }
                /// The obligation ledger has one exact bit per class index. Rather than
                /// aliasing high indices onto one overflow bit (which finalization could
                /// not verify exactly), a proof that cannot record its obligation fails
                /// closed like every other unverifiable assumption.
                if (*class_index >= 64)
                {
                    auto result = impl->unsupported(JoinOrderPropertyUnsupportedReason::UnrepresentableObligation);
                    impl->uniqueness_cache.emplace(cache_key, result);
                    return result;
                }
                obligation_classes |= UInt64{1} << *class_index;
            }
        }

        for (const UInt32 key_index : impl->column_to_keys[column.value])
        {
            const auto & key = impl->usable_keys[key_index];
            if (!impl->groupContains(*group, key.relation))
                continue;
            if (impl->key_remaining_generation[key_index] != generation)
            {
                impl->key_remaining_generation[key_index] = generation;
                impl->key_remaining[key_index] = checkedJoinOrderUInt32(key.columns.size(), "key members");
            }
            if (!impl->key_remaining[key_index])
                continue;

            ++impl->metrics.key_checks;
            --impl->key_remaining[key_index];
            if (impl->key_remaining[key_index])
                continue;

            ++impl->metrics.key_firings;
            if (impl->relation_determined_generation[key.relation] == generation)
                continue;
            impl->relation_determined_generation[key.relation] = generation;
            for (const auto output : impl->catalog->columnsForRelation(key.relation))
                reach(output);
        }
    }

    impl->metrics.maximum_closure_size = std::max<UInt64>(impl->metrics.maximum_closure_size, impl->uniqueness_queue.size());
    bool proven = true;
    impl->forEachGroupRelation(*group, [&](UInt32 relation) { proven &= impl->relation_determined_generation[relation] == generation; });
    JoinOrderUniquenessResult result = proven
        ? JoinOrderUniquenessResult{JoinOrderUniquenessProof{impl->mintProofHandle(), obligation_classes}}
        : JoinOrderUniquenessResult{JoinOrderUniquenessNotProven{}};
    impl->uniqueness_cache.emplace(cache_key, result);
    return result;
}

JoinOrderCardinalityCap JoinOrderCanonicalProperties::inferCardinalityCapForCut(
    JoinOrderLogicalGroupId left, JoinOrderLogicalGroupId right, JoinOrderEqualityCutId cut_id, UInt64 left_rows, UInt64 right_rows) const
{
    if (!impl->owns(left.provider) || !impl->owns(right.provider) || !impl->owns(cut_id.provider))
        return JoinOrderPropertyUnsupportedReason::ProviderMismatch;
    const auto * cut = impl->getCutDefinition(cut_id);
    if (!cut || cut->left != left || cut->right != right)
        return JoinOrderPropertyUnsupportedReason::InvalidCut;

    const auto left_unique = isUniqueOn(left, cut->left_columns);
    const auto right_unique = isUniqueOn(right, cut->right_columns);
    if (const auto * unsupported = getUnsupportedReason(left_unique))
        return *unsupported;
    if (const auto * unsupported = getUnsupportedReason(right_unique))
        return *unsupported;

    const auto * left_proof = getUniquenessProof(left_unique);
    const auto * right_proof = getUniquenessProof(right_unique);
    if (!left_proof && !right_proof)
        return JoinOrderNoCardinalityCapReason::NotProven;

    UInt64 upper_bound = 0;
    UInt64 obligation_classes = 0;
    if (left_proof && right_proof)
    {
        upper_bound = std::min(left_rows, right_rows);
        obligation_classes = left_proof->obligation_classes | right_proof->obligation_classes;
    }
    else if (left_proof)
    {
        upper_bound = right_rows;
        obligation_classes = left_proof->obligation_classes;
    }
    else
    {
        upper_bound = left_rows;
        obligation_classes = right_proof->obligation_classes;
    }
    auto * mutable_cut = impl->getMutableCutDefinition(cut_id);
    if (!mutable_cut->cardinality_proof)
        mutable_cut->cardinality_proof = impl->mintProofHandle();
    return JoinOrderCardinalityCapProof{upper_bound, *mutable_cut->cardinality_proof, obligation_classes};
}

JoinOrderCardinalityCap JoinOrderCanonicalProperties::inferInnerAllCardinalityCap(
    JoinOrderGroupLookupResult left, JoinOrderGroupLookupResult right, UInt64 left_rows, UInt64 right_rows) const
{
    if (!left)
        return left.error();
    if (!right)
        return right.error();

    const auto cut = getEqualityCut(*left, *right);
    if (const auto * unsupported = getUnsupportedReason(cut))
        return *unsupported;
    const auto cut_id = getEqualityCutId(cut);
    if (!cut_id)
        return JoinOrderNoCardinalityCapReason::NoEqualityCut;
    return inferCardinalityCapForCut(*left, *right, *cut_id, left_rows, right_rows);
}

JoinOrderCardinalityCap JoinOrderCanonicalProperties::inferInnerAllCardinalityCap(
    const BitSet & left_subset, const BitSet & right_subset, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const
{
    if (!left_rows || !right_rows)
        return JoinOrderNoCardinalityCapReason::MissingInputRows;
    return inferInnerAllCardinalityCap(getGroup(left_subset), getGroup(right_subset), *left_rows, *right_rows);
}

JoinOrderCardinalityCap JoinOrderCanonicalProperties::inferInnerAllCardinalityCap(
    UInt32 left_subset, UInt32 right_subset, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const
{
    if (!left_rows || !right_rows)
        return JoinOrderNoCardinalityCapReason::MissingInputRows;
    return inferInnerAllCardinalityCap(getGroup(left_subset), getGroup(right_subset), *left_rows, *right_rows);
}

std::optional<JoinOrderPropertyUnsupportedReason> JoinOrderCanonicalProperties::regionUnsupportedReason() const
{
    return impl->region_rejection;
}

std::span<const JoinOrderColumnId> JoinOrderCanonicalProperties::equalityClassMembers(size_t class_index) const
{
    if (class_index >= impl->equality_classes.size())
        return {};
    return impl->equality_classes[class_index].members;
}

size_t JoinOrderCanonicalProperties::equalityClassCount() const
{
    return impl->equality_classes.size();
}

JoinOrderLogicalGroupDescription JoinOrderCanonicalProperties::describeGroup(JoinOrderLogicalGroupId group) const
{
    auto * definition = impl->getMutableGroupDefinition(group);
    if (!definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid canonical join-order logical group {}", group.value);
    impl->materializeGroupDiagnostics(*definition);
    return {impl->region, impl->materializeSubset(*definition), definition->closure, definition->output_contract};
}

JoinOrderCanonicalMetrics JoinOrderCanonicalProperties::getMetrics() const
{
    return impl->metrics;
}

String JoinOrderCanonicalProperties::dumpGroup(JoinOrderLogicalGroupId group) const
{
    const auto description = describeGroup(group);
    return fmt::format(
        "provider={}, region={}, subset=[{}], predicate_closure={}, output_contract={}",
        impl->provider_id,
        description.region.value,
        fmt::join(description.subset, ","),
        description.predicate_closure.value,
        description.output_contract.value);
}

String JoinOrderCanonicalProperties::dumpMetrics() const
{
    const auto value = getMetrics();
    /// Keep each label textually adjacent to its value so a new counter cannot silently
    /// shift the meaning of every later positional format argument.
    const std::pair<std::string_view, UInt64> fields[] = {
        {"groups", value.groups},
        {"demands", value.demands},
        {"cuts", value.cuts},
        {"cache_hits", value.cache_hits},
        {"cache_misses", value.cache_misses},
        {"equality_members", value.equality_members_visited},
        {"key_checks", value.key_checks},
        {"key_firings", value.key_firings},
        {"maximum_closure", value.maximum_closure_size},
        {"proofs", value.proofs},
        {"retained_subset_payload_members", value.retained_subset_payload_members},
        {"retained_subset_payload_bytes", value.retained_subset_payload_bytes},
        {"generic_subset_scratch_capacity_changes", value.generic_subset_scratch_capacity_changes},
        {"generic_subset_scratch_uses", value.generic_subset_scratch_uses},
        {"expanded_predicate_members", value.retained_expanded_predicate_closure_members},
        {"expanded_output_members", value.retained_expanded_output_contract_members},
        {"cut_cache_hits", value.cut_cache_hits},
        {"cut_cache_misses", value.cut_cache_misses},
        {"negative_cut_cache_hits", value.negative_cut_cache_hits},
        {"cut_scratch_initializations", value.cut_scratch_initializations},
        {"cut_scratch_capacity_changes", value.cut_scratch_capacity_changes},
        {"cut_scratch_uses", value.cut_scratch_uses},
        {"uniqueness_scratch_initializations", value.uniqueness_scratch_initializations},
        {"uniqueness_scratch_capacity_changes", value.uniqueness_scratch_capacity_changes},
        {"uniqueness_scratch_uses", value.uniqueness_scratch_uses},
    };
    return fmt::format(
        "{}",
        fmt::join(
            fields | std::views::transform([](const auto & field) { return fmt::format("{}={}", field.first, field.second); }), ", "));
}

String joinOrderPropertyUnsupportedReasonToString(JoinOrderPropertyUnsupportedReason reason)
{
    switch (reason)
    {
        case JoinOrderPropertyUnsupportedReason::MissingCatalog: return "missing-catalog";
        case JoinOrderPropertyUnsupportedReason::NonInnerAllRegion: return "non-inner-all-region";
        case JoinOrderPropertyUnsupportedReason::CrossOrCommaRegion: return "cross-or-comma-region";
        case JoinOrderPropertyUnsupportedReason::NonDeterministicPredicate: return "non-deterministic-predicate";
        case JoinOrderPropertyUnsupportedReason::NullSafeEquality: return "null-safe-equality";
        case JoinOrderPropertyUnsupportedReason::AmbiguousEqualityBinding: return "ambiguous-equality-binding";
        case JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType: return "unsupported-equality-type";
        case JoinOrderPropertyUnsupportedReason::NullableEqualityColumn: return "nullable-equality-column";
        case JoinOrderPropertyUnsupportedReason::AmbiguousOutputContract: return "ambiguous-output-contract";
        case JoinOrderPropertyUnsupportedReason::InvalidSubset: return "invalid-subset";
        case JoinOrderPropertyUnsupportedReason::InvalidColumnId: return "invalid-column-id";
        case JoinOrderPropertyUnsupportedReason::ColumnOutsideGroup: return "column-outside-group";
        case JoinOrderPropertyUnsupportedReason::NullableDemandColumn: return "nullable-demand-column";
        case JoinOrderPropertyUnsupportedReason::NullableKeyColumn: return "nullable-key-column";
        case JoinOrderPropertyUnsupportedReason::EmptyDemand: return "empty-demand";
        case JoinOrderPropertyUnsupportedReason::InvalidGroup: return "invalid-group";
        case JoinOrderPropertyUnsupportedReason::InvalidCut: return "invalid-cut";
        case JoinOrderPropertyUnsupportedReason::ProviderMismatch: return "provider-mismatch";
        case JoinOrderPropertyUnsupportedReason::UnrepresentableObligation: return "unrepresentable-obligation";
    }
    return "unknown";
}

}
