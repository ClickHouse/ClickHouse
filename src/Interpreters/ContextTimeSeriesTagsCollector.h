#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SharedMutex.h>
#include <Common/PODArray_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>
#include <city.h>


namespace DB
{
class ColumnString;

/// Mapping between identifiers and tags which are collected in the context of the currently executed query.
///
/// Identifiers can be of any type. Lookups must use the same type of identifiers which was used to store them.
/// Identifier columns with one of the common fixed-size layouts (UInt64, UInt128, UUID, FixedString(16),
/// and two-element tuples with a UInt64 first component and a UInt64/UInt128/UUID second component)
/// are stored in typed maps keyed by the ids' native binary representation; identifiers of other types
/// are stored in a generic map in serialized form.
/// Identifiers sharing one map are treated as the same identifier if their key representations are identical
/// (for example, UUID, UInt128 and FixedString(16) with the same bit pattern, or, in the generic map,
/// Int64 and FixedString(8) with the same bytes).
///
/// Set of tags are always sorted.
///
/// Different identifiers with the same set of tags are allowed in the mapping,
/// however different sets of tags with the same identifier are not allowed.
///
/// Each unique set of tags is assigned an index named "group". Groups are integers 0, 1, 2, 3, ...
/// If the same set of tags is added multiple times to the mapping only one group will be added.
/// It's so even in the case when the same set of tags is added with different identifiers.
/// Group 0 is always reserved for an empty set of tags.
///
/// Example of the mapping stored in this class:
///                                           -->  group: 0  -->  {}
/// id: 8df7aad3-37c4-49a8-94c4-63fb2e09535c  -->  group: 1  -->  {'__name__': 'http_requests', 'env': 'dev'}
/// id: 060d6345-5438-4fa8-8cae-de9d0099cd2f  -->  group: 2  -->  {'__name__': 'http_requests', 'env': 'prod'}
/// id: 8f1f8376-a0b3-4894-bc5b-3ca49451e275  -->  group: 3  -->  {'__name__': 'http_failures', 'code': '404', 'job': 'prometheus'}
/// id: 82c596fd-78ba-4213-9fa8-91aaaa0d0174  -->  group: 3  -->  {'__name__': 'http_failures', 'code': '404', 'job': 'prometheus'}
/// id: 0ac8129e-3248-4f81-b636-5779eb6e7782  -->  group: 4  -->  {'__name__': 'http_response_bytes', 'env': 'prod'}
/// ...
///
class ContextTimeSeriesTagsCollector
{
public:
    ContextTimeSeriesTagsCollector();
    ~ContextTimeSeriesTagsCollector();

    /// A sorted list of tags with their values.
    using TagNamesAndValues = VectorWithMemoryTracking<std::pair<String, String>>;
    using TagNamesAndValuesPtr = std::shared_ptr<const TagNamesAndValues>;

    static String toString(const TagNamesAndValues & tags);
    static String toString(const TagNamesAndValuesPtr & tags);

    /// A group is just an integer.
    using Group = UInt64;

    /// Adds mapping between identifiers from a column and sets of tags to the collector.
    /// `id_column` is allowed to be Nullable, in that case rows with NULL identifiers are skipped.
    /// `tags_vector` must contain one element per row of `id_column`.
    void storeTags(const ColumnPtr & id_column, const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector);

    /// Returns the group assigned to a specified set of tags.
    /// If that set of tags hasn't been added to the collector yet then this functions adds it.
    Group getGroupForTags(const TagNamesAndValuesPtr & tags);
    VectorWithMemoryTracking<Group> getGroupForTags(const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector);

    /// Group #0 is always reserved for an empty set of tags.
    static Group getGroupForNoTags() { return 0; }

    /// Returns the set of tags which is assigned a specified group.
    TagNamesAndValuesPtr getTagsByGroup(Group group) const;
    VectorWithMemoryTracking<TagNamesAndValuesPtr> getTagsByGroup(const VectorWithMemoryTracking<Group> & groups_) const;

    /// Returns a sampling key for a specified group. The sampling key is a stable UInt64 hash
    /// derived from the tags of a specified group. It's intended as a deterministic sort key
    /// for sampling operations like `limitk` and `limit_ratio`.
    UInt64 getSamplingKeyByGroup(Group group) const;
    void getSamplingKeyByGroup(const VectorWithMemoryTracking<Group> & groups_, PaddedPODArray<UInt64> & res) const;

    /// Extracts the value of a specified tag, or an empty string if there is no such tag in the group.
    String extractTag(Group group, const String & tag_to_extract) const;
    VectorWithMemoryTracking<String> extractTag(const VectorWithMemoryTracking<Group> & groups_, const String & tag_to_extract) const;
    /// Fills `null_map` with 1 for groups without the specified tag.
    void extractTag(
        const VectorWithMemoryTracking<Group> & groups_,
        const String & tag_to_extract,
        ColumnString & out_column,
        PaddedPODArray<UInt8> & null_map) const;

    /// Fills `res` with the groups assigned to the sets of tags which were added to the collector
    /// with identifiers from a column. Throws an exception if some identifier is unknown.
    /// `id_column` must not be Nullable. Any previous contents of `res` are discarded.
    /// Dictionary-encoded (LowCardinality) id components are read through the dictionary and use
    /// the same typed maps as plain components; only the identifiers of actual rows are looked up,
    /// so a shared dictionary is allowed to also contain identifiers whose rows were all filtered
    /// out and which are therefore unknown to the collector.
    void getGroupByID(const ColumnPtr & id_column, PaddedPODArray<Group> & res) const;

    /// Returns the sets of tags which were added to the collector with identifiers from a column.
    /// Throws an exception if some identifier is unknown.
    /// `id_column` must not be Nullable.
    /// Dictionary-encoded identifiers are processed the same way as in getGroupByID.
    VectorWithMemoryTracking<TagNamesAndValuesPtr> getTagsByID(const ColumnPtr & id_column) const;

    /// Removes a tag from a group and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeTag(Group group, const String & tag_to_remove);
    VectorWithMemoryTracking<Group> removeTag(const VectorWithMemoryTracking<Group> & groups_, const String & tag_to_remove);

    /// Removes multiple tags from a group and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeTags(Group group, const Strings & tags_to_remove);
    VectorWithMemoryTracking<Group> removeTags(const VectorWithMemoryTracking<Group> & groups_, const Strings & tags_to_remove);

    /// Removes all tags from a group except specified ones and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeAllTagsExcept(Group group, const Strings & tags_to_keep);
    VectorWithMemoryTracking<Group> removeAllTagsExcept(const VectorWithMemoryTracking<Group> & groups_, const Strings & tags_to_keep);

    /// Copies a specified tag from `src_group` to `dest_group`. The function replaces any previous value of the copied tag in `dest_group`.
    /// If the copied tag doesn't present in `src_group` then the function will remove them in `dest_group` as well.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group copyTag(Group dest_group, Group src_group, const String & tag_to_copy);
    VectorWithMemoryTracking<Group> copyTag(Group dest_group, const VectorWithMemoryTracking<Group> & src_groups, const String & tag_to_copy);
    VectorWithMemoryTracking<Group> copyTag(const VectorWithMemoryTracking<Group> & dest_groups, Group src_group, const String & tag_to_copy);
    VectorWithMemoryTracking<Group> copyTag(const VectorWithMemoryTracking<Group> & dest_groups, const VectorWithMemoryTracking<Group> & src_groups, const String & tag_to_copy);

    /// Copies specified tags from `src_group` to `dest_group`. The function replaces any previous values of the copied tags in `dest_group`.
    /// If some of the copied tags don't present in `src_group` then the function will remove them in `dest_group` as well.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group copyTags(Group dest_group, Group src_group, const Strings & tags_to_copy);
    VectorWithMemoryTracking<Group> copyTags(Group dest_group, const VectorWithMemoryTracking<Group> & src_groups, const Strings & tags_to_copy);
    VectorWithMemoryTracking<Group> copyTags(const VectorWithMemoryTracking<Group> & dest_groups, Group src_group, const Strings & tags_to_copy);
    VectorWithMemoryTracking<Group> copyTags(const VectorWithMemoryTracking<Group> & dest_groups, const VectorWithMemoryTracking<Group> & src_groups, const Strings & tags_to_copy);

    /// Joins all the values of all the `src_tags` using `separator` and returns a new group with the tag `dest_tag` set to the joined value.
    /// This function implements the logic of promql function label_join().
    Group joinTags(Group group, const String & dest_tag, const String & separator, const Strings & src_tags);
    VectorWithMemoryTracking<Group> joinTags(const VectorWithMemoryTracking<Group> & groups, const String & dest_tag, const String & separator, const Strings & src_tags);

    /// Matches the regular expression `regex` against the value of the tag `src_tag`.
    /// If it matches, the value of the tag `dest_tag` in the returned group will be the expansion of `replacement`,
    /// together with the original tags in the input.
    /// Capturing groups in the regular expression can be referenced with $1, $2, etc.
    /// Named capturing groups in the regular expression can be referenced with $name (where name is the capturing group name).
    /// If the regular expression doesn't match then the original group is returned unchanged.
    /// This function implements the logic of promql function label_replace().
    Group replaceTag(Group group, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex);
    VectorWithMemoryTracking<Group> replaceTag(const VectorWithMemoryTracking<Group> & groups, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex);

private:
    /// Transforms the set of tags assigned to a group using a one-argument function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TransformFunc>
    Group transformTags(Group group, TransformFunc && transform_func);

    template <typename TransformFunc>
    VectorWithMemoryTracking<Group> transformTags(const VectorWithMemoryTracking<Group> & groups_, TransformFunc && transform_func);

    /// Transforms the set of tags assigned to a group using a two-arguments function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TransformFunc2>
    Group transformTags2(Group group1, Group group2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    VectorWithMemoryTracking<Group> transformTags2(Group group1, const VectorWithMemoryTracking<Group> & groups2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    VectorWithMemoryTracking<Group> transformTags2(const VectorWithMemoryTracking<Group> & groups1, Group group2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    VectorWithMemoryTracking<Group> transformTags2(const VectorWithMemoryTracking<Group> & groups1, const VectorWithMemoryTracking<Group> & groups2, TransformFunc2 && transform_func);

    /// Key for `groups_for_tags`. The hash is computed once in the constructor.
    struct TagsKey
    {
        TagNamesAndValuesPtr tags;
        UInt64 hash; /// hash calculated from `tags`
        explicit TagsKey(TagNamesAndValuesPtr tags_);
    };

    struct Hash
    {
        size_t operator()(const TagsKey & key) const { return key.hash; }
    };

    struct Equal
    {
        bool operator()(const TagsKey & left, const TagsKey & right) const;
    };

    /// Adds a group associated with a specified set of tags.
    /// If there is such a group already the function returns it.
    Group tryAddGroupUnlocked(const TagNamesAndValuesPtr & tags) TSA_REQUIRES(mutex);
    Group tryAddGroupUnlocked(TagsKey && key) TSA_REQUIRES(mutex);

    /// Hash for the keys of the typed identifier maps.
    struct IDMapHash
    {
        size_t operator()(UInt64 id) const { return DefaultHash<UInt64>{}(id); }
        size_t operator()(const UInt128 & id) const { return DefaultHash<UInt128>{}(id); }
        size_t operator()(const std::pair<UInt64, UInt64> & id) const { return CityHash_v1_0_2::Hash128to64({id.first, id.second}); }
        size_t operator()(const std::pair<UInt64, UInt128> & id) const { return CityHash_v1_0_2::Hash128to64({id.first, UInt128Hash{}(id.second)}); }
    };

    /// A typed identifier map: the keys are the ids' native binary representation.
    template <typename IDType>
    struct IDMap
    {
        HashMap<IDType, Group, IDMapHash> map;
    };

    /// The generic identifier map: the keys are the ids' serialized form, the key bytes are owned by `arena`.
    struct GenericIDMap
    {
        using Map = HashMapWithSavedHash<std::string_view, Group>;
        Arena arena;
        Map map;
    };

    /// Returns the typed identifier map with the specified key type.
    template <typename IDType>
    IDMap<IDType> & getTypedIDMap() TSA_REQUIRES(mutex);
    template <typename IDType>
    const IDMap<IDType> & getTypedIDMap() const TSA_REQUIRES_SHARED(mutex);

    /// Implementations of storeTags, getGroupByID, getTagsByID for identifier columns
    /// matching one of the typed maps. `IDGetter` extracts the identifier for a specified row.
    template <typename IDGetter>
    void storeTagsTyped(const IDGetter & id_getter, const IColumn & id_data, const UInt8 * null_map,
                        size_t num_rows_to_store, const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector);

    template <typename IDGetter>
    void getGroupByIDTyped(const IDGetter & id_getter, const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const;

    template <typename IDGetter>
    VectorWithMemoryTracking<TagNamesAndValuesPtr> getTagsByIDTyped(const IDGetter & id_getter, const IColumn & id_data, size_t num_rows) const;

    /// Implementations of storeTags, getGroupByID, getTagsByID for identifier columns
    /// which don't match any of the typed maps.
    void storeTagsGeneric(const IColumn & id_data, const UInt8 * null_map,
                          size_t num_rows_to_store, const VectorWithMemoryTracking<TagNamesAndValuesPtr> & tags_vector);

    void getGroupByIDGeneric(const IColumn & id_data, size_t num_rows, PaddedPODArray<Group> & res) const;

    VectorWithMemoryTracking<TagNamesAndValuesPtr> getTagsByIDGeneric(const IColumn & id_data, size_t num_rows) const;

    mutable SharedMutex mutex;

    VectorWithMemoryTracking<TagNamesAndValuesPtr> groups TSA_GUARDED_BY(mutex);

    /// Sampling key (stable UInt64 hash of tags) for each group.
    VectorWithMemoryTracking<UInt64> sampling_keys TSA_GUARDED_BY(mutex);

    std::unordered_map<TagsKey, Group, Hash, Equal> groups_for_tags TSA_GUARDED_BY(mutex);

    /// Identifier maps: a typed map per common fixed-size id layout, and the generic map for other id types.
    IDMap<UInt64> id_map_uint64 TSA_GUARDED_BY(mutex);
    IDMap<UInt128> id_map_uint128 TSA_GUARDED_BY(mutex);
    IDMap<std::pair<UInt64, UInt64>> id_map_pair_uint64_uint64 TSA_GUARDED_BY(mutex);
    IDMap<std::pair<UInt64, UInt128>> id_map_pair_uint64_uint128 TSA_GUARDED_BY(mutex);
    GenericIDMap generic_id_map TSA_GUARDED_BY(mutex);
};

}
