#pragma once

#include <Common/SharedMutex.h>
#include <Core/Types.h>


namespace DB
{

/// Mapping between identifiers and tags which are collected in the context of the currently executed query.
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
    using TagNamesAndValues = std::vector<std::pair<String, String>>;
    using TagNamesAndValuesPtr = std::shared_ptr<const TagNamesAndValues>;

    static String toString(const TagNamesAndValues & tags);

    /// A group is just an integer.
    using Group = UInt64;

    /// Adds mapping between a specified identifier and a set of tags to the collector.
    /// The function assigns a group to that set of tags and returns it.
    template <typename IDType>
    void addIDForTags(const IDType & id, const TagNamesAndValuesPtr & tags);

    template <typename IDType>
    void addIDForTags(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector);

    /// Returns the group assigned to a specified set of tags.
    /// If that set of tags hasn't been added to the collector yet then this functions adds it.
    Group getGroupForTags(const TagNamesAndValuesPtr & tags);
    std::vector<Group> getGroupForTags(const std::vector<TagNamesAndValuesPtr> & tags_vector);

    /// Group #0 is always reserved for an empty set of tags.
    static Group getGroupForNoTags() { return 0; }

    /// Returns the set of tags which is assigned a specified group.
    TagNamesAndValuesPtr getTagsByGroup(Group group) const;
    std::vector<TagNamesAndValuesPtr> getTagsByGroup(const std::vector<Group> & groups_) const;

    /// Returns the group assigned to the set of tags which was added to the collector
    /// with a specified identifier.
    template <typename IDType>
    Group getGroupByID(const IDType & id) const;

    template <typename IDType>
    std::vector<Group> getGroupByID(const std::vector<IDType> & ids) const;

    /// Returns the set of tags which was added to the collector with a specified identifier.
    template <typename IDType>
    TagNamesAndValuesPtr getTagsByID(const IDType & id) const;

    template <typename IDType>
    std::vector<TagNamesAndValuesPtr> getTagsByID(const std::vector<IDType> & ids) const;

    /// Removes a tag from a group and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeTagFromGroup(Group group, const String & tag_to_remove);
    std::vector<Group> removeTagFromGroup(const std::vector<Group> & groups_, const String & tag_to_remove);

    /// Removes multiple tags from a group and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeTagsFromGroup(Group group, const Strings & tags_to_remove);
    std::vector<Group> removeTagsFromGroup(const std::vector<Group> & groups_, const Strings & tags_to_remove);

    /// Removes all tags from a group except specified ones and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeAllTagsFromGroupExcept(Group group, const Strings & tags_to_keep);
    std::vector<Group> removeAllTagsFromGroupExcept(const std::vector<Group> & groups_, const Strings & tags_to_keep);

    /// Copies specified tags from `src_group` to `dest_group`. The function replaces any previous values of the copied tags in `dest_group`.
    /// If some of the copied tags don't present in `src_group` then the function will remove them in `dest_group` as well.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group copyTagsToGroup(Group dest_group, Group src_group, const Strings & tags_to_copy);
    std::vector<Group> copyTagsToGroup(Group dest_group, const std::vector<Group> & src_groups, const Strings & tags_to_copy);
    std::vector<Group> copyTagsToGroup(const std::vector<Group> & dest_groups, Group src_group, const Strings & tags_to_copy);
    std::vector<Group> copyTagsToGroup(const std::vector<Group> & dest_groups, const std::vector<Group> & src_groups, const Strings & tags_to_copy);

    /// Joins all the values of all the `src_tags` using `separator` and returns the group with the tag `dest_tag` containing the joined value.
    /// This function implements the logic of promql function label_join().
    Group labelJoin(Group group, const String & dest_tag, const String & separator, const Strings & src_tags);
    std::vector<Group> labelJoin(const std::vector<Group> & groups, const String & dest_tag, const String & separator, const Strings & src_tags);

    /// Matches the regular expression `regex` against the value of the tag `src_tag`.
    /// If it matches, the value of the tag `dest_tag` in the returned group will be the expansion of `replacement`,
    /// together with the original tags in the input.
    /// Capturing groups in the regular expression can be referenced with $1, $2, etc.
    /// Named capturing groups in the regular expression can be referenced with $name (where name is the capturing group name).
    /// If the regular expression doesn't match then the original group is returned unchanged.
    /// This function implements the logic of promql function label_replace().
    Group labelReplace(Group group, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex);
    std::vector<Group> labelReplace(const std::vector<Group> & groups, const String & dest_tag, const String & replacement, const String & src_tag, const String & regex);

private:
    /// Transforms the set of tags assigned to a group using a one-argument function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TranformFunc>
    Group transformTags(Group group, TranformFunc && transform_func);

    template <typename TranformFunc>
    std::vector<Group> transformTags(const std::vector<Group> & groups_, TranformFunc && transform_func);

    /// Transforms the set of tags assigned to a group using a two-arguments function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TransformFunc2>
    Group transformTags2(Group group1, Group group2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(Group group1, const std::vector<Group> & groups2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(const std::vector<Group> & groups1, Group group2, TransformFunc2 && transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(const std::vector<Group> & groups1, const std::vector<Group> & groups2, TransformFunc2 && transform_func);

    mutable SharedMutex mutex;

    std::vector<TagNamesAndValuesPtr> groups;

    struct Equal
    {
        bool operator()(const TagNamesAndValuesPtr & left, const TagNamesAndValuesPtr & right) const;
    };

    struct Hash
    {
        size_t operator()(const TagNamesAndValuesPtr & ptr) const;
    };

    std::unordered_map<TagNamesAndValuesPtr, Group, Hash, Equal> groups_for_tags;

    template <typename IDType>
    struct IDMap
    {
        std::unordered_map<IDType, Group> groups_by_id;
    };

    template <typename IDType>
    IDMap<IDType> & getIDMap();

    template <typename IDType>
    const IDMap<IDType> & getIDMap() const;

    IDMap<UInt64> uint64_id_map;
    IDMap<UInt128> uint128_id_map;
};

}
