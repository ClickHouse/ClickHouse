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
///
/// Example of the mapping stored in this class:
/// id: 8df7aad3-37c4-49a8-94c4-63fb2e09535c  -->  group: 0  -->  {'__name__': 'http_requests', 'env': 'dev'}
/// id: 060d6345-5438-4fa8-8cae-de9d0099cd2f  -->  group: 1  -->  {'__name__': 'http_requests', 'env': 'prod'}
/// id: 8f1f8376-a0b3-4894-bc5b-3ca49451e275  -->  group: 2  -->  {'__name__': 'http_failures', 'code': '404', 'job': 'prometheus'}
/// id: 82c596fd-78ba-4213-9fa8-91aaaa0d0174  -->  group: 2  -->  {'__name__': 'http_failures', 'code': '404', 'job': 'prometheus'}
/// id: 0ac8129e-3248-4f81-b636-5779eb6e7782  -->  group: 3  -->  {'__name__': 'http_response_bytes', 'env': 'prod'}
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
    using Group = size_t;

    /// Adds mapping between a specified identifier and a set of tags to the collector.
    /// The function assigns a group to that set of tags and returns it.
    template <typename IDType>
    Group insert(const IDType & id, const TagNamesAndValuesPtr & tags);

    template <typename IDType>
    std::vector<Group> insert(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector);

    /// Returns the group assigned to a specified set of tags.
    /// If that set of tags hasn't been added to the collector yet then this functions adds it.
    Group getGroupForTags(const TagNamesAndValuesPtr & tags);
    std::vector<Group> getGroupForTags(const std::vector<TagNamesAndValuesPtr> & tags_vector);

    /// Returns the group assigned to an empty set of tags.
    /// If that set of tags hasn't been added to the collector yet then this functions adds it.
    Group getGroupForNoTags();

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

    /// Removes some tags from a group and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeTagsFromGroup(Group group, const Strings & tags_to_remove);
    std::vector<Group> removeTagsFromGroup(const std::vector<Group> & groups_, const Strings & tags_to_remove);

    /// Removes all tags from a group except specified ones and returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group removeAllTagsFromGroupExcept(Group group, const Strings & tags_to_keep);
    std::vector<Group> removeAllTagsFromGroupExcept(const std::vector<Group> & groups_, const Strings & tags_to_keep);

    /// Appends tags from the second groups to the tags from the first group, returns the result group.
    /// The function also sorts the result set of tags (because a set of tags should always be sorted).
    /// If same tags with different values present in both groups, the function will prefer values from the second group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    Group appendTagsFromGroup(Group group1, Group group2);
    std::vector<Group> appendTagsFromGroup(const std::vector<Group> & groups1, Group group2);
    std::vector<Group> appendTagsFromGroup(Group group1, const std::vector<Group> & groups2);
    std::vector<Group> appendTagsFromGroup(const std::vector<Group> & groups1, const std::vector<Group> & groups2);

private:
    /// Transforms the set of tags assigned to a group using a one-argument function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TranformFunc>
    Group transformTags(Group group, const TranformFunc & transform_func);

    template <typename TranformFunc>
    std::vector<Group> transformTags(const std::vector<Group> & groups_, const TranformFunc & transform_func);

    /// Transforms the set of tags assigned to a group using a two-arguments function, returns the result group.
    /// If the result set of tags hasn't been added to the collector yet then this functions adds it and assigns a group to it.
    template <typename TransformFunc2>
    Group transformTags2(Group group1, Group group2, const TransformFunc2 & transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(Group group1, const std::vector<Group> & groups2, const TransformFunc2 & transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(const std::vector<Group> & groups1, Group group2, const TransformFunc2 & transform_func);

    template <typename TransformFunc2>
    std::vector<Group> transformTags2(const std::vector<Group> & groups1, const std::vector<Group> & groups2, const TransformFunc2 & transform_func);

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
    std::optional<Group> group_for_no_tags;

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
