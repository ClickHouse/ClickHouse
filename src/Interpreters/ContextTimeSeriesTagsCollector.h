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
    void storeTags(const IDType & id, const TagNamesAndValuesPtr & tags);

    template <typename IDType>
    void storeTags(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector);

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

private:
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
