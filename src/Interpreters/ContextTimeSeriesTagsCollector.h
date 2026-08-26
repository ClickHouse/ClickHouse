#pragma once

#include <Common/SharedMutex.h>
#include <Core/Types.h>


namespace DB
{

/// Mapping between identifiers and tags which are collected in the context of the currently executed query.
class ContextTimeSeriesTagsCollector
{
public:
    ContextTimeSeriesTagsCollector();
    ~ContextTimeSeriesTagsCollector();

    /// Sorted list of tags and their values.
    using TagNamesAndValues = std::vector<std::pair<String, String>>;
    using TagNamesAndValuesPtr = std::shared_ptr<const TagNamesAndValues>;

    /// Adds a mapping between a specified `id` and `tags` to the mapping.
    /// Different identifiers with the same set of tags are allowed to add to the mapping,
    /// the same identifier with different sets of tags are not allowed.
    template <typename IDType>
    void add(const IDType & id, TagNamesAndValuesPtr tags);

    template <typename IDType>
    void add(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector);

    /// Returns the tags corresponding to a specified `id`.
    /// The functions must never be called with identifiers which have not been collected yet.
    template <typename IDType>
    TagNamesAndValuesPtr getTagsById(const IDType & id) const;

    template <typename IDType>
    std::vector<TagNamesAndValuesPtr> getTagsById(const std::vector<IDType> & ids) const;

    /// Returns the group corresponding to a specified `id`.
    /// Groups are numbers 0, 1, 2, 3, ... associated with each collected set of tags.
    /// If the same set of tags have been found multiple times but with different identifiers that means
    /// those identifiers all belong to the same group.
    template <typename IDType>
    size_t getGroupById(const IDType & id) const;

    template <typename IDType>
    std::vector<size_t> getGroupById(const std::vector<IDType> & ids) const;

    /// Returns the tags corresponding to a specified group.
    TagNamesAndValuesPtr getTagsByGroup(size_t group) const;
    std::vector<TagNamesAndValuesPtr> getTagsByGroup(const std::vector<size_t> & groups_) const;

private:
    template <typename IDType>
    void addNoLock(const IDType & id, TagNamesAndValuesPtr tags);

    template <typename IDType>
    TagNamesAndValuesPtr getTagsByIdNoLock(const IDType & id) const;

    template <typename IDType>
    size_t getGroupByIdNoLock(const IDType & id) const;

    TagNamesAndValuesPtr getTagsByGroupNoLock(size_t group) const;

    mutable SharedMutex mutex;

    struct Group
    {
        size_t index = 0;
        TagNamesAndValuesPtr tags;
    };

    std::vector<Group> groups;

    struct Equal
    {
        bool operator()(const TagNamesAndValuesPtr & left, const TagNamesAndValuesPtr & right) const;
    };

    struct Hash
    {
        size_t operator()(const TagNamesAndValuesPtr & ptr) const;
    };

    std::unordered_map<TagNamesAndValuesPtr, size_t, Hash, Equal> group_by_tags;

    template <typename IDType>
    struct MapIdToGroup
    {
        std::unordered_map<IDType, size_t> group_by_id;
    };

    template <typename IDType>
    MapIdToGroup<IDType> & getMapIdToGroup();

    template <typename IDType>
    const MapIdToGroup<IDType> & getMapIdToGroup() const;

    MapIdToGroup<UInt64> map_uint64_id_to_group;
    MapIdToGroup<UInt128> map_uint128_id_to_group;
};

}
