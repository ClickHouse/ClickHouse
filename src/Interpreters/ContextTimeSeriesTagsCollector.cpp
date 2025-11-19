#include <Interpreters/ContextTimeSeriesTagsCollector.h>

#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>

#include <IO/WriteHelpers.h>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using TagNamesAndValues = ContextTimeSeriesTagsCollector::TagNamesAndValues;
    using TagNamesAndValuesPtr = ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr;
}


ContextTimeSeriesTagsCollector::ContextTimeSeriesTagsCollector() = default;
ContextTimeSeriesTagsCollector::~ContextTimeSeriesTagsCollector() = default;

bool ContextTimeSeriesTagsCollector::Equal::operator()(const TagNamesAndValuesPtr & left, const TagNamesAndValuesPtr & right) const
{
    return *left == *right;
}

size_t ContextTimeSeriesTagsCollector::Hash::operator ()(const TagNamesAndValuesPtr & ptr) const
{
    const auto & tags = *ptr;
    UInt64 hash = 0;
    for (const auto & [tag_name, tag_value] : tags)
    {
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_name.data(), tag_name.length(), hash);
        hash = CityHash_v1_0_2::CityHash64WithSeed(tag_value.data(), tag_value.length(), hash);
    }
    return hash;
}

template <typename IDType>
void ContextTimeSeriesTagsCollector::add(const IDType & id, TagNamesAndValuesPtr tags)
{
    std::lock_guard lock{mutex};
    addNoLock(id, tags);
}

template <typename IDType>
void ContextTimeSeriesTagsCollector::add(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector)
{
    chassert(ids.size() == tags_vector.size());
    std::unique_lock lock{mutex};
    for (size_t i = 0; i != ids.size(); ++i)
        addNoLock(ids[i], tags_vector[i]);
}

template <typename IDType>
void ContextTimeSeriesTagsCollector::addNoLock(const IDType & id, TagNamesAndValuesPtr tags)
{
    auto & map_id_to_group = getMapIdToGroup<IDType>();

    auto it = map_id_to_group.group_by_id.find(id);
    if (it != map_id_to_group.group_by_id.end())
    {
        chassert(*groups.at(it->second).tags == *tags);
        return;
    }

    size_t group_index = groups.size();
    auto [it_group, group_inserted] = group_by_tags.try_emplace(tags, group_index);

    if (group_inserted)
    {
        Group group;
        group.tags = tags;
        group.index = group_index;
        groups.emplace_back(std::move(group));
    }
    else
    {
        group_index = it_group->second;
    }

    map_id_to_group.group_by_id[id] = group_index;
}

template <typename IDType>
TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsById(const IDType & id) const
{
    SharedLockGuard lock{mutex};
    return getTagsByIdNoLock(id);
}

template <typename IDType>
std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsById(const std::vector<IDType> & ids) const
{
    std::vector<TagNamesAndValuesPtr> res;
    res.resize(ids.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != ids.size(); ++i)
    {
        res[i] = getTagsByIdNoLock(ids[i]);
    }
    return res;
}

template <typename IDType>
TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByIdNoLock(const IDType & id) const
{
    const auto & map_id_to_group = getMapIdToGroup<IDType>();
    auto it = map_id_to_group.group_by_id.find(id);
    if (it == map_id_to_group.group_by_id.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't find tags for id {} in the context", toString(id));
    return groups[it->second].tags;
}

template <typename IDType>
size_t ContextTimeSeriesTagsCollector::getGroupById(const IDType & id) const
{
    SharedLockGuard lock{mutex};
    return getGroupByIdNoLock(id);
}

template <typename IDType>
std::vector<size_t> ContextTimeSeriesTagsCollector::getGroupById(const std::vector<IDType> & ids) const
{
    std::vector<size_t> res;
    res.resize(ids.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != ids.size(); ++i)
    {
        res[i] = getGroupByIdNoLock(ids[i]);
    }
    return res;
}

template <typename IDType>
size_t ContextTimeSeriesTagsCollector::getGroupByIdNoLock(const IDType & id) const
{
    const auto & map_id_to_group = getMapIdToGroup<IDType>();
    auto it = map_id_to_group.group_by_id.find(id);
    if (it == map_id_to_group.group_by_id.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't find tags for id {} in the context", toString(id));
    return it->second;
}

TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroup(size_t group) const
{
    SharedLockGuard lock{mutex};
    return getTagsByGroupNoLock(group);
}

std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByGroup(const std::vector<size_t> & groups_) const
{
    std::vector<TagNamesAndValuesPtr> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        res[i] = getTagsByGroupNoLock(groups_[i]);
    }
    return res;
}

TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroupNoLock(size_t group) const
{
    return groups[group].tags;
}

template <typename IDType>
ContextTimeSeriesTagsCollector::MapIdToGroup<IDType> & ContextTimeSeriesTagsCollector::getMapIdToGroup()
{
    if constexpr (std::is_same_v<IDType, UInt64>)
    {
        return map_uint64_id_to_group;
    }
    else
    {
        static_assert(std::is_same_v<IDType, UInt128>);
        return map_uint128_id_to_group;
    }
}

template <typename IDType>
const ContextTimeSeriesTagsCollector::MapIdToGroup<IDType> & ContextTimeSeriesTagsCollector::getMapIdToGroup() const
{
    return const_cast<ContextTimeSeriesTagsCollector *>(this)->getMapIdToGroup<IDType>();
}


#define TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(IDType) \
    template void ContextTimeSeriesTagsCollector::add<IDType>(const IDType & id, TagNamesAndValuesPtr tags); \
    template void ContextTimeSeriesTagsCollector::add<IDType>(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector); \
    template ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsById<IDType>(const IDType & id) const; \
    template std::vector<ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsById<IDType>(const std::vector<IDType> & ids) const; \
    template size_t ContextTimeSeriesTagsCollector::getGroupById<IDType>(const IDType & id) const; \
    template std::vector<size_t> ContextTimeSeriesTagsCollector::getGroupById<IDType>(const std::vector<IDType> & ids) const; \

TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt64)
TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt128)

#undef TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE

}
