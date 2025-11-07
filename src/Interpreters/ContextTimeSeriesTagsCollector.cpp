#include <Interpreters/ContextTimeSeriesTagsCollector.h>

#include <Common/Exception.h>
#include <Common/SharedLockGuard.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


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
    using Group = ContextTimeSeriesTagsCollector::Group;

    const Group INVALID_GROUP = static_cast<Group>(-1);

    [[noreturn]] void throwGroupOutOfBound(Group group, size_t num_groups)
    {
        if (num_groups > 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Group {} is out of bounds, must be between 0 and {}", group, num_groups - 1);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No groups exist");
    }

    template <typename IDType>
    [[noreturn]] void throwIDWasAddedWithOtherTags(const IDType & id, const TagNamesAndValuesPtr & tags, const TagNamesAndValuesPtr & existing_tags)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot add identifier {} with tags {} because it was added before with tags {}",
                        toString(id),
                        ContextTimeSeriesTagsCollector::toString(*tags),
                        ContextTimeSeriesTagsCollector::toString(*existing_tags));
    }

    template <typename IDType>
    [[noreturn]] void throwUnknownID(const IDType & id)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown identifier {}", toString(id));
    }
}


String ContextTimeSeriesTagsCollector::toString(const TagNamesAndValues & tags)
{
    WriteBufferFromOwnString ostr;
    ostr << "{";
    for (size_t i = 0; i != tags.size(); ++i)
    {
        if (i)
            ostr << ", ";
        ostr << quoteString(tags[i].first) << ": " << quoteString(tags[i].second);
    }
    ostr << "}";
    return ostr.str();
}


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


ContextTimeSeriesTagsCollector::ContextTimeSeriesTagsCollector()
{
    /// Group #0 is reserved for an empty set of tags.
    auto no_tags = std::make_shared<TagNamesAndValues>();
    groups.push_back(no_tags);
    groups_for_tags.try_emplace(no_tags, 0);
}


ContextTimeSeriesTagsCollector::~ContextTimeSeriesTagsCollector() = default;


Group ContextTimeSeriesTagsCollector::getGroupForTags(const TagNamesAndValuesPtr & tags)
{
    {
        SharedLockGuard lock{mutex};
        auto it = groups_for_tags.find(tags);
        if (it != groups_for_tags.end())
            return it->second;
    }

    {
        std::lock_guard lock{mutex};
        auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());
        if (inserted)
            groups.push_back(tags);
        return it->second;
    }
}


std::vector<Group> ContextTimeSeriesTagsCollector::getGroupForTags(const std::vector<TagNamesAndValuesPtr> & tags_vector)
{
    std::vector<Group> res;
    res.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found = 0;

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            const auto & tags = tags_vector[i];
            auto it = groups_for_tags.find(tags);
            if (it != groups_for_tags.end())
            {
                res[i] = it->second;
                ++num_found;
            }
        }
    }

    if (num_found != tags_vector.size())
    {
        std::lock_guard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            if (res[i] != INVALID_GROUP)
                continue;
            const auto & tags = tags_vector[i];
            auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());
            if (inserted)
                groups.push_back(tags);
            res[i] = it->second;
            if (++num_found == tags_vector.size())
                break;
        }
    }

    return res;
}


TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByGroup(Group group) const
{
    SharedLockGuard lock{mutex};
    if (group >= groups.size())
        throwGroupOutOfBound(group, groups.size());
    return groups[group];
}


std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByGroup(const std::vector<Group> & groups_) const
{
    std::vector<TagNamesAndValuesPtr> res;
    res.resize(groups_.size());
    SharedLockGuard lock{mutex};
    for (size_t i = 0; i != groups_.size(); ++i)
    {
        Group group = groups_[i];
        if (group >= groups.size())
            throwGroupOutOfBound(group, groups.size());
        res[i] = groups[group];
    }
    return res;
}


template <typename IDType>
void ContextTimeSeriesTagsCollector::storeTags(const IDType & id, const TagNamesAndValuesPtr & tags)
{
    auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    {
        SharedLockGuard lock{mutex};
        auto it = groups_by_id.find(id);
        if (it != groups_by_id.end())
        {
            Group existing_group = it->second;
            if (*tags != *groups.at(existing_group))
                throwIDWasAddedWithOtherTags(id, tags, groups.at(existing_group));
            return;
        }
    }

    {
        std::lock_guard lock{mutex};
        auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());

        if (inserted)
            groups.push_back(tags);

        Group group = it->second;
        auto it2 = groups_by_id.try_emplace(id, group).first;

        if (group != it2->second)
            throwIDWasAddedWithOtherTags(id, tags, groups.at(it2->second));
    }
}


template <typename IDType>
void ContextTimeSeriesTagsCollector::storeTags(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector)
{
    chassert(ids.size() == tags_vector.size());
    auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<Group> found_groups;
    found_groups.resize(tags_vector.size(), INVALID_GROUP);
    size_t num_found_groups = 0;

    {
        SharedLockGuard lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            const auto & id = ids[i];
            const auto & tags = tags_vector[i];
            auto it = groups_by_id.find(id);
            if (it != groups_by_id.end())
            {
                Group existing_group = it->second;
                if (*tags != *groups.at(existing_group))
                    throwIDWasAddedWithOtherTags(id, tags, groups.at(existing_group));
                found_groups[i] = existing_group;
                ++num_found_groups;
            }
        }
    }

    if (num_found_groups == tags_vector.size())
        return;

    {
        std::unique_lock lock{mutex};
        for (size_t i = 0; i != tags_vector.size(); ++i)
        {
            if (found_groups[i] != INVALID_GROUP)
                continue;
            const auto & id = ids[i];
            const auto & tags = tags_vector[i];

            auto [it, inserted] = groups_for_tags.try_emplace(tags, groups.size());

            if (inserted)
                groups.push_back(tags);

            Group group = it->second;
            auto it2 = groups_by_id.try_emplace(id, group).first;

            if (group != it2->second)
                throwIDWasAddedWithOtherTags(id, tags, groups.at(it2->second));

            found_groups[i] = group;
            if (++num_found_groups == tags_vector.size())
                break;
        }
    }
}


template <typename IDType>
Group ContextTimeSeriesTagsCollector::getGroupByID(const IDType & id) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    SharedLockGuard lock{mutex};

    auto it = groups_by_id.find(id);
    if (it == groups_by_id.end())
        throwUnknownID(id);

    return it->second;
}


template <typename IDType>
std::vector<Group> ContextTimeSeriesTagsCollector::getGroupByID(const std::vector<IDType> & ids) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<Group> res;
    res.reserve(ids.size());

    SharedLockGuard lock{mutex};
    for (const auto & id : ids)
    {
        auto it = groups_by_id.find(id);
        if (it == groups_by_id.end())
            throwUnknownID(id);
        res.push_back(it->second);
    }

    return res;
}


template <typename IDType>
TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByID(const IDType & id) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    SharedLockGuard lock{mutex};

    auto it = groups_by_id.find(id);
    if (it == groups_by_id.end())
        throwUnknownID(id);

    return groups[it->second];
}

template <typename IDType>
std::vector<TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID(const std::vector<IDType> & ids) const
{
    const auto & groups_by_id = getIDMap<IDType>().groups_by_id;

    std::vector<TagNamesAndValuesPtr> res;
    res.reserve(ids.size());

    SharedLockGuard lock{mutex};
    for (const auto & id : ids)
    {
        auto it = groups_by_id.find(id);
        if (it == groups_by_id.end())
            throwUnknownID(id);
        res.push_back(groups[it->second]);
    }

    return res;
}


template <typename IDType>
ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getIDMap()
{
    if constexpr (std::is_same_v<IDType, UInt64>)
    {
        return uint64_id_map;
    }
    else
    {
        static_assert(std::is_same_v<IDType, UInt128>);
        return uint128_id_map;
    }
}

template <typename IDType>
const ContextTimeSeriesTagsCollector::IDMap<IDType> & ContextTimeSeriesTagsCollector::getIDMap() const
{
    return const_cast<ContextTimeSeriesTagsCollector *>(this)->getIDMap<IDType>();
}


#define TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(IDType) \
    template void ContextTimeSeriesTagsCollector::storeTags<IDType>(const IDType & id, const TagNamesAndValuesPtr & tags); \
    template void ContextTimeSeriesTagsCollector::storeTags<IDType>(const std::vector<IDType> & ids, const std::vector<TagNamesAndValuesPtr> & tags_vector); \
    template Group ContextTimeSeriesTagsCollector::getGroupByID<IDType>(const IDType & id) const; \
    template std::vector<Group> ContextTimeSeriesTagsCollector::getGroupByID<IDType>(const std::vector<IDType> & ids) const; \
    template ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr ContextTimeSeriesTagsCollector::getTagsByID<IDType>(const IDType & id) const; \
    template std::vector<ContextTimeSeriesTagsCollector::TagNamesAndValuesPtr> ContextTimeSeriesTagsCollector::getTagsByID<IDType>(const std::vector<IDType> & ids) const; \

TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt64)
TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE(UInt128)

#undef TIME_SERIES_ID_TO_TAGS_MAP_INSTANTIATE

}
