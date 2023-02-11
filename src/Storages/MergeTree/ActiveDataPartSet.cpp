#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <cassert>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ActiveDataPartSet::ActiveDataPartSet(MergeTreeDataFormatVersion format_version_, const Strings & names)
    : format_version(format_version_)
{
    for (const auto & name : names)
        add(name);
}

bool ActiveDataPartSet::add(const String & name, Strings * out_replaced_parts)
{
    /// TODO make it exception safe (out_replaced_parts->push_back(...) may throw)
    auto part_info = MergeTreePartInfo::fromPartName(name, format_version);

    if (getContainingPartImpl(part_info) != part_info_to_name.end())
        return false;

    /// Parts contained in `part` are located contiguously in `part_info_to_name`, overlapping with the place where the part itself would be inserted.
    auto it = part_info_to_name.lower_bound(part_info);

    if (out_replaced_parts)
        out_replaced_parts->clear();

    /// Let's go left.
    while (it != part_info_to_name.begin())
    {
        --it;
        if (!part_info.contains(it->first))
        {
            if (!part_info.isDisjoint(it->first))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}. It is a bug or a result of manual intervention in the ZooKeeper data.", name, it->first.getPartName());
            ++it;
            break;
        }

        if (out_replaced_parts)
            out_replaced_parts->push_back(it->second);
        it = part_info_to_name.erase(it);
    }

    if (out_replaced_parts)
        std::reverse(out_replaced_parts->begin(), out_replaced_parts->end());

    /// Let's go to the right.
    while (it != part_info_to_name.end() && part_info.contains(it->first))
    {
        assert(part_info != it->first);
        if (out_replaced_parts)
            out_replaced_parts->push_back(it->second);
        it = part_info_to_name.erase(it);
    }

    if (it != part_info_to_name.end() && !part_info.isDisjoint(it->first))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects next part {}. It is a bug or a result of manual intervention in the ZooKeeper data.", name, it->first.getPartName());

    part_info_to_name.emplace(part_info, name);
    return true;
}


String ActiveDataPartSet::getContainingPart(const MergeTreePartInfo & part_info) const
{
    auto it = getContainingPartImpl(part_info);
    if (it != part_info_to_name.end())
        return it->second;
    return {};
}


String ActiveDataPartSet::getContainingPart(const String & name) const
{
    auto it = getContainingPartImpl(MergeTreePartInfo::fromPartName(name, format_version));
    if (it != part_info_to_name.end())
        return it->second;
    return {};
}


std::map<MergeTreePartInfo, String>::const_iterator
ActiveDataPartSet::getContainingPartImpl(const MergeTreePartInfo & part_info) const
{
    /// A part can only be covered/overlapped by the previous or next one in `part_info_to_name`.
    auto it = part_info_to_name.lower_bound(part_info);

    if (it != part_info_to_name.end())
    {
        if (it->first.contains(part_info))
            return it;
    }

    if (it != part_info_to_name.begin())
    {
        --it;
        if (it->first.contains(part_info))
            return it;
    }

    return part_info_to_name.end();
}

Strings ActiveDataPartSet::getPartsCoveredBy(const MergeTreePartInfo & part_info) const
{
    auto it_middle = part_info_to_name.lower_bound(part_info);
    auto begin = it_middle;
    while (begin != part_info_to_name.begin())
    {
        auto prev = std::prev(begin);
        if (!part_info.contains(prev->first))
        {
            if (prev->first.contains(part_info))
                return {};

            break;
        }

        begin = prev;
    }

    auto end = it_middle;
    while (end != part_info_to_name.end())
    {
        if (!part_info.contains(end->first))
        {
            if (end->first.contains(part_info))
                return {};
            break;
        }

        ++end;
    }

    Strings covered;
    for (auto it = begin; it != end; ++it)
        covered.push_back(it->second);

    return covered;
}

Strings ActiveDataPartSet::getParts() const
{
    Strings res;
    res.reserve(part_info_to_name.size());
    for (const auto & kv : part_info_to_name)
        res.push_back(kv.second);

    return res;
}

size_t ActiveDataPartSet::size() const
{
    return part_info_to_name.size();
}

}
