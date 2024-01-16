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

ActiveDataPartSet::AddPartOutcome ActiveDataPartSet::tryAddPart(const MergeTreePartInfo & part_info, String * out_reason)
{
    return addImpl(part_info, part_info.getPartNameAndCheckFormat(format_version), nullptr, out_reason);
}

bool ActiveDataPartSet::add(const MergeTreePartInfo & part_info, const String & name, Strings * out_replaced_parts)
{
    String out_reason;
    AddPartOutcome outcome = addImpl(part_info, name, out_replaced_parts, &out_reason);
    if (outcome == AddPartOutcome::HasIntersectingPart)
    {
        chassert(!out_reason.empty());
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::runtime(out_reason));
    }

    return outcome == AddPartOutcome::Added;
}

void ActiveDataPartSet::checkIntersectingParts(const MergeTreePartInfo & part_info) const
{
    auto it = part_info_to_name.lower_bound(part_info);
    /// Let's go left.
    while (it != part_info_to_name.begin())
    {
        --it;
        if (!part_info.contains(it->first))
        {
            if (!part_info.isDisjoint(it->first))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}. It is a bug or a result of manual intervention in the ZooKeeper data.", part_info.getPartNameForLogs(), it->first.getPartNameForLogs());
            ++it;
            break;
        }
    }
    /// Let's go to the right.
    while (it != part_info_to_name.end() && part_info.contains(it->first))
    {
        assert(part_info != it->first);
        ++it;
    }

    if (it != part_info_to_name.end() && !part_info.isDisjoint(it->first))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects next part {}. It is a bug or a result of manual intervention in the ZooKeeper data.", part_info.getPartNameForLogs(), it->first.getPartNameForLogs());

}

void ActiveDataPartSet::checkIntersectingParts(const String & name) const
{
    auto part_info = MergeTreePartInfo::fromPartName(name, format_version);
    checkIntersectingParts(part_info);
}

bool ActiveDataPartSet::add(const String & name, Strings * out_replaced_parts)
{
    auto part_info = MergeTreePartInfo::fromPartName(name, format_version);
    String out_reason;
    AddPartOutcome outcome = addImpl(part_info, name, out_replaced_parts, &out_reason);
    if (outcome == AddPartOutcome::HasIntersectingPart)
    {
        chassert(!out_reason.empty());
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::runtime(out_reason));
    }

    return outcome == AddPartOutcome::Added;
}


ActiveDataPartSet::AddPartOutcome ActiveDataPartSet::addImpl(const MergeTreePartInfo & part_info, const String & name, Strings * out_replaced_parts, String * out_reason)
{
    /// TODO make it exception safe (out_replaced_parts->push_back(...) may throw)

    if (getContainingPartImpl(part_info) != part_info_to_name.end())
        return AddPartOutcome::HasCovering;

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
            {
                if (out_reason != nullptr)
                    *out_reason = fmt::format(
                        "Part {} intersects previous part {}. "
                        "It is a bug or a result of manual intervention in the ZooKeeper data.",
                        part_info.getPartNameForLogs(),
                        it->first.getPartNameForLogs());
                return AddPartOutcome::HasIntersectingPart;
            }
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
    {
        if (out_reason != nullptr)
            *out_reason = fmt::format(
                "Part {} intersects part {}. It is a bug or a result of manual intervention "
                "in the ZooKeeper data.",
                name,
                it->first.getPartNameForLogs());

        return AddPartOutcome::HasIntersectingPart;
    }

    part_info_to_name.emplace(part_info, name);
    return AddPartOutcome::Added;

}

bool ActiveDataPartSet::add(const MergeTreePartInfo & part_info, Strings * out_replaced_parts)
{
    String out_reason;
    AddPartOutcome outcome = addImpl(part_info, part_info.getPartNameAndCheckFormat(format_version), out_replaced_parts, &out_reason);
    if (outcome == AddPartOutcome::HasIntersectingPart)
    {
        chassert(!out_reason.empty());
        throw Exception(ErrorCodes::LOGICAL_ERROR, fmt::runtime(out_reason));
    }

    return outcome == AddPartOutcome::Added;
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


std::vector<std::map<MergeTreePartInfo, String>::const_iterator> ActiveDataPartSet::getPartsCoveredByImpl(const MergeTreePartInfo & part_info) const
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

    std::vector<std::map<MergeTreePartInfo, String>::const_iterator> covered;
    for (auto it = begin; it != end; ++it)
        covered.push_back(it);

    return covered;
}

Strings ActiveDataPartSet::getPartsCoveredBy(const MergeTreePartInfo & part_info) const
{
    Strings covered;
    for (const auto & it : getPartsCoveredByImpl(part_info))
        covered.push_back(it->second);
    return covered;
}

std::vector<MergeTreePartInfo> ActiveDataPartSet::getPartInfosCoveredBy(const MergeTreePartInfo & part_info) const
{
    std::vector<MergeTreePartInfo> covered;
    for (const auto & it : getPartsCoveredByImpl(part_info))
        covered.push_back(it->first);
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

std::vector<MergeTreePartInfo> ActiveDataPartSet::getPartInfos() const
{
    std::vector<MergeTreePartInfo> res;
    res.reserve(part_info_to_name.size());
    for (const auto & kv : part_info_to_name)
        res.push_back(kv.first);

    return res;
}

size_t ActiveDataPartSet::size() const
{
    return part_info_to_name.size();
}

}
