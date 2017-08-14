#include <Storages/MergeTree/ActiveDataPartSet.h>


namespace DB
{

ActiveDataPartSet::ActiveDataPartSet(const Strings & names)
{
    for (const auto & name : names)
        addImpl(name);
}


void ActiveDataPartSet::add(const String & name)
{
    std::lock_guard<std::mutex> lock(mutex);
    addImpl(name);
}


void ActiveDataPartSet::addImpl(const String & name)
{
    if (!getContainingPartImpl(name).empty())
        return;

    Part part;
    part.name = name;
    part.info = MergeTreePartInfo::fromPartName(name);

    /// Parts contained in `part` are located contiguously inside `data_parts`, overlapping with the place where the part itself would be inserted.
    Parts::iterator it = parts.lower_bound(part);

    /// Let's go left.
    while (it != parts.begin())
    {
        --it;
        if (!part.contains(*it))
        {
            ++it;
            break;
        }
        parts.erase(it++);
    }

    /// Let's go to the right.
    while (it != parts.end() && part.contains(*it))
    {
        parts.erase(it++);
    }

    parts.insert(part);
}


String ActiveDataPartSet::getContainingPart(const String & part_name) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return getContainingPartImpl(part_name);
}


String ActiveDataPartSet::getContainingPartImpl(const String & part_name) const
{
    Part part;
    part.info = MergeTreePartInfo::fromPartName(part_name);

    /// A part can only be covered/overlapped by the previous or next one in `parts`.
    Parts::iterator it = parts.lower_bound(part);

    if (it != parts.end())
    {
        if (it->name == part_name)
            return it->name;
        if (it->contains(part))
            return it->name;
    }

    if (it != parts.begin())
    {
        --it;
        if (it->contains(part))
            return it->name;
    }

    return String();
}


Strings ActiveDataPartSet::getParts() const
{
    std::lock_guard<std::mutex> lock(mutex);

    Strings res;
    res.reserve(parts.size());
    for (const Part & part : parts)
        res.push_back(part.name);

    return res;
}


size_t ActiveDataPartSet::size() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return parts.size();
}


}
