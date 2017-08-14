#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <mutex>
#include <common/DateLUT.h>
#include <Core/Types.h>
#include <set>


namespace DB
{

/** Supports multiple names of active parts of data.
  * Repeats part of the MergeTreeData functionality.
  * TODO: generalize with MergeTreeData. It is possible to leave this class approximately as is and use it from MergeTreeData.
  *       Then in MergeTreeData you can make map<String, DataPartPtr> data_parts and all_data_parts.
  */
class ActiveDataPartSet
{
public:
    ActiveDataPartSet() {}
    ActiveDataPartSet(const Strings & names);

    struct Part
    {
        String name;
        MergeTreePartInfo info;

        bool operator<(const Part & rhs) const { return info < rhs.info; }

        bool contains(const Part & rhs) const { return info.contains(rhs.info); }
    };

    void add(const String & name);

    /// If not found, returns an empty string.
    String getContainingPart(const String & name) const;

    Strings getParts() const; /// In ascending order of the partition_id and block number.

    size_t size() const;

private:
    using Parts = std::set<Part>;

    mutable std::mutex mutex;
    Parts parts;

    /// Do not block mutex.
    void addImpl(const String & name);
    String getContainingPartImpl(const String & name) const;
};

}
