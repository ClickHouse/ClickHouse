#pragma once
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
        DayNum_t left_date;
        DayNum_t right_date;
        Int64 left;
        Int64 right;
        UInt32 level;
        std::string name;
        DayNum_t month;

        bool operator<(const Part & rhs) const
        {
            if (month != rhs.month)
                return month < rhs.month;

            if (left != rhs.left)
                return left < rhs.left;
            if (right != rhs.right)
                return right < rhs.right;

            if (level != rhs.level)
                return level < rhs.level;

            return false;
        }

        /// Contains another part (obtained after combining another part with some other)
        bool contains(const Part & rhs) const
        {
            return month == rhs.month        /// Parts for different months are not combined
                && left_date <= rhs.left_date
                && right_date >= rhs.right_date
                && left <= rhs.left
                && right >= rhs.right
                && level >= rhs.level;
        }
    };

    void add(const String & name);

    /// If not found, returns an empty string.
    String getContainingPart(const String & name) const;

    Strings getParts() const; /// In ascending order of the month and block number.

    size_t size() const;

    static String getPartName(DayNum_t left_date, DayNum_t right_date, Int64 left_id, Int64 right_id, UInt64 level);

    /// Returns true if the directory name matches the format of the directory name of the parts
    static bool isPartDirectory(const String & dir_name);

    static bool parsePartNameImpl(const String & dir_name, Part * part);

    /// Put data in DataPart from the name of the part.
    static void parsePartName(const String & dir_name, Part & part);

    static bool contains(const String & outer_part_name, const String & inner_part_name);

private:
    using Parts = std::set<Part>;

    mutable std::mutex mutex;
    Parts parts;

    /// Do not block mutex.
    void addImpl(const String & name);
    String getContainingPartImpl(const String & name) const;
};

}
