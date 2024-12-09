#include "MySQLGtid.h"
#include <boost/algorithm/string.hpp>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void GTIDSet::tryMerge(size_t i)
{
    if ((i + 1) >= intervals.size())
        return;

    if (intervals[i].end != intervals[i + 1].start)
        return;
    intervals[i].end = intervals[i + 1].end;
    intervals.erase(intervals.begin() + i + 1, intervals.begin() + i + 1 + 1);
}

void GTIDSets::parse(String gtid_format)
{
    if (gtid_format.empty())
        return;

    std::vector<String> gtid_sets;
    boost::split(gtid_sets, gtid_format, [](char c) { return c == ','; });

    for (const auto & gs : gtid_sets)
    {
        auto gset = boost::trim_copy(gs);

        std::vector<String> server_ids;
        boost::split(server_ids, gset, [](char c) { return c == ':'; });

        GTIDSet set;
        set.uuid = DB::parse<UUID>(server_ids[0]);

        for (size_t k = 1; k < server_ids.size(); ++k)
        {
            std::vector<String> inters;
            boost::split(inters, server_ids[k], [](char c) { return c == '-'; });

            GTIDSet::Interval val;
            switch (inters.size())
            {
                case 1: {
                    val.start = std::stol(inters[0]);
                    val.end = val.start + 1;
                    break;
                }
                case 2: {
                    val.start = std::stol(inters[0]);
                    val.end = std::stol(inters[1]) + 1;
                    break;
                }
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "GTIDParse: Invalid GTID interval: {}", server_ids[k]);
            }
            set.intervals.emplace_back(val);
        }
        sets.emplace_back(set);
    }
}

void GTIDSets::update(const GTID & other)
{
    for (GTIDSet & set : sets)
    {
        if (set.uuid == other.uuid)
        {
            for (auto i = 0U; i < set.intervals.size(); ++i)
            {
                auto & current = set.intervals[i];

                /// Already Contained.
                if (other.seq_no >= current.start && other.seq_no < current.end)
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "GTIDSets updates other: {} invalid successor to {}",
                        std::to_string(other.seq_no), std::to_string(current.end));
                }

                /// Try to shrink Sequence interval.
                GTIDSet::tryShrink(set, i, current);

                /// Sequence, extend the interval.
                if (other.seq_no == current.end)
                {
                    set.intervals[i].end = other.seq_no + 1;
                    set.tryMerge(i);
                    return;
                }
            }

            /// Add new interval.
            GTIDSet::Interval new_interval{other.seq_no, other.seq_no + 1};
            for (auto it = set.intervals.begin(); it != set.intervals.end(); ++it)
            {
                if (other.seq_no < (*it).start)
                {
                    set.intervals.insert(it, new_interval);
                    return;
                }
            }
            set.intervals.emplace_back(new_interval);
            return;
        }
    }

    GTIDSet set;
    GTIDSet::Interval interval{other.seq_no, other.seq_no + 1};
    set.uuid = other.uuid;
    set.intervals.emplace_back(interval);
    sets.emplace_back(set);
}

void GTIDSet::tryShrink(GTIDSet & set, unsigned int i, GTIDSet::Interval & current)
{
    if (i != set.intervals.size() -1)
    {
        auto & next = set.intervals[i+1];
        if (current.end == next.start)
            set.tryMerge(i);
    }
}

String GTIDSets::toString() const
{
    WriteBufferFromOwnString buffer;

    for (size_t i = 0; i < sets.size(); ++i)
    {
        GTIDSet set = sets[i];
        writeUUIDText(set.uuid, buffer);

        for (const auto & interval : set.intervals)
        {
            buffer.write(':');
            auto start = interval.start;
            auto end = interval.end;

            if (end == (start + 1))
            {
                writeString(std::to_string(start), buffer);
            }
            else
            {
                writeString(std::to_string(start), buffer);
                buffer.write('-');
                writeString(std::to_string(end - 1), buffer);
            }
        }

        if (i < (sets.size() - 1))
        {
            buffer.write(',');
        }
    }
    return buffer.str();
}

String GTIDSets::toPayload() const
{
    WriteBufferFromOwnString buffer;

    UInt64 sets_size = sets.size();
    buffer.write(reinterpret_cast<const char *>(&sets_size), 8);

    for (const auto & set : sets)
    {
        // MySQL UUID is big-endian.
        writeBinaryBigEndian(UUIDHelpers::getHighBytes(set.uuid), buffer);
        writeBinaryBigEndian(UUIDHelpers::getLowBytes(set.uuid), buffer);

        UInt64 intervals_size = set.intervals.size();
        buffer.write(reinterpret_cast<const char *>(&intervals_size), 8);
        for (const auto & interval : set.intervals)
        {
            buffer.write(reinterpret_cast<const char *>(&interval.start), 8);
            buffer.write(reinterpret_cast<const char *>(&interval.end), 8);
        }
    }
    return buffer.str();
}

bool GTIDSet::contains(const GTIDSet & gtid_set) const
{
    //we contain the other set if each of its intervals are contained in any of our intervals.
    //use the fact that intervals are sorted to make this linear instead of quadratic.
    if (uuid != gtid_set.uuid) { return false; }

    auto mine = intervals.begin(), other = gtid_set.intervals.begin();
    auto my_end = intervals.end(), other_end = gtid_set.intervals.end();
    while (mine != my_end && other != other_end)
    {
        bool mine_contains_other = mine->start <= other->start && mine->end >= other->end;
        if (mine_contains_other)
        {
            ++other;
        }
        else
        {
            ++mine;
        }
    }

    return other == other_end; //if we've iterated through all intervals in the argument, all its intervals are contained in this
}

bool GTIDSets::contains(const GTIDSet & gtid_set) const
{
    for (const auto & my_gtid_set : sets)
    {
        if (my_gtid_set.contains(gtid_set)) { return true; }
    }
    return false;
}

bool GTIDSets::contains(const GTIDSets & gtid_sets) const
{
    for (const auto & gtid_set : gtid_sets.sets)
    {
        if (!this->contains(gtid_set)) { return false; }
    }
    return true;
}

}
