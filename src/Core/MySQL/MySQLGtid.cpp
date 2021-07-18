#include "MySQLGtid.h"
#include <boost/algorithm/string.hpp>
#include <IO/ReadHelpers.h>


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

void GTIDSets::parse(const String gtid_format)
{
    if (gtid_format.empty())
    {
        return;
    }

    std::vector<String> gtid_sets;
    boost::split(gtid_sets, gtid_format, [](char c) { return c == ','; });

    for (const auto & gs : gtid_sets)
    {
        auto gset = boost::trim_copy(gs);

        std::vector<String> server_ids;
        boost::split(server_ids, gset, [](char c) { return c == ':'; });

        GTIDSet set;
        set.uuid = DB::parse<UUID>(server_ids[0]);

        for (size_t k = 1; k < server_ids.size(); k++)
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
                    throw Exception("GTIDParse: Invalid GTID interval: " + server_ids[k], ErrorCodes::LOGICAL_ERROR);
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
            for (auto i = 0U; i < set.intervals.size(); i++)
            {
                auto & current = set.intervals[i];

                /// Already Contained.
                if (other.seq_no >= current.start && other.seq_no < current.end)
                {
                    throw Exception(
                        "GTIDSets updates other: " + std::to_string(other.seq_no) + " invalid successor to " + std::to_string(current.end),
                        ErrorCodes::LOGICAL_ERROR);
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

    for (size_t i = 0; i < sets.size(); i++)
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
        writeBinaryBigEndian(set.uuid.toUnderType().items[0], buffer);
        writeBinaryBigEndian(set.uuid.toUnderType().items[1], buffer);

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

}
