#include "MySQLGtid.h"

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
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
    std::vector<String> gtid_sets;
    boost::split(gtid_sets, gtid_format, boost::is_any_of(","));

    for (const auto & gset : gtid_sets)
    {
        std::vector<String> server_ids;
        boost::split(server_ids, gset, [](char c) { return c == ':'; });

        GTIDSet set;
        parseUUID(reinterpret_cast<const UInt8 *>(server_ids[0].data()), set.uuid);

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
                    throw Exception("GTIDParse: Invalid GTID interval: " + server_ids[k], ErrorCodes::UNKNOWN_EXCEPTION);
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
        if (std::equal(std::begin(set.uuid), std::end(set.uuid), std::begin(other.uuid)))
        {
            for (auto i = 0U; i < set.intervals.size(); i++)
            {
                auto current = set.intervals[i];

                /// Already Contained.
                if (other.seq_no >= current.start && other.seq_no < current.end)
                {
                    throw Exception(
                        "GTIDSets updates other: " + std::to_string(other.seq_no) + " invalid successor to " + std::to_string(current.end),
                        ErrorCodes::UNKNOWN_EXCEPTION);
                }

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
    memcpy(set.uuid, other.uuid, 16);
    GTIDSet::Interval interval{other.seq_no, other.seq_no + 1};
    set.intervals.emplace_back(interval);
    sets.emplace_back(set);
}

String GTIDSets::toString() const
{
    WriteBufferFromOwnString buffer;

    for (size_t i = 0; i < sets.size(); i++)
    {
        GTIDSet set = sets[i];

        String dst36;
        dst36.resize(36);
        formatUUID(set.uuid, reinterpret_cast<UInt8 *>(dst36.data()));
        writeString(dst36, buffer);

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
        buffer.write(reinterpret_cast<const char *>(set.uuid), sizeof(set.uuid));

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
