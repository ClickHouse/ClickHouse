#pragma once
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
class GTID
{
public:
    UUID uuid;
    Int64 seq_no;

    GTID() : seq_no(0) { }
};

class GTIDSet
{
public:
    struct Interval
    {
        Int64 start;
        Int64 end;
    };

    UUID uuid;
    std::vector<Interval> intervals;

    void tryMerge(size_t i);

    static void tryShrink(GTIDSet & set, unsigned int i, Interval & current);

    bool contains(const GTIDSet & gtid_set) const;
};

class GTIDSets
{
public:
    std::vector<GTIDSet> sets;

    void parse(String gtid_format_);
    void update(const GTID & other);

    String toString() const;
    String toPayload() const;
    bool contains(const GTIDSet & gtid_set) const;
    bool contains(const GTIDSets & gtid_sets) const;
};

inline bool operator==(const GTID & left, const GTID & right)
{
    return left.uuid == right.uuid
           && left.seq_no == right.seq_no;
}

inline bool operator==(const GTIDSet::Interval & left, const GTIDSet::Interval & right)
{
    return left.start == right.start
           && left.end == right.end;
}

inline bool operator==(const GTIDSet & left, const GTIDSet & right)
{
    return left.uuid == right.uuid
           && left.intervals == right.intervals;
}

inline bool operator==(const GTIDSets & left, const GTIDSets & right)
{
    return left.sets == right.sets;
}

}
