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
};

class GTIDSets
{
public:
    std::vector<GTIDSet> sets;

    void parse(String gtid_format_);
    void update(const GTID & other);

    String toString() const;
    String toPayload() const;
};

}
