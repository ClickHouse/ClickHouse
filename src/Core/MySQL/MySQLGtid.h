#pragma once
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
class GTID
{
public:
    UInt8 uuid[16];
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

    UInt8 uuid[16];
    std::vector<Interval> intervals;

    void tryMerge(size_t i);
};

class GTIDSets
{
public:
    std::vector<GTIDSet> sets;

    void parse(const String gtid_format_);
    void update(const GTID & other);

    String toString() const;
    String toPayload() const;
};

}
