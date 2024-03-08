#pragma once

#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>

namespace DB
{

using Destinations = std::vector<String>;
using Sources = std::unordered_map<UInt32, std::vector<String>>; // exchange id, Sources

struct FragmentRequest
{
    UInt32 fragment_id;
    Destinations data_to;
    Sources data_from;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);

    String toString() const;
};

struct FragmentsRequest
{
    String query;
    std::vector<FragmentRequest> fragments_request;

    void write(WriteBuffer & out) const;
    void read(ReadBuffer & in);

    const std::vector<FragmentRequest> & fragmentsRequest() const { return fragments_request; }
};

}
