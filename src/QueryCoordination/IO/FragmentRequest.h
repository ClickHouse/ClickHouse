#pragma once

#include <Common/typeid_cast.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <vector>

namespace DB
{

using Destinations = std::vector<String>;
using Sources = std::unordered_map<UInt32, std::vector<String>>; // exchange id, Sources

class FragmentRequest
{
public:
    void write(WriteBuffer & out) const
    {
        writeVarInt(fragment_id, out);

        size_t to_size = data_to.size();
        writeVarUInt(to_size, out);

        for (const String & to : data_to)
        {
            writeStringBinary(to, out);
        }

        size_t from_size = data_from.size();
        writeVarUInt(from_size, out);

        for (const auto & [exchange_id, sources] : data_from)
        {
            writeVarUInt(exchange_id, out);

            size_t e_source_size = sources.size();
            writeVarUInt(e_source_size, out);

            for (const auto & source : sources)
            {
                writeStringBinary(source, out);
            }
        }
    }

    void read(ReadBuffer & in)
    {
        readVarInt(fragment_id, in);

        size_t to_size = 0;
        readVarUInt(to_size, in);
        data_to.reserve(to_size);

        for (size_t i = 0; i < to_size; ++i)
        {
            String to;
            readStringBinary(to, in);
            data_to.emplace_back(to);
        }

        size_t from_size = 0;
        readVarUInt(from_size, in);

        for (size_t i = 0; i < from_size; ++i)
        {
            UInt32 exchange_id;
            readVarUInt(exchange_id, in);
            auto & e_sources = data_from[exchange_id];

            size_t e_source_size;
            readVarUInt(e_source_size, in);
            e_sources.reserve(e_source_size);

            for (size_t j = 0; j < e_source_size; ++j)
            {
                String source;
                readStringBinary(source, in);
                e_sources.emplace_back(source);
            }
        }
    }

    String toString() const
    {
        String fragment = "Fragment id " + std::to_string(fragment_id) + ".";
        String data_to_str = " Data to: ";
        for (const auto & to : data_to)
        {
            data_to_str += (to + ", ");
        }

        String data_from_str = "Data from: ";

        for (const auto & [exchange_id, sources] : data_from)
        {
            data_from_str += ("exchange_id " + std::to_string(exchange_id)) + " from: ";

            for (const auto & source : sources)
            {
                data_from_str += (source + ", ");
            }
        }

        return fragment + data_to_str + data_from_str;
    }

    Int64 fragment_id;
    Destinations data_to;
    Sources data_from;
};

}
