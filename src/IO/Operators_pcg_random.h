#pragma once

#include <IO/Operators.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

struct PcgSerializer
{
    static void serializePcg32(const pcg32_fast & rng, WriteBuffer & buf)
    {
        writeText(pcg32_fast::multiplier(), buf);
        writeChar(' ', buf);
        writeText(pcg32_fast::increment(), buf);
        writeChar(' ', buf);
        writeText(rng.state_, buf);
    }
};

inline WriteBuffer & operator<<(WriteBuffer & buf, const pcg32_fast & x)
{
    PcgSerializer::serializePcg32(x, buf);
    return buf;
}

struct PcgDeserializer
{
    static void deserializePcg32(pcg32_fast & rng, ReadBuffer & buf)
    {
        decltype(rng.state_) multiplier;
        decltype(rng.state_) increment;
        decltype(rng.state_) state;
        readText(multiplier, buf);
        assertChar(' ', buf);
        readText(increment, buf);
        assertChar(' ', buf);
        readText(state, buf);

        if (multiplier != pcg32_fast::multiplier())
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Incorrect multiplier in pcg32: expected {}, got {}", pcg32_fast::multiplier(), multiplier);
        if (increment != pcg32_fast::increment())
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Incorrect increment in pcg32: expected {}, got {}", pcg32_fast::increment(), increment);

        rng.state_ = state;
    }
};

template <>
inline ReadBuffer & operator>>(ReadBuffer & buf, pcg32_fast & x)
{
    PcgDeserializer::deserializePcg32(x, buf);
    return buf;
}

}
