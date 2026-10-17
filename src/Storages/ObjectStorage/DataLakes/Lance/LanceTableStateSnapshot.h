#pragma once

#include "config.h"

#if USE_LANCE

#include <Core/Types.h>

#include <array>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace DB::Lance
{

struct TableStateSnapshot
{
    static constexpr size_t DIGEST_SIZE = 32;
    using Digest = std::array<UInt8, DIGEST_SIZE>;

    UInt64 version = 0;
    Digest manifest_id{};
    UInt64 manifest_size = 0;
    Digest manifest_sha256{};
    bool has_etag = false;
    Digest etag_sha256{};

    void serialize(WriteBuffer & out) const;
    static TableStateSnapshot deserialize(ReadBuffer & in, int datalake_state_protocol_version);
    void validate(int error_code) const;

    bool operator==(const TableStateSnapshot & other) const = default;
};

}

#endif
