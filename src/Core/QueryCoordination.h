#pragma once

#include <Core/Block.h>
#include <Core/Types.h>

#include <functional>
#include <vector>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

enum class QueryCoordinationRequestKind : UInt64
{
    DistributedTopKCandidates = 0,

    MAX = DistributedTopKCandidates,
};

enum class QueryCoordinationRequestMode : UInt64
{
    Candidates = 0,
    FallbackAll = 1,

    MAX = FallbackAll,
};

enum class QueryCoordinationResponseMode : UInt64
{
    Selected = 0,
    FallbackAll = 1,

    MAX = FallbackAll,
};

/** A completed coordination exchange has one request and one response per logical shard.
  * Selection or fallback completes it; cancellation is propagated by the transport owner. The
  * exchange uses the connection owned by that shard's `RemoteQueryExecutor`.
  */
struct QueryCoordinationRequest
{
    static constexpr UInt64 CURRENT_VERSION = 1;

    UInt64 request_id = 0;
    QueryCoordinationRequestKind kind = QueryCoordinationRequestKind::DistributedTopKCandidates;
    UInt64 version = CURRENT_VERSION;
    QueryCoordinationRequestMode mode = QueryCoordinationRequestMode::Candidates;
    Block payload;

    void serialize(WriteBuffer & out, UInt64 peer_revision) const;
    static QueryCoordinationRequest deserialize(ReadBuffer & in, UInt64 peer_revision);
};

struct QueryCoordinationResponse
{
    UInt64 request_id = 0;
    QueryCoordinationResponseMode mode = QueryCoordinationResponseMode::FallbackAll;
    std::vector<UInt64> selected_ordinals;

    void serialize(WriteBuffer & out) const;
    static QueryCoordinationResponse deserialize(ReadBuffer & in, size_t candidate_rows);
};

using QueryCoordinationCallback = std::function<QueryCoordinationResponse(QueryCoordinationRequest)>;

}
