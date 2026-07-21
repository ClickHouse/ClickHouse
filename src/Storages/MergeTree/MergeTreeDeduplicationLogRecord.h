#pragma once
#include <cstdint>
#include <string>

namespace DB
{

/// Deduplication operation stored on disk: a part was added, dropped, or a
/// drop was rolled back.
enum class MergeTreeDeduplicationOp : uint8_t
{
    ADD = 1,
    DROP = 2,
    /// Written when a part drop that had already durably written some of its
    /// DROP records fails and must be rolled back (e.g. a write, rotation or
    /// fsync failure right after the writes): one CANCEL per written DROP. On
    /// replay it cancels the matching preceding DROP of the same block id and
    /// the same part name (block ids can be reused across part generations, so
    /// the part name pins the exact DROP this rollback undoes), so a
    /// rolled-back drop does not erase a block id that stayed published. The
    /// record carries the real part name, so a server from before this op
    /// existed replays it as the insert that restores the block id - the
    /// rollback's intended net effect (only the entry's position in the
    /// eviction order diverges) - keeping the log downgrade-safe without a
    /// format version. The rollback of a failed *insert* deliberately does not
    /// use this op: replaying an unknown op as an insert would keep the
    /// never-committed block id published on an older server, silently
    /// deduplicating - and dropping the data of - a client retry of the failed
    /// insert. It is encoded as a DROP record carrying
    /// DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME instead, which an older server
    /// replays as the erase that unpublishes the block id.
    CANCEL = 3,
};

/// The part name carried by a DROP record that rolls back the ADD record(s) of a
/// failed insert. It can never collide with a real record: real part names
/// always end in `_<min>_<max>_<level>`. The part name of a DROP record is never
/// parsed, on any server version, so an older server simply replays such a
/// record as a plain erase of the rolled-back block id - the correct net effect -
/// while servers with this code recognize the marker and cancel the (ADD, DROP)
/// pair out of the replay entirely, so the rolled-back insert does not consume a
/// deduplication-window slot either.
constexpr const char * DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME = "cancel";

/// Record for deduplication on disk
struct MergeTreeDeduplicationLogRecord
{
    MergeTreeDeduplicationOp operation{};
    std::string part_name;
    std::string block_id;
};

}
