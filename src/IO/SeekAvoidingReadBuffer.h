#pragma once

#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/// `SeekAvoidingReadBuffer` prefers sequential reads over seeks within specified window.
/// It is useful in network and spinning disk storage media when seek is relatively expensive
/// operation.
/// See also: `merge_tree_min_rows_for_seek`.
class SeekAvoidingReadBuffer : public ReadBufferFromFileBase
{
    std::unique_ptr<ReadBufferFromFileBase> nested;

    UInt64 min_bytes_for_seek; /// Minimum positive seek offset which shall be executed using seek operation.

public:
    SeekAvoidingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> nested_, UInt64 min_bytes_for_seek_);

    std::string getFileName() const override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;
};

}
