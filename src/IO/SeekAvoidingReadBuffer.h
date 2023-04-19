#pragma once

#include <IO/ReadBufferFromFileDecorator.h>


namespace DB
{

/// `SeekAvoidingReadBuffer` prefers sequential reads over seeks within specified window.
/// It is useful in network and spinning disk storage media when seek is relatively expensive
/// operation.
/// See also: `merge_tree_min_rows_for_seek`.
class SeekAvoidingReadBuffer : public ReadBufferFromFileDecorator
{
public:
    SeekAvoidingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> impl_, UInt64 min_bytes_for_seek_);

    off_t seek(off_t off, int whence) override;

    void prefetch() override { impl->prefetch(); }

private:
    UInt64 min_bytes_for_seek; /// Minimum positive seek offset which shall be executed using seek operation.
};

}
