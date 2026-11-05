#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <functional>
#include <memory>


namespace DB
{

/// The struct holds the write access check for each partition of `INSERT INTO FUNCTION ... PARTITION BY`.
/// The table function checks only the URI template, but a partition id comes from the inserted data and can
/// point outside the granted path: with `WRITE ON URL('^http://host/allowed/.*$')`, the partition id
/// `../protected/x` writes to `http://host/protected/x`.
struct PartitionedWriteAccessCheck
{
    /// The URI template the table function checked. It is kept because a partition's destination is known only at write time.
    String uri_template;

    /// The function checks that the user may write to a partition's destination, and throws otherwise.
    std::function<void(const ContextPtr & context, const String & uri)> check;
};

using PartitionedWriteAccessCheckPtr = std::shared_ptr<const PartitionedWriteAccessCheck>;

}
