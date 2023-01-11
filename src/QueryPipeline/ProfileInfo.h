#pragma once

#include <base/types.h>
#include <Common/Stopwatch.h>

#include <vector>

namespace DB
{

class Block;
class ReadBuffer;
class WriteBuffer;

/// Information for profiling. See SourceWithProgress.h
struct ProfileInfo
{
    bool started = false;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};    /// Time with waiting time

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    using ProfileInfos = std::vector<const ProfileInfo *>;

    /** Get the number of rows if there were no LIMIT.
      * If there is no LIMIT, 0 is returned.
      * If the query does not contain ORDER BY, the number can be underestimated - return the number of rows in blocks that were read before LIMIT reached.
      * If the query contains an ORDER BY, then returns the exact number of rows as if LIMIT is removed from query.
      */
    size_t getRowsBeforeLimit() const;
    bool hasAppliedLimit() const;

    void update(Block & block);
    void update(size_t num_rows, size_t num_bytes);

    /// Binary serialization and deserialization of main fields.
    /// Writes only main fields i.e. fields that required by internal transmission protocol.
    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    /// Sets main fields from other object (see methods above).
    /// If skip_block_size_info if true, then rows, bytes and block fields are ignored.
    void setFrom(const ProfileInfo & rhs, bool skip_block_size_info);

    /// Only for Processors.
    void setRowsBeforeLimit(size_t rows_before_limit_)
    {
        applied_limit = true;
        rows_before_limit = rows_before_limit_;
    }

private:
    /// For these fields we make accessors, because they must be calculated beforehand.
    mutable bool applied_limit = false;                    /// Whether LIMIT was applied
    mutable size_t rows_before_limit = 0;
    mutable bool calculated_rows_before_limit = false;    /// Whether the field rows_before_limit was calculated
};

}
