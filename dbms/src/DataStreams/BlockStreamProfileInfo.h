#pragma once

#include <vector>
#include <Common/Stopwatch.h>

#include <Core/Types.h>

#if __APPLE__
#include <common/apple_rt.h>
#endif

namespace DB
{

class Block;
class ReadBuffer;
class WriteBuffer;

/// Information for profiling. See IProfilingBlockInputStream.h
struct BlockStreamProfileInfo
{
    bool started = false;
    Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};    /// Time with waiting time

    String stream_name;            /// The short name of the stream for which information is collected

    size_t rows = 0;
    size_t blocks = 0;
    size_t bytes = 0;

    /// Information about nested threads - to calculate pure processing time.
    using BlockStreamProfileInfos = std::vector<const BlockStreamProfileInfo *>;
    BlockStreamProfileInfos nested_infos;

    /// Collect BlockStreamProfileInfo for the nearest sources in the tree named `name`. Example; collect all info for PartialSorting streams.
    void collectInfosForStreamsWithName(const char * name, BlockStreamProfileInfos & res) const;

    /** Get the number of rows if there were no LIMIT.
      * If there is no LIMIT, 0 is returned.
      * If the query does not contain ORDER BY, the number can be underestimated - return the number of rows in blocks that were read before LIMIT reached.
      * If the query contains an ORDER BY, then returns the exact number of rows as if LIMIT is removed from query.
      */
    size_t getRowsBeforeLimit() const;
    bool hasAppliedLimit() const;

    void update(Block & block);

    /// Binary serialization and deserialization of main fields.
    /// Writes only main fields i.e. fields that required by internal transmission protocol.
    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    /// Sets main fields from other object (see methods above).
    /// If skip_block_size_info if true, then rows, bytes and block fields are ignored.
    void setFrom(const BlockStreamProfileInfo & rhs, bool skip_block_size_info);

private:
    void calculateRowsBeforeLimit() const;

    /// For these fields we make accessors, because they must be calculated beforehand.
    mutable bool applied_limit = false;                    /// Whether LIMIT was applied
    mutable size_t rows_before_limit = 0;
    mutable bool calculated_rows_before_limit = false;    /// Whether the field rows_before_limit was calculated
};

}
