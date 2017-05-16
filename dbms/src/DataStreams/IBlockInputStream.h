#pragma once

#include <vector>
#include <memory>
#include <functional>
#include <boost/noncopyable.hpp>
#include <Core/Block.h>
#include <Core/SortDescription.h>


namespace DB
{


class IBlockInputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

class TableStructureReadLock;

using TableStructureReadLockPtr = std::shared_ptr<TableStructureReadLock>;
using TableStructureReadLocks = std::vector<TableStructureReadLockPtr>;
using TableStructureReadLocksList = std::list<TableStructureReadLockPtr>;

struct Progress;

namespace ErrorCodes
{
    extern const int OUTPUT_IS_NOT_SORTED;
}


/** Callback to track the progress of the query.
  * Used in IProfilingBlockInputStream and Context.
  * The function takes the number of rows in the last block, the number of bytes in the last block.
  * Note that the callback can be called from different threads.
  */
using ProgressCallback = std::function<void(const Progress & progress)>;


/** The stream interface for reading data by blocks from the database.
  * Relational operations are supposed to be done also as implementations of this interface.
  */
class IBlockInputStream : private boost::noncopyable
{
public:
    IBlockInputStream() {}

    /** Read next block.
      * If there are no more blocks, return an empty block (for which operator `bool` returns false).
      */
    virtual Block read() = 0;

    /** Get information about the last block received.
      */
    virtual BlockExtraInfo getBlockExtraInfo() const
    {
        throw Exception("Method getBlockExtraInfo is not supported by the data stream " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Read something before starting all data or after the end of all data.
      * In the `readSuffix` function, you can implement a finalization that can lead to an exception.
      * readPrefix() must be called before the first call to read().
      * readSuffix() should be called after read() returns an empty block, or after a call to cancel(), but not during read() execution.
      */
    virtual void readPrefix() {}
    virtual void readSuffix() {}

    virtual ~IBlockInputStream() {}

    /** To output the data stream transformation tree (query execution plan).
      */
    virtual String getName() const = 0;

    /** The unique identifier of the pipeline part of the query execution.
      * Sources with the same identifier are considered identical
      *  (producing the same data), and can be replaced by one source
      *  if several queries are executed simultaneously.
      * If the source can not be glued together with any other - return the object's address as an identifier.
      */
    virtual String getID() const = 0;

    /// If this stream generates data in grouped by some keys, return true.
    virtual bool isGroupedOutput() const { return false; }
    /// If this stream generates data in order by some keys, return true.
    virtual bool isSortedOutput() const { return false; }
    /// In case of isGroupedOutput or isSortedOutput, return corresponding SortDescription
    virtual const SortDescription & getSortDescription() const { throw Exception("Output of " + getName() + " is not sorted", ErrorCodes::OUTPUT_IS_NOT_SORTED); }

    BlockInputStreams & getChildren() { return children; }

    void dumpTree(std::ostream & ostr, size_t indent = 0, size_t multiplier = 1);

    /// Get leaf sources (not including this one).
    BlockInputStreams getLeaves();

    /// Get the number of rows and bytes read in the leaf sources.
    void getLeafRowsBytes(size_t & rows, size_t & bytes);

    /** Check the depth of the pipeline.
      * If max_depth is specified and the `depth` is greater - throw an exception.
      */
    size_t checkDepth(size_t max_depth) const;

    /** Do not allow to change the table while the blocks stream is alive.
      */
    void addTableLock(const TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

protected:
    TableStructureReadLocks table_locks;

    BlockInputStreams children;

private:
    void getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr = nullptr);

    size_t checkDepthImpl(size_t max_depth, size_t level) const;

    /** Get text that identifies this source and the entire subtree.
      * Unlike getID - without taking into account the parameters.
      */
    String getTreeID() const;
};


}

