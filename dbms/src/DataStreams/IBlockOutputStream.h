#pragma once

#include <string>
#include <vector>
#include <memory>
#include <boost/noncopyable.hpp>
#include <Core/Block.h>
#include <Storages/TableStructureLockHolder.h>


namespace DB
{

struct Progress;

/** Interface of stream for writing data (into table, filesystem, network, terminal, etc.)
  */
class IBlockOutputStream : private boost::noncopyable
{
public:
    IBlockOutputStream() {}

    /** Get data structure of the stream in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      * You must pass blocks of exactly this structure to the 'write' method.
      */
    virtual Block getHeader() const = 0;

    /** Write block.
      */
    virtual void write(const Block & block) = 0;

    /** Write or do something before all data or after all data.
      */
    virtual void writePrefix() {}
    virtual void writeSuffix() {}

    /** Flush output buffers if any.
      */
    virtual void flush() {}

    /** Methods to set additional information for output in formats, that support it.
      */
    virtual void setRowsBeforeLimit(size_t /*rows_before_limit*/) {}
    virtual void setTotals(const Block & /*totals*/) {}
    virtual void setExtremes(const Block & /*extremes*/) {}

    /** Notify about progress. Method could be called from different threads.
      * Passed value are delta, that must be summarized.
      */
    virtual void onProgress(const Progress & /*progress*/) {}

    /** Content-Type to set when sending HTTP response.
      */
    virtual std::string getContentType() const { return "text/plain; charset=UTF-8"; }

    virtual ~IBlockOutputStream() {}

    /** Don't let to alter table while instance of stream is alive.
      */
    void addTableLock(const TableStructureReadLockHolder & lock) { table_locks.push_back(lock); }

private:
    std::vector<TableStructureReadLockHolder> table_locks;
};

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

}
