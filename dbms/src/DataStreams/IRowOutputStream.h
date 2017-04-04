#pragma once

#include <memory>
#include <cstdint>
#include <boost/noncopyable.hpp>
#include <Core/Types.h>


namespace DB
{

class Block;
class IColumn;
class IDataType;
struct Progress;


/** Interface of stream for writing data by rows (for example: for output to terminal).
  */
class IRowOutputStream : private boost::noncopyable
{
public:

    /** Write a row.
      * Default implementation calls methods to write single values and delimiters
      * (except delimiter between rows (writeRowBetweenDelimiter())).
      */
    virtual void write(const Block & block, size_t row_num);

    /** Write single value. */
    virtual void writeField(const IColumn & column, const IDataType & type, size_t row_num) = 0;

    /** Write delimiter. */
    virtual void writeFieldDelimiter() {};        /// delimiter between values
    virtual void writeRowStartDelimiter() {};    /// delimiter before each row
    virtual void writeRowEndDelimiter() {};        /// delimiter after each row
    virtual void writeRowBetweenDelimiter() {};    /// delimiter between rows
    virtual void writePrefix() {};                /// delimiter before resultset
    virtual void writeSuffix() {};                /// delimiter after resultset

    /** Flush output buffers if any. */
    virtual void flush() {}

    /** Methods to set additional information for output in formats, that support it.
      */
    virtual void setRowsBeforeLimit(size_t rows_before_limit) {}
    virtual void setTotals(const Block & totals) {}
    virtual void setExtremes(const Block & extremes) {}

    /** Notify about progress. Method could be called from different threads.
      * Passed value are delta, that must be summarized.
      */
    virtual void onProgress(const Progress & progress) {}

    /** Content-Type to set when sending HTTP response. */
    virtual String getContentType() const { return "text/plain; charset=UTF-8"; }

    virtual ~IRowOutputStream() {}
};

using RowOutputStreamPtr = std::shared_ptr<IRowOutputStream>;

}
