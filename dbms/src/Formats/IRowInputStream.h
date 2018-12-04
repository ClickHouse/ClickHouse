#pragma once

#include <boost/noncopyable.hpp>
#include <memory>
#include <string>

#include <Columns/IColumn.h>


namespace DB
{

/// A way to set some extentions to read and return extra information too. IRowInputStream.extendedRead() output.
struct RowReadExtension
{
    /// IRowInputStream.extendedRead() output value.
    /// Contains one bit per column in resently read row. IRowInputStream could leave it empty, or partialy set.
    /// It should contain true for columns that actually read from the source and false for defaults.
    std::vector<UInt8> read_columns;
};

/** Interface of stream, that allows to read data by rows.
  */
class IRowInputStream : private boost::noncopyable
{
public:
    /** Read next row and append it to the columns.
      * If no more rows - return false.
      */
    virtual bool read(MutableColumns & columns, RowReadExtension & extra) = 0;

    virtual void readPrefix() {}                /// delimiter before begin of result
    virtual void readSuffix() {}                /// delimiter after end of result

    /// Skip data until next row.
    /// This is intended for text streams, that allow skipping of errors.
    /// By default - throws not implemented exception.
    virtual bool allowSyncAfterError() const { return false; }
    virtual void syncAfterError();

    /// In case of parse error, try to roll back and parse last one or two rows very carefully
    ///  and collect as much as possible diagnostic information about error.
    /// If not implemented, returns empty string.
    virtual std::string getDiagnosticInfo() { return {}; }

    virtual ~IRowInputStream() {}
};

using RowInputStreamPtr = std::shared_ptr<IRowInputStream>;

}
