#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <vector>

namespace DB
{

class IDictionarySource;
using DictionarySourcePtr = std::unique_ptr<IDictionarySource>;

/** Data-provider interface for external dictionaries,
*    abstracts out the data source (file, MySQL, ClickHouse, external program, network request et cetera)
*    from the presentation and memory layout (the dictionary itself).
*/
class IDictionarySource
{
public:
    /// Returns an input stream with all the data available from this source.
    virtual BlockInputStreamPtr loadAll() = 0;

    /// Returns an input stream with updated data available from this source.
    virtual BlockInputStreamPtr loadUpdatedAll() = 0;

    /** Indicates whether this source supports "random access" loading of data
      *  loadId and loadIds can only be used if this function returns true.
      */
    virtual bool supportsSelectiveLoad() const = 0;

    /** Returns an input stream with the data for a collection of identifiers.
      * It must be guaranteed, that 'ids' array will live at least until all data will be read from returned stream.
      */
    virtual BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) = 0;

    /** Returns an input stream with the data for a collection of composite keys.
      * `requested_rows` contains indices of all rows containing unique keys.
      * It must be guaranteed, that 'requested_rows' array will live at least until all data will be read from returned stream.
      */
    virtual BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) = 0;

    /// indicates whether the source has been modified since last load* operation
    virtual bool isModified() const = 0;

    /// Returns true if update field is defined
    virtual bool hasUpdateField() const = 0;

    virtual DictionarySourcePtr clone() const = 0;

    /// returns an informal string describing the source
    virtual std::string toString() const = 0;

    virtual ~IDictionarySource() = default;
};

}
