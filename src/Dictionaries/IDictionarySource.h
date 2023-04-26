#pragma once

#include <Columns/IColumn.h>
#include <QueryPipeline/QueryPipeline.h>

#include <vector>
#include <atomic>


namespace DB
{
class IDictionarySource;
using DictionarySourcePtr = std::shared_ptr<IDictionarySource>;

/** Data-provider interface for external dictionaries,
*    abstracts out the data source (file, MySQL, ClickHouse, external program, network request et cetera)
*    from the presentation and memory layout (the dictionary itself).
*/
class IDictionarySource
{
public:

    /// Returns a pipe with all the data available from this source.
    virtual QueryPipeline loadAll() = 0;

    /// Returns a pipe with updated data available from this source.
    virtual QueryPipeline loadUpdatedAll() = 0;

    /**
     * result_size_hint - approx number of rows in the stream.
     * Returns a pipe with all the data available from this source.
     *
     * NOTE: result_size_hint may be changed during you are reading (usually it
     * will be non zero for the first block and zero for others, since it uses
     * Progress::total_rows_approx,) from the pipe, and may be called
     * in parallel, so you should use something like this:
     *
     *   ...
     *   std::atomic<uint64_t> new_size = 0;
     *
     *   QueryPipeline pipeline;
     *   pipeline.init(source->loadAll(&new_size));
     *   PullingPipelineExecutor executor;
     *
     *   Block block;
     *   while (executor.pull(block))
     *   {
     *       if (new_size)
     *       {
     *           size_t current_new_size = new_size.exchange(0);
     *           if (current_new_size)
     *               resize(current_new_size);
     *       }
     *       else
     *       {
     *           resize(block.rows());
     *       }
     *   }
     *
     *   ...
     */
    virtual QueryPipeline loadAllWithSizeHint(std::atomic<size_t> * /* result_size_hint */)
    {
        return loadAll();
    }

    /** Indicates whether this source supports "random access" loading of data
      *  loadId and loadIds can only be used if this function returns true.
      */
    virtual bool supportsSelectiveLoad() const = 0;

    /** Returns an input stream with the data for a collection of identifiers.
      * It must be guaranteed, that 'ids' array will live at least until all data will be read from returned stream.
      */
    virtual QueryPipeline loadIds(const std::vector<UInt64> & ids) = 0;

    /** Returns an input stream with the data for a collection of composite keys.
      * `requested_rows` contains indices of all rows containing unique keys.
      * It must be guaranteed, that 'requested_rows' array will live at least until all data will be read from returned stream.
      */
    virtual QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) = 0;

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
