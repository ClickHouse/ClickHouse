#pragma once

#include <common/BorrowedObjectPool.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>

#include "IDictionarySource.h"
#include "DictionaryStructure.h"

namespace Poco { class Logger; }


namespace DB
{

using ProcessPool = BorrowedObjectPool<std::unique_ptr<ShellCommand>>;

/** ExecutablePoolDictionarySource allows loading data from pool of processes.
  * When client requests ids or keys source get process from ProcessPool
  * and create stream based on source format from process stdout.
  * It is important that stream format will expect only rows that were requested.
  * When stream is finished process is returned back to the ProcessPool.
  * If there are no processes in pool during request client will be blocked
  * until some process will be returned to pool.
  */
class ExecutablePoolDictionarySource final : public IDictionarySource
{
public:
    struct Configuration
    {
        const String command;
        const String format;
        const size_t pool_size;
        const size_t command_termination_timeout;
        const size_t max_command_execution_time;
        /// Implicit key means that the source script will return only values,
        /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
        const bool implicit_key;
    };

    ExecutablePoolDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        Block & sample_block_,
        ContextPtr context_);

    ExecutablePoolDictionarySource(const ExecutablePoolDictionarySource & other);
    ExecutablePoolDictionarySource & operator=(const ExecutablePoolDictionarySource &) = delete;

    BlockInputStreamPtr loadAll() override;

    /** The logic of this method is flawed, absolutely incorrect and ignorant.
      * It may lead to skipping some values due to clock sync or timezone changes.
      * The intended usage of "update_field" is totally different.
      */
    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

    BlockInputStreamPtr getStreamForBlock(const Block & block);

private:
    Poco::Logger * log;
    time_t update_time = 0;
    const DictionaryStructure dict_struct;
    const Configuration configuration;

    Block sample_block;
    ContextPtr context;
    std::shared_ptr<ProcessPool> process_pool;
};

}
