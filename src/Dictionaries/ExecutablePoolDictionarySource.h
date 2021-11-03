#pragma once

#include <base/logger_useful.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{


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
        String command;
        String format;
        size_t pool_size;
        size_t command_termination_timeout;
        size_t max_command_execution_time;
        /// Implicit key means that the source script will return only values,
        /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
        bool implicit_key;
        /// Send number_of_rows\n before sending chunk to process
        bool send_chunk_header;
    };

    ExecutablePoolDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        Block & sample_block_,
        ContextPtr context_);

    ExecutablePoolDictionarySource(const ExecutablePoolDictionarySource & other);
    ExecutablePoolDictionarySource & operator=(const ExecutablePoolDictionarySource &) = delete;

    Pipe loadAll() override;

    /** The logic of this method is flawed, absolutely incorrect and ignorant.
      * It may lead to skipping some values due to clock sync or timezone changes.
      * The intended usage of "update_field" is totally different.
      */
    Pipe loadUpdatedAll() override;

    Pipe loadIds(const std::vector<UInt64> & ids) override;

    Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

    Pipe getStreamForBlock(const Block & block);

private:
    const DictionaryStructure dict_struct;
    const Configuration configuration;

    Block sample_block;
    ContextPtr context;
    std::shared_ptr<ProcessPool> process_pool;
    Poco::Logger * log;
};

}
