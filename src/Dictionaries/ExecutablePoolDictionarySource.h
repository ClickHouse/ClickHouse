#pragma once

#include <Core/Block.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Context.h>

#include "IDictionarySource.h"
#include "DictionaryStructure.h"

namespace Poco { class Logger; }


namespace DB
{

using ProcessPool = ConcurrentBoundedQueue<std::unique_ptr<ShellCommand>>;

/// Allows loading data from pool of processes
/// TODO: Add documentation
class ExecutablePoolDictionarySource final : public IDictionarySource
{
public:
    ExecutablePoolDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block_,
        const Context & context_);

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
    bool implicit_key;
    const std::string command;
    const std::string update_field;
    const std::string format;
    const size_t pool_size;

    Block sample_block;
    Context context;
    std::shared_ptr<ProcessPool> process_pool;
};

}
