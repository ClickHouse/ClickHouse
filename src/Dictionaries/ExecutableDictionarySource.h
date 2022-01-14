#pragma once

#include "DictionaryStructure.h"
#include "IDictionarySource.h"
#include <Core/Block.h>
#include <Interpreters/Context.h>

namespace Poco { class Logger; }


namespace DB
{

/// Allows loading dictionaries from executable
class ExecutableDictionarySource final : public IDictionarySource
{
public:

    struct Configuration
    {
        const std::string command;
        const std::string format;
        const std::string update_field;
        const UInt64 update_lag;
        /// Implicit key means that the source script will return only values,
        /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
        const bool implicit_key;
    };

    ExecutableDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        Block & sample_block_,
        ContextPtr context_);

    ExecutableDictionarySource(const ExecutableDictionarySource & other);
    ExecutableDictionarySource & operator=(const ExecutableDictionarySource &) = delete;

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
};

}
