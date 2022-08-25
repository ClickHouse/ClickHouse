#pragma once

#include <Common/logger_useful.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Processors/Sources/ShellCommandSource.h>


namespace DB
{

/// Allows loading dictionaries from executable
class ExecutableDictionarySource final : public IDictionarySource
{
public:

    struct Configuration
    {
        std::string command;
        std::vector<std::string> command_arguments;
        std::string update_field;
        UInt64 update_lag;
        /// Implicit key means that the source script will return only values,
        /// and the correspondence to the requested keys is determined implicitly - by the order of rows in the result.
        bool implicit_key;
    };

    ExecutableDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        Block & sample_block_,
        std::shared_ptr<ShellCommandSourceCoordinator> coordinator_,
        ContextPtr context_);

    ExecutableDictionarySource(const ExecutableDictionarySource & other);
    ExecutableDictionarySource & operator=(const ExecutableDictionarySource &) = delete;

    QueryPipeline loadAll() override;

    /** The logic of this method is flawed, absolutely incorrect and ignorant.
      * It may lead to skipping some values due to clock sync or timezone changes.
      * The intended usage of "update_field" is totally different.
      */
    QueryPipeline loadUpdatedAll() override;

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

    QueryPipeline getStreamForBlock(const Block & block);

private:
    Poco::Logger * log;
    time_t update_time = 0;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    Block sample_block;
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator;
    ContextPtr context;
};

}
