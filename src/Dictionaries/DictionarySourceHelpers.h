#pragma once

#include <vector>

#include <common/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DataStreams/IBlockInputStream.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

struct DictionaryStructure;

/// Write keys to block output stream.

void formatBlock(BlockOutputStreamPtr & out, const Block & block);

/// For simple key

Block blockForIds(
    const DictionaryStructure & dict_struct,
    const std::vector<UInt64> & ids);

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct,
    const Columns & key_columns,
    const std::vector<size_t> & requested_rows);

/// Used for applying settings to copied context in some register[...]Source functions
ContextMutablePtr copyContextAndApplySettings(
    const std::string & config_prefix,
    ContextPtr context,
    const Poco::Util::AbstractConfiguration & config);

/** A stream, adds additional columns to each block that it will read from inner stream.
     *
     *  block_to_add rows size must be equal to final sum rows size of all inner stream blocks.
     */
class BlockInputStreamWithAdditionalColumns final : public IBlockInputStream
{
public:
    BlockInputStreamWithAdditionalColumns(Block block_to_add_, std::unique_ptr<IBlockInputStream> && stream_);

    Block getHeader() const override;

    Block readImpl() override;

    void readPrefix() override;

    void readSuffix() override;

    String getName() const override;

private:
    Block block_to_add;
    std::unique_ptr<IBlockInputStream> stream;
    size_t current_range_index = 0;
};

}
