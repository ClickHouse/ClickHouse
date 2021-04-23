#pragma once

#include <vector>

#include <common/types.h>

#include <Poco/File.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>

namespace DB
{
class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

struct DictionaryStructure;
class Context;

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
Context copyContextAndApplySettings(
    const std::string & config_prefix,
    const Context & context,
    const Poco::Util::AbstractConfiguration & config);

void applySettingsToContext(
    const std::string & config_prefix,
    Context & context,
    const Poco::Util::AbstractConfiguration & config);

}
