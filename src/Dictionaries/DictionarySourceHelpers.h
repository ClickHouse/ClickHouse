#pragma once

#include <vector>
#include <Columns/IColumn.h>
#include <common/types.h>
#include <Poco/File.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

struct DictionaryStructure;
class Context;

/// Write keys to block output stream.

/// For simple key
void formatIDs(BlockOutputStreamPtr & out, const std::vector<UInt64> & ids);

/// For composite key
void formatKeys(
    const DictionaryStructure & dict_struct,
    BlockOutputStreamPtr & out,
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
