#pragma once

#include <vector>

#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Processors/ISimpleTransform.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

struct DictionaryStructure;
class SettingsChanges;

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
SettingsChanges readSettingsFromDictionaryConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
ContextMutablePtr copyContextAndApplySettingsFromDictionaryConfig(const ContextPtr & context, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

/** A stream, adds additional columns to each block that it will read from inner stream.
     *
     *  block_to_add rows size must be equal to final sum rows size of all inner stream blocks.
     */
class TransformWithAdditionalColumns final : public ISimpleTransform
{
public:
    TransformWithAdditionalColumns(Block block_to_add_, const Block & header);

    void transform(Chunk & chunk) override;

    String getName() const override;

private:
    Block block_to_add;
    size_t current_range_index = 0;
};

}
