#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

struct DictionaryStructure;
class SettingsChanges;

/// For simple key

Block blockForIds(const DictionaryStructure & dict_struct, const VectorWithMemoryTracking<UInt64> & ids);

/// For composite key

Block blockForKeys(
    const DictionaryStructure & dict_struct, const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows);

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
    TransformWithAdditionalColumns(SharedHeader block_to_add_, SharedHeader header);

    void transform(Chunk & chunk) override;

    String getName() const override;

private:
    SharedHeader block_to_add;
    size_t current_range_index = 0;
};

}
