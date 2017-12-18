#pragma once

#include <unordered_map>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class IFunction;

/// Implicitly converts string and numeric values to Enum, numeric types to other numeric types.
class CastTypeBlockInputStream : public IProfilingBlockInputStream
{
public:
    CastTypeBlockInputStream(const Context & context,
                             const BlockInputStreamPtr & input,
                             const Block & reference_definition);

    String getName() const override;

    String getID() const override;

protected:
    Block readImpl() override;

private:
    const Context & context;
    Block ref_definition;

    /// Initializes cast_description and prepares tmp_conversion_block
    void initialize(const Block & src_block);
    bool initialized = false;

    /// Describes required conversions on source block
    /// Contains column numbers in source block that should be converted
    std::unordered_map<size_t, DataTypePtr> cast_description;
};

}
