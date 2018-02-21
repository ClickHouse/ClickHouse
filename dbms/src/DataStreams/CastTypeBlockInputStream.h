#pragma once

#include <unordered_map>
#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/// Implicitly converts types.
class CastTypeBlockInputStream : public IProfilingBlockInputStream
{
public:
    CastTypeBlockInputStream(const Context & context,
                             const BlockInputStreamPtr & input,
                             const Block & reference_definition);

    String getName() const override;

    Block getHeader() const override { return header; }

private:
    Block readImpl() override;

    const Context & context;
    Block header;

    /// Describes required conversions on source block
    /// Contains column numbers in source block that should be converted
    std::unordered_map<size_t, DataTypePtr> cast_description;
};

}
