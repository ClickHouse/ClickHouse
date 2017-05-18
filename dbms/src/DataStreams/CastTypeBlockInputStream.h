#pragma once

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
    Block ref_defenition;

    void initialize(const Block & src_block);
    bool initialized = false;

    struct CastElement
    {
        std::shared_ptr<IFunction> function;
        size_t tmp_col_offset;

        CastElement(std::shared_ptr<IFunction> && function_, size_t tmp_col_offset_);
    };

    /// Describes required conversions on source block
    std::map<size_t, CastElement> cast_description;
    /// Auxiliary block, stores arguments and results of required CAST calls
    Block tmp_conversion_block;
};

}
