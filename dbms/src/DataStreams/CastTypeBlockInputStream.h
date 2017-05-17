#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class IFunction;

/// Implicitly converts string and numeric values to Enum, numeric types to other numeric types.
class CastTypeBlockInputStream : public IProfilingBlockInputStream
{
public:
    CastTypeBlockInputStream(const Context & context_,
                             BlockInputStreamPtr input_,
                             const Block & ref_defenition_);

    String getName() const override;

    String getID() const override;

protected:
    Block readImpl() override;

private:
    void collectDifferent(const Block & src_block, const Block & ref_sample);

private:
    const Context & context;
    Block ref_defenition;

    bool initialized = false;

    struct CastElement
    {
        std::shared_ptr<IFunction> function;
        size_t tmp_col_offset;

        CastElement(std::shared_ptr<IFunction> && function_, size_t tmp_col_offset_);
    };

    /// Used to perform type conversions in src block
    std::map<size_t, CastElement> cast_description;
    Block tmp_conversion_block;
};

}
