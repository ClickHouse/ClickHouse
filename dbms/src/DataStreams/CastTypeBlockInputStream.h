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

    /// Initializes cast_description and prepares tmp_conversion_block
    void initialize(const Block & src_block);
    bool initialized = false;

    struct CastElement
    {
        /// Prepared function to do conversion
        std::shared_ptr<IFunction> function;
        /// Position of first function argument in tmp_conversion_block
        size_t tmp_col_offset;

        CastElement(std::shared_ptr<IFunction> && function_, size_t tmp_col_offset_);
    };

    /// Describes required conversions on source block
    /// Contains column numbers in source block that should be converted
    std::map<size_t, CastElement> cast_description;

    /// Auxiliary block, stores prefilled arguments and result for each CAST function in cast_description
    /// 3 columns are allocated for each conversion: [blank of source column, column with res type name, blank of res column]
    Block tmp_conversion_block;
};

}
