#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Extracts required columns and subcolumns from the block.
class ExtractColumnsTransform final : public ISimpleTransform
{
public:
    ExtractColumnsTransform(
        SharedHeader header_,
        const NamesAndTypesList & requested_columns_);

    String getName() const override { return "ExtractColumnsTransform"; }

    static Block transformHeader(Block header, const NamesAndTypesList & requested_columns_);

protected:
    void transform(Chunk & chunk) override;

private:
    const NamesAndTypesList requested_columns;
};

}
