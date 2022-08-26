#pragma once
#include <Processors/IProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>
#include <Common/WeakHash.h>

namespace DB
{

/// Add IColumn::Selector to chunk (see SelectorInfo.h).
/// Selector is filled by formula (WeakHash(key_columns) * num_outputs / MAX_INT).
class AddingSelectorTransform : public ISimpleTransform
{
public:
    AddingSelectorTransform(const Block & header, size_t num_outputs_, ColumnNumbers key_columns_);
    String getName() const override { return "AddingSelector"; }
    void transform(Chunk & input_chunk, Chunk & output_chunk) override;

private:
    size_t num_outputs;
    ColumnNumbers key_columns;

    WeakHash32 hash;
};

}
