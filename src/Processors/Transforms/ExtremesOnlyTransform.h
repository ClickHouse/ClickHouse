#pragma once
#include <Columns/IColumn_fwd.h>
#include <Processors/IAccumulatingTransform.h>

namespace DB
{

/// Consumes all rows and produces only their extremes on its single output. Unlike
/// `ExtremesTransform` it has no main output, so it needs no sink to drain one and can never be
/// a childless node.
class ExtremesOnlyTransform final : public IAccumulatingTransform
{
public:
    explicit ExtremesOnlyTransform(SharedHeader header);

    String getName() const override { return "ExtremesOnlyTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    MutableColumns extremes_columns;
};

}
