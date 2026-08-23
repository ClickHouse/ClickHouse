#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Converts columns-constants to full columns ("materializes" them).
class MaterializingTransform final : public ISimpleTransform
{
public:
    explicit MaterializingTransform(SharedHeader header, bool remove_special_representations_ = true);

    String getName() const override { return "MaterializingTransform"; }

    /// Stateless with respect to preview chunks (see `QueryResultPreview.h`).
    bool supportsQueryResultPreviews() const override { return true; }

protected:
    void transform(Chunk & chunk) override;
    bool remove_special_representations;
};

}
