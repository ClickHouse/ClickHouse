#pragma once

#include <Compression/ICodecSelector.h>

namespace DB::AdaptiveCodec
{

/// Runs every candidate on every block, so the smallest output always wins.
class CodecSelectorSampled final : public ICodecSelector
{
public:
    explicit CodecSelectorSampled(Candidates all_);

    CompressionStatePtr createState() const override;
    Candidates select(CompressionState * state, const Outcome & outcome) const override;

private:
    const Candidates all;
};

}
