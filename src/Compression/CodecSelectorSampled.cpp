#include <Compression/CodecSelectorSampled.h>

namespace DB::AdaptiveCodec
{

CodecSelectorSampled::CodecSelectorSampled(Candidates all_)
    : all(all_)
{
}

CompressionStatePtr CodecSelectorSampled::createState() const { return nullptr; }

Candidates CodecSelectorSampled::select(CompressionState *, const Outcome & outcome) const
{
    return outcome.ran.none() ? all : Candidates{};
}

}
