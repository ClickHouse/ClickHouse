#include <Analyzer/TableExpressionModifiers.h>

#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

void TableExpressionModifiers::dump(WriteBuffer & buffer) const
{
    buffer << "final: " << has_final;

    if (sample_size_ratio)
        buffer << ", sample_size: " << ASTSampleRatio::toString(*sample_size_ratio);

    if (sample_offset_ratio)
        buffer << ", sample_offset: " << ASTSampleRatio::toString(*sample_offset_ratio);
}

void TableExpressionModifiers::updateTreeHash(SipHash & hash_state) const
{
    hash_state.update(has_final);
    hash_state.update(sample_size_ratio.has_value());
    hash_state.update(sample_offset_ratio.has_value());

    if (sample_size_ratio.has_value())
    {
        hash_state.update(sample_size_ratio->numerator);
        hash_state.update(sample_size_ratio->denominator);
    }

    if (sample_offset_ratio.has_value())
    {
        hash_state.update(sample_offset_ratio->numerator);
        hash_state.update(sample_offset_ratio->denominator);
    }
}

String TableExpressionModifiers::formatForErrorMessage() const
{
    WriteBufferFromOwnString buffer;
    if (has_final)
        buffer << "FINAL";

    if (sample_size_ratio)
    {
        if (has_final)
            buffer << ' ';
        buffer << "SAMPLE " << ASTSampleRatio::toString(*sample_size_ratio);
    }

    if (sample_offset_ratio)
    {
        if (has_final || sample_size_ratio)
            buffer << ' ';
        buffer << "OFFSET " << ASTSampleRatio::toString(*sample_offset_ratio);
    }

    return buffer.str();
}

}
