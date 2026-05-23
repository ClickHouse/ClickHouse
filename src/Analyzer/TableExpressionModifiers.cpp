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

    if (final_by)
        buffer << ", final_by: " << final_by->formatForErrorMessage();

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

    if (final_by)
    {
        auto tree_hash = final_by->getTreeHash(/*ignore_aliases=*/true);
        hash_state.update(tree_hash.low64);
        hash_state.update(tree_hash.high64);
    }
}

String TableExpressionModifiers::formatForErrorMessage() const
{
    WriteBufferFromOwnString buffer;
    if (has_final)
        buffer << "FINAL";

    if (final_by)
    {
        chassert(has_final);
        buffer << " BY " << final_by->formatForErrorMessage();
    }

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
