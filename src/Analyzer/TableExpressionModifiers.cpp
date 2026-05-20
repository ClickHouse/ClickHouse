#include <Analyzer/TableExpressionModifiers.h>

#include <Common/SipHash.h>

#include <Core/Streaming/CursorTree.h>

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

    if (stream_settings)
    {
        buffer << ", stream";

        if (stream_settings->cursor)
            buffer << " cursor";

        if (stream_settings->watermark)
            buffer << " watermark";
    }
}

void TableExpressionModifiers::updateTreeHash(SipHash & hash_state) const
{
    hash_state.update(has_final);
    hash_state.update(sample_size_ratio.has_value());
    hash_state.update(sample_offset_ratio.has_value());
    hash_state.update(stream_settings.has_value());

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

    if (stream_settings.has_value())
    {
        if (stream_settings->cursor)
        {
            for (const auto & entry : cursorTreeToMap(stream_settings->cursor))
            {
                const auto & tuple = entry.safeGet<Tuple>();
                hash_state.update(tuple.at(0).safeGet<String>());
                hash_state.update(tuple.at(1).safeGet<Int64>());
            }
        }

        if (stream_settings->watermark)
        {
            hash_state.update(stream_settings->watermark->column);
            hash_state.update(stream_settings->watermark->expression->getTreeHash());
        }
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

    if (stream_settings)
    {
        if (has_final || sample_size_ratio || sample_offset_ratio)
            buffer << ' ';
        buffer << "STREAM";
        if (stream_settings->cursor)
            buffer << " CURSOR";
        if (stream_settings->watermark)
            buffer << " WATERMARK";
    }

    return buffer.str();
}

}
