#pragma once

#include <Parsers/ASTSampleRatio.h>

#include <Analyzer/IQueryTreeNode.h>

#include <Core/Streaming/CursorTree_fwd.h>
#include <Core/Streaming/Settings.h>

namespace DB
{

/** Modifiers that can be used for table, table function and subquery in JOIN TREE.
  *
  * Example: SELECT * FROM test_table SAMPLE 0.1 OFFSET 0.1 FINAL
  */
class TableExpressionModifiers
{
public:
    using Rational = ASTSampleRatio::Rational;

    TableExpressionModifiers() = default;
    TableExpressionModifiers(bool has_final_,
        std::optional<Rational> sample_size_ratio_,
        std::optional<Rational> sample_offset_ratio_,
        std::optional<StreamingSettings> stream_settings_ = {})
        : has_final(has_final_)
        , sample_size_ratio(sample_size_ratio_)
        , sample_offset_ratio(sample_offset_ratio_)
        , stream_settings(std::move(stream_settings_))
    {}

    /// Returns true if final is specified, false otherwise
    bool hasFinal() const
    {
        return has_final;
    }

    /// Set has final value
    void setHasFinal(bool value)
    {
        has_final = value;
    }

    /// Returns true if sample size ratio is specified, false otherwise
    bool hasSampleSizeRatio() const
    {
        return sample_size_ratio.has_value();
    }

    /// Get sample size ratio
    std::optional<Rational> getSampleSizeRatio() const
    {
        return sample_size_ratio;
    }

    /// Returns true if sample offset ratio is specified, false otherwise
    bool hasSampleOffsetRatio() const
    {
        return sample_offset_ratio.has_value();
    }

    /// Get sample offset ratio
    std::optional<Rational> getSampleOffsetRatio() const
    {
        return sample_offset_ratio;
    }

    /// Returns true if STREAM modifier is specified
    bool hasStream() const
    {
        return stream_settings.has_value();
    }

    /// Get stream settings
    const std::optional<StreamingSettings> & getStreamingSettings() const
    {
        return stream_settings;
    }

    /// Dump into buffer
    void dump(WriteBuffer & buffer) const;

    /// Update tree hash
    void updateTreeHash(SipHash & hash_state) const;

    /// Format for error message
    String formatForErrorMessage() const;

private:
    bool has_final = false;
    std::optional<Rational> sample_size_ratio;
    std::optional<Rational> sample_offset_ratio;
    std::optional<StreamingSettings> stream_settings;
};

inline bool operator==(const WatermarkSettings & lhs, const WatermarkSettings & rhs)
{
    if (lhs.column != rhs.column)
        return false;

    if ((lhs.expression == nullptr) != (rhs.expression == nullptr))
        return false;

    return !lhs.expression || lhs.expression->isEqual(*rhs.expression);
}

inline bool operator==(const StreamingSettings & lhs, const StreamingSettings & rhs)
{
    /// Compare cursors
    {
        if ((lhs.cursor == nullptr) != (rhs.cursor == nullptr))
            return false;

        if (lhs.cursor)
            if (cursorTreeToMap(lhs.cursor) != cursorTreeToMap(rhs.cursor))
                return false;
    }

    /// Compare watermarks
    {
        if ((lhs.watermark == nullptr) != (rhs.watermark == nullptr))
            return false;

        if (lhs.watermark)
            if (*lhs.watermark != *rhs.watermark)
                return false;
    }

    return true;
}

inline bool operator==(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    return lhs.hasFinal() == rhs.hasFinal()
        && lhs.getSampleSizeRatio() == rhs.getSampleSizeRatio()
        && lhs.getSampleOffsetRatio() == rhs.getSampleOffsetRatio()
        && lhs.getStreamingSettings() == rhs.getStreamingSettings();
}

inline bool operator!=(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    return !(lhs == rhs);
}

}
