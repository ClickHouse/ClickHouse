#pragma once

#include <Parsers/ASTSampleRatio.h>

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
        std::optional<Rational> sample_offset_ratio_)
        : has_final(has_final_)
        , sample_size_ratio(sample_size_ratio_)
        , sample_offset_ratio(sample_offset_ratio_)
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
};

inline bool operator==(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    return lhs.hasFinal() == rhs.hasFinal() && lhs.getSampleSizeRatio() == rhs.getSampleSizeRatio() && lhs.getSampleOffsetRatio() == rhs.getSampleOffsetRatio();
}

inline bool operator!=(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    return !(lhs == rhs);
}

}
