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

    TableExpressionModifiers(bool has_final_,
        std::optional<Rational> sample_size_ratio_,
        std::optional<Rational> sample_offset_ratio_)
        : has_final(has_final_)
        , sample_size_ratio(sample_size_ratio_)
        , sample_offset_ratio(sample_offset_ratio_)
    {}

    bool hasFinal() const
    {
        return has_final;
    }

    bool hasSampleSizeRatio() const
    {
        return sample_size_ratio.has_value();
    }

    std::optional<Rational> getSampleSizeRatio() const
    {
        return sample_size_ratio;
    }

    bool hasSampleOffsetRatio() const
    {
        return sample_offset_ratio.has_value();
    }

    std::optional<Rational> getSampleOffsetRatio() const
    {
        return sample_offset_ratio;
    }

    void dump(WriteBuffer & buffer) const;

    void updateTreeHash(SipHash & hash_state) const;

private:
    bool has_final;
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
