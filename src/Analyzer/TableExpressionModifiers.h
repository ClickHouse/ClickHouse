#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/IAST.h>

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
        ASTPtr final_by_ = nullptr)
        : has_final(has_final_)
        , sample_size_ratio(sample_size_ratio_)
        , sample_offset_ratio(sample_offset_ratio_)
        , final_by(std::move(final_by_))
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

    /// Returns true if FINAL BY expression list is specified
    bool hasFinalBy() const
    {
        return final_by != nullptr;
    }

    /// Get FINAL BY expression list (may be nullptr)
    const ASTPtr & getFinalBy() const
    {
        return final_by;
    }

    /// Returns true if FINAL BY has been resolved through the new analyzer
    bool hasResolvedFinalBy() const
    {
        return !resolved_final_by.empty();
    }

    /// Get resolved FINAL BY expression nodes (populated by QueryAnalyzer)
    const QueryTreeNodes & getResolvedFinalBy() const
    {
        return resolved_final_by;
    }

    /// Set resolved FINAL BY expression nodes
    void setResolvedFinalBy(QueryTreeNodes nodes)
    {
        resolved_final_by = std::move(nodes);
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
    ASTPtr final_by;
    /// Resolved FINAL BY query tree nodes (populated by QueryAnalyzer, used by Planner)
    QueryTreeNodes resolved_final_by;
};

inline bool operator==(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    if (lhs.hasFinal() != rhs.hasFinal()
        || lhs.getSampleSizeRatio() != rhs.getSampleSizeRatio()
        || lhs.getSampleOffsetRatio() != rhs.getSampleOffsetRatio()
        || lhs.hasFinalBy() != rhs.hasFinalBy())
        return false;

    if (lhs.hasFinalBy() && lhs.getFinalBy()->getTreeHash(/*ignore_aliases=*/true) != rhs.getFinalBy()->getTreeHash(/*ignore_aliases=*/true))
        return false;

    return true;
}

inline bool operator!=(const TableExpressionModifiers & lhs, const TableExpressionModifiers & rhs)
{
    return !(lhs == rhs);
}

}
