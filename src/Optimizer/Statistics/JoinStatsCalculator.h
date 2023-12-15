#pragma once

#include <Core/Joins.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Optimizer/Statistics/Stats.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Common/logger_useful.h>


namespace DB
{

/**Calculate statistics for Join
 *
 * Variable declaration:
 *
 * A: Left table join on keys NDV (such as col1, col2 ...)
 * B: Right table join on keys NDV (such as col1, col2 ...)
 * C: NDV of left intersecting right
 *
 * col1: First join on key
 * col2: Second join on key
 *
 * D: Left table row count
 * E: Right table row count
 * F: Row count of C in left
 * G: Row count of C in right
 *
 * **********************************************
 *
 * Formulas:
 *
 * 1. NDV
 * A = 1 * col1_ndv * 0.8 * col2_ndv * ...
 * B = ...
 * C = min(A, B) * 0.5
 *
 * If tables has column whose statis is unknown
 * A = log2D
 * B = log2E
 *
 * 2. Row Count
 * F = (C / A ) * D
 * G = (C/ B ) * E
 *
 * 3. Inner Join
 * H: row count of inner join
 * H = F * G / C * 1.2
 * Constraint : H < F * G & H > C
 *
 * 4. Semi & Anti
 * Left Semi = F
 * Left Anti = D - F
 *
 * 5. Outer Join
 * Left outer = D - F + H
 * Right outer = E - G +H
 * Full outer = (D - F) + (E - G) + H
 *
 * 6. Any
 * Inner any = C
 * Left any = D
 * Right any = E
 * Full any : not implemented
 */
class JoinStatsCalculator
{
public:
    static Stats calculateStatistics(JoinStep & step, const Stats & left_input, const Stats & right_input);

private:
    struct Impl
    {
    public:
        explicit Impl(JoinStep & step_, const Stats & left_input_, const Stats & right_input_);
        Stats calculate();

    private:
        void calculateCrossJoin(Stats & statistics);
        /// asof join: not equal join
        void calculateAsofJoin(Stats & statistics);

        void calculateInnerJoin(Stats & statistics, JoinStrictness strictness);
        /// JoinStrictness: any, all
        void calculateCommonInnerJoin(Stats & statistics, bool is_any);
        /// All Join:
        ///   Variable: H
        ///   H = F * G / C * 1.2
        ///   Constraint : H < F * G & H > C
        /// Any Join:
        ///   equals C
        Float64 calculateRowCountForInnerJoin(bool is_any);
        void calculateColumnStatsForInnerJoin(Stats & statistics);

        void calculateOuterJoin(Stats & statistics, JoinStrictness strictness);

        /// JoinStrictness: any, all
        void calculateCommonOuterJoin(Stats & statistics, bool is_any);
        /// JoinStrictness: semi, anti
        void calculateFilterOuterJoin(Stats & statistics, bool is_semi);

        /// column statistics for outer join
        void calculateColumnStatsForOuterJoin(Stats & statistics, JoinKind type);
        /// column statistics for JoinStrictness: semi, anti
        void calculateColumnStatsForFilterJoin(Stats & statistics, bool is_semi);

        void calculateColumnStatsForIntersecting(Stats & statistics);
        void calculateColumnStatsForAnti(Stats & statistics, bool is_left);

        /// ndv for join on keys
        /// Variable: A
        Float64 leftDataSetNDV();
        /// Variable: B
        Float64 rightDataSetNDV();

        /// Variable: C
        /// Return the count of intersecting NDV of left and right table
        Float64 calculateIntersectingNDV();

        /// Variable: F, the intersecting row count of left table.
        /// Formula: F = (C / A ) * D
        Float64 calculateIntersectingRowCountForLeft(Float64 intersecting_ndv);

        /// Variable: G,  the intersecting row count of right table.
        /// Formula: G = (C / B) * E
        Float64 calculateIntersectingRowCountForRight(Float64 intersecting_ndv);

        /// Whether join on key has columns whose statistics is unknown
        bool hasUnknownStatsColumn();

        void removeNonOutputColumn(Stats & input);

        JoinStep & step;

        /// Inputs, should never changed
        const Stats & left_input;
        const Stats & right_input;

        /// Join on keys
        Names left_join_on_keys;
        Names right_join_on_keys;

        /// Step output columns which the result statistics
        /// should contain and only contain.
        Names output_columns;
    };
};

}
