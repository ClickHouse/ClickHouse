#pragma once

#include <Core/Joins.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Optimizer/Statistics/Statistics.h>
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
    static Statistics calculateStatistics(JoinStep & step, const Statistics & left_input, const Statistics & right_input);

private:
    struct Impl
    {
    public:
        explicit Impl(JoinStep & step_, const Statistics & left_input_, const Statistics & right_input_);
        Statistics calculate();

    private:
        void calculateCrossJoin(Statistics & statistics);
        /// asof join: not equal join
        void calculateAsofJoin(Statistics & statistics);

        void calculateInnerJoin(Statistics & statistics, JoinStrictness strictness);
        /// JoinStrictness: any, all
        void calculateCommonInnerJoin(Statistics & statistics, bool is_any);
        /// All Join:
        ///   Variable: H
        ///   H = F * G / C * 1.2
        ///   Constraint : H < F * G & H > C
        /// Any Join:
        ///   equals C
        Float64 calculateRowCountForInnerJoin(bool is_any);
        void calculateColumnStatsForInnerJoin(Statistics & statistics);

        void calculateOuterJoin(Statistics & statistics, JoinStrictness strictness);

        /// JoinStrictness: any, all
        void calculateCommonOuterJoin(Statistics & statistics, bool is_any);
        /// JoinStrictness: semi, anti
        void calculateFilterOuterJoin(Statistics & statistics, bool is_semi);

        /// column statistics for outer join
        void calculateColumnStatsForOuterJoin(Statistics & statistics, JoinKind type);
        /// column statistics for JoinStrictness: semi, anti
        void calculateColumnStatsForFilterJoin(Statistics & statistics, bool is_semi);

        void calculateColumnStatsForIntersecting(Statistics & statistics);
        void calculateColumnStatsForAnti(Statistics & statistics, bool is_left);

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

        void removeNonOutputColumn(Statistics & input);

        JoinStep & step;

        /// Inputs, should never changed
        const Statistics & left_input;
        const Statistics & right_input;

        /// Join on keys
        Names left_join_on_keys;
        Names right_join_on_keys;

        /// Step output columns which the result statistics
        /// should contain and only contain.
        Names output_columns;
    };
};

}
