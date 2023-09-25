#pragma once

#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>


namespace DB
{

class JoinStatsCalculator
{
private:
    struct Calculator
    {
    public:
        explicit Calculator(JoinStep & step_, const Statistics & left_input_, const Statistics & right_input_)
            : step(step_), left_input(left_input_), right_input(right_input_), log(&Poco::Logger::get("JoinStatsCalculator"))
        {
        }

        Statistics calculate();

    private:
        void calculateCrossJoin(Statistics & input);
        void calculateInnerJoin(Statistics & input);

        void calculateOuterJoin(Statistics & input);
        void calculateFilterJoin(Statistics & input);

        void calculateAnyJoin(Statistics & input);

        /// whether join on key has columns whose statistics is unknown
        bool hasUnknownStatsColumn(const std::vector<TableJoin::JoinOnClause> on_clauses);

        void removeNonOutputColumn(Statistics & input);

        JoinStep & step;

        const Statistics & left_input;
        const Statistics & right_input;

        Poco::Logger * log;
    };

public:
    static Statistics calculateStatistics(JoinStep & step, const Statistics & left_input, const Statistics & right_input);
};

}

