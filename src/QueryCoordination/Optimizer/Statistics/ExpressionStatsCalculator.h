#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/ActionNodeVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

class ExpressionNodeVisitor
    : public ActionNodeVisitor<ActionNodeStatistics, std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>>
{
public:
    using Base = ActionNodeVisitor<ActionNodeStatistics, std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>>;
    using VisitContext = std::unordered_map<const ActionsDAG::Node *, ActionNodeStatistics>;

    ExpressionNodeVisitor() : log(&Poco::Logger::get("ExpressionNodeVisitor")) { }

    ActionNodeStatistics visit(const ActionsDAG::Node * node, ContextType & context) override;

    ActionNodeStatistics visitChildren(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitDefault(const ActionsDAG::Node * node, ContextType & context) override;

    ActionNodeStatistics visitInput(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitColumn(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitAlias(const ActionsDAG::Node * node, ContextType & context) override;
    ActionNodeStatistics visitArrayJoin(const ActionsDAG::Node * node, ContextType & context) override;

    /// functions
    ActionNodeStatistics visitFunction(const ActionsDAG::Node * node, ContextType & context);

private:
    ActionNodeStatistics visitUnaryFunction(const ActionsDAG::Node * node, ContextType & context);
    ActionNodeStatistics visitBinaryFunction(const ActionsDAG::Node * node, ContextType & context);

    Poco::Logger * log;
};

class ExpressionStatsCalculator
{
public:
    static Statistics calculateStatistics(const ActionsDAGPtr & expression, const Statistics & input);
    static ActionNodeStatistics calculateStatistics(const ActionsDAG::Node * node, ExpressionNodeVisitor::ContextType & context);
};


}
