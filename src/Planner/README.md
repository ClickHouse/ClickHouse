Contains query planning functionality on top of Query Tree, see src/Analyzer/README.md.

This component is responsible for planning every query. The planning that preceded it, in
src/Interpreters/ExpressionAnalyzer.h and src/Interpreters/InterpreterSelectQuery.h, was chosen by the `enable_analyzer`
setting until that setting became obsolete in 26.9, and was removed in 26.10; what is left of those two is the machinery
that compiles a standalone expression - a `TTL` expression, a sharding key, a column default - outside query planning.

The `PlannerActionsVisitor` builds `ActionsDAG` from an expression represented as a query tree.
It also creates unique execution names for all nodes in the `ActionsDAG`, i.e., constants.
It is responsible for creating the same execution names for expression on initiator and follower nodes during distributed query execution.

The `PlannerExpressionAnalysis.h` contains the `buildExpressionAnalysisResult` function to calculate information about the stream header after every query step.

`PlannerContext` contains the proper query `Context` and is responsible for creating unique execution names for `ColumnNode`.
`TableExpressionData` for table expression nodes of the corresponding query must be registered in the `PlannerContext`.

Other files contain the implementation of query planning for different parts of the query.
