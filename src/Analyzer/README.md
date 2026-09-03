Contains query analysis implementation for the ClickHouse SQL dialect.

The query analysis is performed on top of a query tree, which is an intermediate representation of `SELECT` queries.
`QueryTreeBuilder` recursively traverses query AST and creates corresponding query tree nodes.
At this stage, the query is in an unresolved state, and  all identifiers are represented by `IdentifierNode`.
Later, during analysis, all `IdentifierNodes` are replaced by `ColumnNode`, `TableNode`, or an aliased expression.
There should be no `IdentifierNode` in the resolved query tree.

This folder contains definitions for all Query Tree components and utility functions for them. 

The `Resolve/` folder contains functionality responsible for the identifier resolution and type inference.
`QueryAnalyzer` performs identifier resolution, constant folding and substitutes all the used in the query aliases and CTEs.
Some parts of the query may still be unresolved after that, e.g., some table functions arguments.

The `Passes/` folder contains implementations of query tree level optimizations.
All of them are implementations of the `IQueryTreePass` interface.
In most cases, they use `InDepthQueryTreeVisitorWithContext` to traverse the query and modify some query tree nodes.
Please note that `InDepthQueryTreeVisitor`, compared to `InDepthQueryTreeVisitorWithContext`, doesn't know about unresolved parts of the query tree and doesn't keep track of the correct subquery `Context`.
It traverses the whole subtree starting from the root node passed to it; thus, it should be used only when you are sure it's safe.

The `tests/` folder contains unit tests for some parts of the query tree infrastructure.
