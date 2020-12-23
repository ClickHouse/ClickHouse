What is AST?
===
AST stands for Abstract Syntax Tree, which is opposed to Concrete Syntax Tree (or Parse Tree). Read [this](https://eli.thegreenplace.net/2009/02/16/abstract-vs-concrete-syntax-trees/) post to get a sketchy overview of the difference between two concepts.

AST **must not** repeat the grammar constructions or follow them. It's convenient to have similar structure but nothing more.
The main purpose of AST is to be easily handled by interpreter - the formatting of the original query is not the purpose of AST.

Basic principles in code
===

- The base class for all AST elements is `INode` (INode.h).
- **All** sub-elements must be stored inside `INode::children` vector in a
**predetermined order** and with **predetermined type**: some elements may be `nullptr` to preserve positions of other elements.
- The order may be defined as a position in vector from the start, the last element, and some pattern of variable number of elements
in between. It's convenient to define `enum ChildIndex : Uint8 {…}` with index numbers for each class.
- If there is more than one variable pack of elements or the order can't be determenistic, then wrap elements into the lists and store the
multi-level structure (see `ColumnExpr::ExprType::FUNCTION` for example).
- Don't do multi-level structure just for nothing or to mimic the parse tree: the less is depth the better.
- The whole grammar separates expressions for databases, tables and columns. That way we already assess the semantics on the parser level.
E.g. don't use `identifier` where you know you should use `tableIdentifier`, etc.

Name conventions
===

**Query**. The top-level element that allows to distinguish different types of SQL queries. The base class is `Query` (Query.h).

**Statement**. An essential part of a query that describes its structure and possible alternatives.

**Clause**. A part of the statement designed to differ logical parts for more convenient parsing. I.e. there are many clauses in SELECT statement that are optional and contain `columnExpr` elements. Without clauses it will be hard for visitor to distinguish which `columnExpr` refers to what.

**Expression**. An element that should be somehow calculated or interpreted and result in some value.
**
