---
toc_priority: 40
toc_title: ANSI Compatibility
---

# ANSI SQL Compatibility of ClickHouse SQL Dialect {#ansi-sql-compatibility-of-clickhouse-sql-dialect}

!!! note "Note"
    This article relies on Table 38, “Feature taxonomy and definition for mandatory features”, Annex F of ISO/IEC CD 9075-2:2013.

## Differences in Behaviour {#differences-in-behaviour}

The following table lists cases when query feature works in ClickHouse, but behaves not as specified in ANSI SQL.

| Feature ID | Feature Name                | Difference                                                                                                |
|------------|-----------------------------|-----------------------------------------------------------------------------------------------------------|
| E051-05    | Select items can be renamed | Item renames have a wider visibility scope than just the SELECT result                                    |
| E141-01    | NOT NULL constraints        | Yes                                                                                                       |
| E011-04    | Arithmetic operators        | ClickHouse overflows instead of checked arithmetic and changes the result data type based on custom rules |

## Feature Status {#feature-status}

| Feature ID | Feature Name                                                                                                             | Status      | Comment                                                                                                                                           |
|------------|--------------------------------------------------------------------------------------------------------------------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| **E011**   | **Numeric data types**                                                                                                   | **Partial** |                                                                                                                                                   |
| E011-01    | INTEGER and SMALLINT data types                                                                                          | Yes         |                                                                                                                                                   |
| E011-02    | REAL, DOUBLE PRECISION and FLOAT data types data types                                                                   | Partial     | `FLOAT(<binary_precision>)`, `REAL` and `DOUBLE PRECISION` are not supported                                                                      |
| E011-03    | DECIMAL and NUMERIC data types                                                                                           | Partial     | Only `DECIMAL(p,s)` is supported, not `NUMERIC`                                                                                                   |
| E011-04    | Arithmetic operators                                                                                                     | Yes         |                                                                                                                                                   |
| E011-05    | Numeric comparison                                                                                                       | Yes         |                                                                                                                                                   |
| E011-06    | Implicit casting among the numeric data types                                                                            | No          | ANSI SQL allows arbitrary implicit cast between numeric types, while ClickHouse relies on functions having multiple overloads instead of implicit |
| **E021**   | **Character string types**                                                                                               | **Partial** |                                                                                                                                                   |
| E021-01    | CHARACTER data type                                                                                                      | No          |                                                                                                                                                   |
| E021-02    | CHARACTER VARYING data type                                                                                              | No          | `String` behaves similarly, but without length limit in parentheses                                                                               |
| E021-03    | Character literals                                                                                                       | Partial     | No automatic concatenation of consecutive literals and character set support                                                                      |
| E021-04    | CHARACTER\_LENGTH function                                                                                               | Partial     | No `USING` clause                                                                                                                                 |
| E021-05    | OCTET\_LENGTH function                                                                                                   | No          | `LENGTH` behaves similarly                                                                                                                        |
| E021-06    | SUBSTRING                                                                                                                | Partial     | No support for `SIMILAR` and `ESCAPE` clauses, no `SUBSTRING_REGEX` variant                                                                       |
| E021-07    | Character concatenation                                                                                                  | Partial     | No `COLLATE` clause                                                                                                                               |
| E021-08    | UPPER and LOWER functions                                                                                                | Yes         |                                                                                                                                                   |
| E021-09    | TRIM function                                                                                                            | Yes         |                                                                                                                                                   |
| E021-10    | Implicit casting among the fixed-length and variable-length character string types                                       | No          | ANSI SQL allows arbitrary implicit cast between string types, while ClickHouse relies on functions having multiple overloads instead of implicit  |
| E021-11    | POSITION function                                                                                                        | Partial     | No support for `IN` and `USING` clauses, no `POSITION_REGEX` variant                                                                              |
| E021-12    | Character comparison                                                                                                     | Yes         |                                                                                                                                                   |
| **E031**   | **Identifiers**                                                                                                          | **Partial** |                                                                                                                                                   |
| E031-01    | Delimited identifiers                                                                                                    | Partial     | Unicode literal support is limited                                                                                                                |
| E031-02    | Lower case identifiers                                                                                                   | Yes         |                                                                                                                                                   |
| E031-03    | Trailing underscore                                                                                                      | Yes         |                                                                                                                                                   |
| **E051**   | **Basic query specification**                                                                                            | **Partial** |                                                                                                                                                   |
| E051-01    | SELECT DISTINCT                                                                                                          | Yes         |                                                                                                                                                   |
| E051-02    | GROUP BY clause                                                                                                          | Yes         |                                                                                                                                                   |
| E051-04    | GROUP BY can contain columns not in <select list>                                                                        | Yes         |                                                                                                                                                   |
| E051-05    | Select items can be renamed                                                                                              | Yes         |                                                                                                                                                   |
| E051-06    | HAVING clause                                                                                                            | Yes         |                                                                                                                                                   |
| E051-07    | Qualified \* in select list                                                                                              | Yes         |                                                                                                                                                   |
| E051-08    | Correlation name in the FROM clause                                                                                      | Yes         |                                                                                                                                                   |
| E051-09    | Rename columns in the FROM clause                                                                                        | No          |                                                                                                                                                   |
| **E061**   | **Basic predicates and search conditions**                                                                               | **Partial** |                                                                                                                                                   |
| E061-01    | Comparison predicate                                                                                                     | Yes         |                                                                                                                                                   |
| E061-02    | BETWEEN predicate                                                                                                        | Partial     | No `SYMMETRIC` and `ASYMMETRIC` clause                                                                                                            |
| E061-03    | IN predicate with list of values                                                                                         | Yes         |                                                                                                                                                   |
| E061-04    | LIKE predicate                                                                                                           | Yes         |                                                                                                                                                   |
| E061-05    | LIKE predicate: ESCAPE clause                                                                                            | No          |                                                                                                                                                   |
| E061-06    | NULL predicate                                                                                                           | Yes         |                                                                                                                                                   |
| E061-07    | Quantified comparison predicate                                                                                          | No          |                                                                                                                                                   |
| E061-08    | EXISTS predicate                                                                                                         | No          |                                                                                                                                                   |
| E061-09    | Subqueries in comparison predicate                                                                                       | Yes         |                                                                                                                                                   |
| E061-11    | Subqueries in IN predicate                                                                                               | Yes         |                                                                                                                                                   |
| E061-12    | Subqueries in quantified comparison predicate                                                                            | No          |                                                                                                                                                   |
| E061-13    | Correlated subqueries                                                                                                    | No          |                                                                                                                                                   |
| E061-14    | Search condition                                                                                                         | Yes         |                                                                                                                                                   |
| **E071**   | **Basic query expressions**                                                                                              | **Partial** |                                                                                                                                                   |
| E071-01    | UNION DISTINCT table operator                                                                                            | No          |                                                                                                                                                   |
| E071-02    | UNION ALL table operator                                                                                                 | Yes         |                                                                                                                                                   |
| E071-03    | EXCEPT DISTINCT table operator                                                                                           | No          |                                                                                                                                                   |
| E071-05    | Columns combined via table operators need not have exactly the same data type                                            | Yes         |                                                                                                                                                   |
| E071-06    | Table operators in subqueries                                                                                            | Yes         |                                                                                                                                                   |
| **E081**   | **Basic privileges**                                                                                                     | **Partial** | Work in progress                                                                                                                                  |
| **E091**   | **Set functions**                                                                                                        | **Yes**     |                                                                                                                                                   |
| E091-01    | AVG                                                                                                                      | Yes         |                                                                                                                                                   |
| E091-02    | COUNT                                                                                                                    | Yes         |                                                                                                                                                   |
| E091-03    | MAX                                                                                                                      | Yes         |                                                                                                                                                   |
| E091-04    | MIN                                                                                                                      | Yes         |                                                                                                                                                   |
| E091-05    | SUM                                                                                                                      | Yes         |                                                                                                                                                   |
| E091-06    | ALL quantifier                                                                                                           | No          |                                                                                                                                                   |
| E091-07    | DISTINCT quantifier                                                                                                      | Partial     | Not all aggregate functions supported                                                                                                             |
| **E101**   | **Basic data manipulation**                                                                                              | **Partial** |                                                                                                                                                   |
| E101-01    | INSERT statement                                                                                                         | Yes         | Note: primary key in ClickHouse does not imply the `UNIQUE` constraint                                                                            |
| E101-03    | Searched UPDATE statement                                                                                                | No          | There’s an `ALTER UPDATE` statement for batch data modification                                                                                   |
| E101-04    | Searched DELETE statement                                                                                                | No          | There’s an `ALTER DELETE` statement for batch data removal                                                                                        |
| **E111**   | **Single row SELECT statement**                                                                                          | **No**      |                                                                                                                                                   |
| **E121**   | **Basic cursor support**                                                                                                 | **No**      |                                                                                                                                                   |
| E121-01    | DECLARE CURSOR                                                                                                           | No          |                                                                                                                                                   |
| E121-02    | ORDER BY columns need not be in select list                                                                              | No          |                                                                                                                                                   |
| E121-03    | Value expressions in ORDER BY clause                                                                                     | No          |                                                                                                                                                   |
| E121-04    | OPEN statement                                                                                                           | No          |                                                                                                                                                   |
| E121-06    | Positioned UPDATE statement                                                                                              | No          |                                                                                                                                                   |
| E121-07    | Positioned DELETE statement                                                                                              | No          |                                                                                                                                                   |
| E121-08    | CLOSE statement                                                                                                          | No          |                                                                                                                                                   |
| E121-10    | FETCH statement: implicit NEXT                                                                                           | No          |                                                                                                                                                   |
| E121-17    | WITH HOLD cursors                                                                                                        | No          |                                                                                                                                                   |
| **E131**   | **Null value support (nulls in lieu of values)**                                                                         | **Partial** | Some restrictions apply                                                                                                                           |
| **E141**   | **Basic integrity constraints**                                                                                          | **Partial** |                                                                                                                                                   |
| E141-01    | NOT NULL constraints                                                                                                     | Yes         | Note: `NOT NULL` is implied for table columns by default                                                                                          |
| E141-02    | UNIQUE constraint of NOT NULL columns                                                                                    | No          |                                                                                                                                                   |
| E141-03    | PRIMARY KEY constraints                                                                                                  | No          |                                                                                                                                                   |
| E141-04    | Basic FOREIGN KEY constraint with the NO ACTION default for both referential delete action and referential update action | No          |                                                                                                                                                   |
| E141-06    | CHECK constraint                                                                                                         | Yes         |                                                                                                                                                   |
| E141-07    | Column defaults                                                                                                          | Yes         |                                                                                                                                                   |
| E141-08    | NOT NULL inferred on PRIMARY KEY                                                                                         | Yes         |                                                                                                                                                   |
| E141-10    | Names in a foreign key can be specified in any order                                                                     | No          |                                                                                                                                                   |
| **E151**   | **Transaction support**                                                                                                  | **No**      |                                                                                                                                                   |
| E151-01    | COMMIT statement                                                                                                         | No          |                                                                                                                                                   |
| E151-02    | ROLLBACK statement                                                                                                       | No          |                                                                                                                                                   |
| **E152**   | **Basic SET TRANSACTION statement**                                                                                      | **No**      |                                                                                                                                                   |
| E152-01    | SET TRANSACTION statement: ISOLATION LEVEL SERIALIZABLE clause                                                           | No          |                                                                                                                                                   |
| E152-02    | SET TRANSACTION statement: READ ONLY and READ WRITE clauses                                                              | No          |                                                                                                                                                   |
| **E153**   | **Updatable queries with subqueries**                                                                                    | **No**      | ?                                                                                                                                                 |
| **E161**   | **SQL comments using leading double minus**                                                                              | **Yes**     |                                                                                                                                                   |
| **E171**   | **SQLSTATE support**                                                                                                     | **No**      |                                                                                                                                                   |
| **E182**   | **Host language binding**                                                                                                | **No**      |                                                                                                                                                   |
| **F031** | **Basic schema manipulation** | **Partial** | |
| F031-01 | CREATE TABLE statement to create persistent base tables | Partial | No `SYSTEM VERSIONING`, `ON COMMIT`, `GLOBAL`, `LOCAL`, `PRESERVE`, `DELETE`, `REF IS`, `WITH OPTIONS`, `UNDER`, `LIKE`, `PERIOD FOR` clauses and no support for user resolved data types |
| F031-02 | CREATE VIEW statement | Partial | No `RECURSIVE`, `CHECK`, `UNDER`, `WITH OPTIONS` clauses and no support for user resolved data types |
| F031-03 | GRANT statement | Yes | |
| F031-04 | ALTER TABLE statement: ADD COLUMN clause | Partial | No support for `GENERATED` clause and system time period  |
| F031-13 | DROP TABLE statement: RESTRICT clause | No | |
| F031-16 | DROP VIEW statement: RESTRICT clause | No | |
| F031-19 | REVOKE statement: RESTRICT clause | No | |
| **F041** | **Basic joined table** | **Partial** | |
| F041-01 | Inner join (but not necessarily the INNER keyword) | Yes | |
| F041-02 | INNER keyword | Yes | |
| F041-03 | LEFT OUTER JOIN | Yes | |
| F041-04 | RIGHT OUTER JOIN | Yes | |
| F041-05 | Outer joins can be nested | Yes | |
| F041-07 | The inner table in a left or right outer join can also be used in an inner join | Yes  | |
| F041-08 | All comparison operators are supported (rather than just =) | No | |
| **F051** | **Basic date and time** | **Partial** | |
| F051-01 | DATE data type (including support of DATE literal) | Partial | No literal |
| F051-02 | TIME data type (including support of TIME literal) with fractional seconds precision of at least 0 | No | |
| F051-03 | TIMESTAMP data type (including support of TIMESTAMP literal) with fractional seconds precision of at least 0 and 6 | No | `DateTime64` time provides similar functionality |
| F051-04 | Comparison predicate on DATE, TIME, and TIMESTAMP data types | Partial | Only one data type available |
| F051-05 | Explicit CAST between datetime types and character string types | Yes | |
| F051-06 | CURRENT_DATE | No | `today()` is similar |
| F051-07 | LOCALTIME | No | `now()` is similar |
| F051-08 | LOCALTIMESTAMP | No | |
| **F081** | **UNION and EXCEPT in views** | **Partial** | |
| **F131** | **Grouped operations** | **Partial** | |
| F131-01 | WHERE, GROUP BY, and HAVING clauses supported in queries with grouped views | Yes | |
| F131-02 | Multiple tables supported in queries with grouped views | Yes | |
| F131-03 | Set functions supported in queries with grouped views | Yes | |
| F131-04 | Subqueries with GROUP BY and HAVING clauses and grouped views | Yes | |
| F131-05 | Single row SELECT with GROUP BY and HAVING clauses and grouped views | No | |
| **F181** | **Multiple module support** | **No** | |
| **F201** | **CAST function** | **Yes** | |
| **F221** | **Explicit defaults** | **No** | |
| **F261** | **CASE expression** | **Yes** | |
| F261-01 | Simple CASE | Yes | |
| F261-02 | Searched CASE | Yes | |
| F261-03 | NULLIF | Yes | |
| F261-04 | COALESCE | Yes | |
| **F311** | **Schema definition statement** | Partial | |
| F311-01 | CREATE SCHEMA | No | |
| F311-02 | CREATE TABLE for persistent base tables | Yes | |
| F311-03 | CREATE VIEW | Yes | |
| F311-04 | CREATE VIEW: WITH CHECK OPTION | No | |
| F311-05 | GRANT statement | Yes | |
| **F471** | **Scalar subquery values** | **Yes** | |
| **F481** | **Expanded NULL predicate** | **Yes**| |
| **F812** | **Basic flagging** | **No** | |
| **T321** | **Basic SQL-invoked routines** | **No** | |
| T321-01 | User-defined functions with no overloading | **No** | |
| T321-02 | User-defined stored procedures with no overloading | **No** | |
| T321-03 | Function invocation | **No** | |
| T321-04 | CALL statement | **No** | |
| T321-05 | RETURN statement | **No** | |
| **T631** | **IN predicate with one list element** | **Yes** | |
