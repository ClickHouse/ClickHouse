#include <Parsers/ParserTransactionControl.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

bool ParserTransactionControl::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTTransactionControl::QueryType action = {};
    UInt64 snapshot = 0;

    if (ParserKeyword(Keyword::BEGIN_TRANSACTION).ignore(pos, expected))
        action = ASTTransactionControl::BEGIN;
    else if (ParserKeyword(Keyword::START_TRANSACTION).ignore(pos, expected))
        action = ASTTransactionControl::BEGIN;
    else if (ParserKeyword(Keyword::COMMIT).ignore(pos, expected))
        action = ASTTransactionControl::COMMIT;
    else if (ParserKeyword(Keyword::ROLLBACK).ignore(pos, expected))
        action = ASTTransactionControl::ROLLBACK;
    else if (ParserKeyword(Keyword::SET_TRANSACTION_SNAPSHOT).ignore(pos, expected))
    {
        action = ASTTransactionControl::SET_SNAPSHOT;
        ASTPtr ast;
        if (!ParserNumber{}.parse(pos, ast, expected))
            return false;

        const auto & snapshot_num = ast->as<ASTLiteral>()->value;
        if (!snapshot_num.tryGet(snapshot))
            return false;
    }
    else
        return false;

    auto ast = make_intrusive<ASTTransactionControl>(action);
    ast->snapshot = snapshot;
    node = ast;
    return true;
}

std::map<String, Documentation> ParserTransactionControl::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["BEGIN TRANSACTION"] =
    {
        .description = R"DOCS_MD(
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

Introduced in: v22.4.0

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

The transaction control statements `BEGIN TRANSACTION`, `COMMIT`, `ROLLBACK` and `SET TRANSACTION SNAPSHOT` group several queries over `MergeTree` tables into one transaction.
The queries of a transaction read one consistent snapshot of the data.
Other transactions see its changes only after `COMMIT`, and `ROLLBACK` discards them.
Queries that run outside a transaction can see the changes of a running transaction before it commits.

<Note>
These statements are part of an experimental feature set.
Enable experimental transaction support by adding this setting to your [configuration](/concepts/features/configuration/server-config/configuration-files):

```xml
<clickhouse>
    <allow_experimental_transactions>1</allow_experimental_transactions>
</clickhouse>
```

For more information see the page [Transactional (ACID) support](/concepts/features/operations/insert/transactions#transactions-commit-and-rollback).
</Note>

## Syntax {#syntax}

```sql
BEGIN TRANSACTION
START TRANSACTION
COMMIT
ROLLBACK
SET TRANSACTION SNAPSHOT snapshot
```

A transaction belongs to a session, and a session has at most one transaction.
Over HTTP, send every query of a transaction with the same `session_id`: a request without `session_id` runs in its own session, which ends with the request.
A transaction that is still running when its session ends is rolled back.

## BEGIN TRANSACTION {#begin-transaction}

Starts a transaction in the current session. `START TRANSACTION` is an alias, introduced in v24.3.0.

Nested transactions are not supported: `BEGIN TRANSACTION` inside a transaction throws an `INVALID_TRANSACTION` exception.

## COMMIT {#commit}

Commits the current transaction and makes its changes visible to other transactions.
`COMMIT` without a current transaction throws an `INVALID_TRANSACTION` exception.

If a query inside the transaction fails, the transaction is rolled back. Until `ROLLBACK` ends the transaction, `COMMIT` and the other queries that follow throw an `INVALID_TRANSACTION` exception. Only `ROLLBACK`, `SET TRANSACTION SNAPSHOT` and the `EXPLAIN` queries other than `EXPLAIN ANALYZE` still run.

The [`wait_changes_become_visible_after_commit_mode`](/reference/settings/session-settings/other#wait_changes_become_visible_after_commit_mode) setting controls what `COMMIT` waits for.

## ROLLBACK {#rollback}

Rolls back the current transaction and discards its changes.
`ROLLBACK` without a current transaction throws an `INVALID_TRANSACTION` exception.

## SET TRANSACTION SNAPSHOT {#set-transaction-snapshot}

Changes the snapshot that the current transaction reads to the commit sequence number (CSN) `snapshot`, for example a value that [`transactionLatestSnapshot`](/reference/functions/regular-functions/other-functions#transactionLatestSnapshot) returned in an earlier transaction.
The transaction then sees its own changes, the data written outside transactions and the changes of the transactions committed up to that CSN.
`SET TRANSACTION SNAPSHOT` without a current transaction throws an `INVALID_TRANSACTION` exception.

The values from `0` to `32` are reserved and throw an `INVALID_TRANSACTION` exception, except two:
`1` makes only the data written outside transactions and the transaction's own changes visible, and `3` makes all the parts of a table visible, including the parts that transactions removed or rolled back.

## Queries inside a transaction {#queries-inside-a-transaction}

A transaction can run `SELECT`, `INSERT`, `DELETE`, `OPTIMIZE`, `TRUNCATE TABLE`, `SET` and `EXPLAIN` queries, and the `ALTER TABLE` mutations and partition operations.
Other queries, for example `CREATE TABLE`, `UPDATE` or `ALTER TABLE ... ADD COLUMN`, throw a `NOT_IMPLEMENTED` exception, and so do queries over tables that do not support transactions, for example `ReplicatedMergeTree` or `Memory` tables, or `system.transactions`.

Asynchronous inserts are not supported inside a transaction, and [`async_insert`](/reference/settings/session-settings/async-insert#async_insert) is enabled by default, so an `INSERT ... VALUES` or `INSERT ... SELECT` inside a transaction throws a `NOT_IMPLEMENTED` exception unless `async_insert` is disabled, for example with `SET async_insert = 0`.

With [`throw_on_unsupported_query_inside_transaction`](/reference/settings/session-settings/throw-on#throw_on_unsupported_query_inside_transaction) set to `0`, some of these queries run outside the transaction instead of throwing, so `ROLLBACK` does not undo them: for example, a `CREATE TABLE` or an `INSERT` into a `Memory` table.

With [`implicit_transaction`](/reference/settings/session-settings/implicit#implicit_transaction) enabled, each query that runs outside a transaction runs in its own transaction, which is committed when the query succeeds and rolled back when it fails.

## Examples {#examples}

Roll back an insert:

```sql title="Query"
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n;
SET async_insert = 0;

BEGIN TRANSACTION;
INSERT INTO t VALUES (1);
SELECT count() FROM t;
ROLLBACK;
SELECT count() FROM t;
```

```text title="Response"
┌─count()─┐
│       1 │
└─────────┘
┌─count()─┐
│       0 │
└─────────┘
```

Commit an insert:

```sql title="Query"
BEGIN TRANSACTION;
INSERT INTO t VALUES (2);
COMMIT;
SELECT * FROM t;
```

```text title="Response"
┌─n─┐
│ 2 │
└───┘
```

## Related content {#related-content}

- [Transactional (ACID) support](/concepts/features/operations/insert/transactions)
- [`system.transactions`](/reference/system-tables/transactions)
- [`KILL TRANSACTION`](/reference/statements/kill#kill-transaction)
- [`EXPLAIN CURRENT TRANSACTION`](/reference/statements/explain#explain-current-transaction)
)DOCS_MD",
        .syntax = R"(
BEGIN TRANSACTION
START TRANSACTION
COMMIT
ROLLBACK
SET TRANSACTION SNAPSHOT snapshot
)",
        .related = {"KILL", "EXPLAIN", "SET"},
    };

    return documentation;
}

}
