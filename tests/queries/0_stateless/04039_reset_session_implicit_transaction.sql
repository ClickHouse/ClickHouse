-- Tags: no-ordinary-database, no-fasttest
-- Transactions are only supported with MergeTree engines on non-Ordinary databases,
-- and are disabled in the fast-test config.

-- With `implicit_transaction = 1`, every regular query opens its own implicit
-- transaction. `RESET SESSION` must not get wrapped in one — otherwise the
-- in-transaction guard inside `Context::resetToUserDefaults` would trigger on
-- the implicit transaction we just opened, making a clean-session reset
-- impossible and (worse) preventing the reset from ever clearing the
-- `implicit_transaction` setting that caused the deadlock.

SET send_logs_level = 'fatal';
SET implicit_transaction = 1;

-- `RESET SESSION` is exempted from implicit-transaction wrapping, so the
-- reset succeeds and also clears the `implicit_transaction = 1` setting it
-- was running under.
RESET SESSION;
SELECT getSetting('implicit_transaction') = 0 AS implicit_transaction_cleared;
