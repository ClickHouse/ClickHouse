-- `uptime` is a server constant whose value is snapshotted when the `FunctionBase` is built,
-- so it changes over time. `isDeterministicInScopeOfQuery` promises that identical calls
-- resolve to a single constant within one query: the analyzer must deduplicate the built
-- `FunctionBase` even though the function is a server constant, otherwise these comparisons
-- become timing-dependent (they fail when analysis crosses a second boundary between two
-- resolutions of the call).

SET enable_analyzer = 1;

SELECT uptime() = uptime();
SELECT (SELECT uptime()) = (SELECT uptime());
SELECT abs(uptime()) = abs(uptime());
