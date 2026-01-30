select distinct memory_blocked_context from system.stack_trace where memory_blocked_context in ('Global', 'Max') order by all;
