# system.stack_trace {#system-tables_stack_trace}

Contains stack traces of all server threads. Allows developers to introspect the server state.

To analyze stack frames, use the `addressToLine`, `addressToSymbol` and `demangle` [introspection functions](../../sql-reference/functions/introspection.md).

Columns:

-   `thread_name` ([String](../../sql-reference/data-types/string.md)) — Thread name.
-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread identifier.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](../system-tables/query_log.md) system table.
-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — A [stack trace](https://en.wikipedia.org/wiki/Stack_trace) which represents a list of physical addresses where the called methods are stored.

**Example**

Enabling introspection functions:

``` sql
SET allow_introspection_functions = 1;
```

Getting symbols from ClickHouse object files:

``` sql
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS all SELECT thread_name, thread_id, query_id, arrayStringConcat(all, '\n') AS res FROM system.stack_trace LIMIT 1 \G;
```

``` text
Row 1:
──────
thread_name: clickhouse-serv

thread_id:   5928
query_id:
res:         pthread_cond_wait@@GLIBC_2.3.2
BaseDaemon::waitForTerminationRequest()
DB::Server::main(std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&)
Poco::Util::Application::run()
DB::Server::run()
mainEntryClickHouseServer(int, char**)
main
__libc_start_main
_start
```

Getting filenames and line numbers in ClickHouse source code:

``` sql
WITH arrayMap(x -> addressToLine(x), trace) AS all, arrayFilter(x -> x LIKE '%/dbms/%', all) AS dbms SELECT thread_name, thread_id, query_id, arrayStringConcat(notEmpty(dbms) ? dbms : all, '\n') AS res FROM system.stack_trace LIMIT 1 \G;
```

``` text
Row 1:
──────
thread_name: clickhouse-serv

thread_id:   5928
query_id:
res:         /lib/x86_64-linux-gnu/libpthread-2.27.so
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
/lib/x86_64-linux-gnu/libc-2.27.so
/usr/lib/debug/.build-id/8f/93104326882d1c62c221c34e6bad1b0fefc1e5.debug
```

**See Also**

-   [Introspection Functions](../../sql-reference/functions/introspection.md) — Which introspection functions are available and how to use them.
-   [system.trace_log](../system-tables/trace_log.md) — Contains stack traces collected by the sampling query profiler.
-   [arrayMap](../../sql-reference/functions/array-functions.md#array-map) — Description and usage example of the `arrayMap` function.
-   [arrayFilter](../../sql-reference/functions/array-functions.md#array-filter) — Description and usage example of the `arrayFilter` function.
