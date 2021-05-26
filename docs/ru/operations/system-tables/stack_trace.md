# system.stack_trace {#system-tables_stack_trace}

Содержит трассировки стека всех серверных потоков. Позволяет разработчикам анализировать состояние сервера.

Для анализа логов используйте [функции интроспекции](../../sql-reference/functions/introspection.md): `addressToLine`, `addressToSymbol` и `demangle`.

Столбцы:

-   `thread_name` ([String](../../sql-reference/data-types/string.md)) — имя потока.
-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — идентификатор потока.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор запроса. Может быть использован для получения подробной информации о выполненном запросе из системной таблицы [query_log](#system_tables-query_log).
-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — [трассировка стека](https://en.wikipedia.org/wiki/Stack_trace). Представляет собой список физических адресов, по которым расположены вызываемые методы.

**Пример**

Включение функций интроспекции:

``` sql
SET allow_introspection_functions = 1;
```

Получение символов из объектных файлов ClickHouse:

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

Получение имен файлов и номеров строк в исходном коде ClickHouse:

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

**Смотрите также**

-   [Функции интроспекции](../../sql-reference/functions/introspection.md) — что такое функции интроспекции и как их использовать.
-   [system.trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) — системная таблица, содержащая трассировки стека, собранные профилировщиком выборочных запросов.
-   [arrayMap](../../sql-reference/functions/array-functions.md#array-map) — описание и пример использования функции `arrayMap`.
-   [arrayFilter](../../sql-reference/functions/array-functions.md#array-filter) — описание и пример использования функции `arrayFilter`.
