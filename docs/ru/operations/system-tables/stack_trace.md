# system.stack_trace {#system-tables_stack_trace}

Содержит трассировки стека всех серверных потоков. Позволяет разработчикам анализировать состояние сервера.

Для анализа логов используйте [функции интроспекции](../../sql-reference/functions/introspection.md): `addressToLine`, `addressToSymbol` и `demangle`.

Столбцы:

-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Идентификатор потока.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Идентификатор запроса. Может быть использован для получения подробной информации о выполненном запросе из системной таблицы [query_log](#system_tables-query_log).
-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — [Трассировка стека](https://en.wikipedia.org/wiki/Stack_trace). Представляет собой список физических адресов, по которым расположены вызываемые методы.

**Пример**

Включение функций интроспекции:

``` sql
SET allow_introspection_functions = 1;
```

Получение символов из объектных файлов ClickHouse:

``` sql
WITH arrayMap(x -> demangle(addressToSymbol(x)), trace) AS all SELECT thread_id, query_id, arrayStringConcat(all, '\n') AS res FROM system.stack_trace LIMIT 1 \G
```

``` text
Row 1:
──────
thread_id: 686
query_id:  1a11f70b-626d-47c1-b948-f9c7b206395d
res:       sigqueue
DB::StorageSystemStackTrace::fillData(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::Context const&, DB::SelectQueryInfo const&) const
DB::IStorageSystemOneBlock<DB::StorageSystemStackTrace>::read(std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&, DB::SelectQueryInfo const&, DB::Context const&, DB::QueryProcessingStage::Enum, unsigned long, unsigned int)
DB::InterpreterSelectQuery::executeFetchColumns(DB::QueryProcessingStage::Enum, DB::QueryPipeline&, std::__1::shared_ptr<DB::PrewhereInfo> const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > > > const&)
DB::InterpreterSelectQuery::executeImpl(DB::QueryPipeline&, std::__1::shared_ptr<DB::IBlockInputStream> const&, std::__1::optional<DB::Pipe>)
DB::InterpreterSelectQuery::execute()
DB::InterpreterSelectWithUnionQuery::execute()
DB::executeQueryImpl(char const*, char const*, DB::Context&, bool, DB::QueryProcessingStage::Enum, bool, DB::ReadBuffer*)
DB::executeQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, DB::Context&, bool, DB::QueryProcessingStage::Enum, bool)
DB::TCPHandler::runImpl()
DB::TCPHandler::run()
Poco::Net::TCPServerConnection::start()
Poco::Net::TCPServerDispatcher::run()
Poco::PooledThread::run()
Poco::ThreadImpl::runnableEntry(void*)
start_thread
__clone
```

Получение имен файлов и номеров строк в исходном коде ClickHouse:

``` sql
WITH arrayMap(x -> addressToLine(x), trace) AS all, arrayFilter(x -> x LIKE '%/dbms/%', all) AS dbms SELECT thread_id, query_id, arrayStringConcat(notEmpty(dbms) ? dbms : all, '\n') AS res FROM system.stack_trace LIMIT 1 \G
```

``` text
Row 1:
──────
thread_id: 686
query_id:  cad353e7-1c29-4b2e-949f-93e597ab7a54
res:       /lib/x86_64-linux-gnu/libc-2.27.so
/build/obj-x86_64-linux-gnu/../src/Storages/System/StorageSystemStackTrace.cpp:182
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/vector:656
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:1338
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectQuery.cpp:751
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/optional:224
/build/obj-x86_64-linux-gnu/../src/Interpreters/InterpreterSelectWithUnionQuery.cpp:192
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:384
/build/obj-x86_64-linux-gnu/../src/Interpreters/executeQuery.cpp:643
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:251
/build/obj-x86_64-linux-gnu/../src/Server/TCPHandler.cpp:1197
/build/obj-x86_64-linux-gnu/../contrib/poco/Net/src/TCPServerConnection.cpp:57
/build/obj-x86_64-linux-gnu/../contrib/libcxx/include/atomic:856
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/Mutex_POSIX.h:59
/build/obj-x86_64-linux-gnu/../contrib/poco/Foundation/include/Poco/AutoPtr.h:223
/lib/x86_64-linux-gnu/libpthread-2.27.so
/lib/x86_64-linux-gnu/libc-2.27.so
```

**См. также**

-   [Функции интроспекции](../../sql-reference/functions/introspection.md) — Что такое функции интроспекции и как их использовать.
-   [system.trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) — Содержит трассировки стека, собранные профилировщиком выборочных запросов.
-   [arrayMap](../../sql-reference/functions/array-functions.md#array-map) — Описание и пример использования функции `arrayMap`.
-   [arrayFilter](../../sql-reference/functions/array-functions.md#array-filter) — Описание и пример использования функции `arrayFilter`.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/stack_trace) <!--hide-->
