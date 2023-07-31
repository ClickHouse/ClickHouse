---
sidebar_position: 65
sidebar_label: "Функции интроспекции"
---

# Функции интроспекции {#introspection-functions}

Функции из этого раздела могут использоваться для интроспекции [ELF](https://en.wikipedia.org/wiki/Executable_and_Linkable_Format) и [DWARF](https://en.wikipedia.org/wiki/DWARF) в целях профилирования запросов.

:::danger "Предупреждение"
    Эти функции выполняются медленно и могут приводить к нежелательным последствиям в плане безопасности.

Для правильной работы функций интроспекции:

-   Установите пакет `clickhouse-common-static-dbg`.

-   Установите настройку [allow_introspection_functions](../../operations/settings/settings.md#settings-allow_introspection_functions) в 1.

Из соображений безопасности данные функции отключены по умолчанию.

ClickHouse сохраняет отчеты профилировщика в [журнал трассировки](../../operations/system-tables/trace_log.md#system_tables-trace_log) в системной таблице. Убедитесь, что таблица и профилировщик настроены правильно.

## addresssToLine {#addresstoline}

Преобразует адрес виртуальной памяти внутри процесса сервера ClickHouse в имя файла и номер строки в исходном коде ClickHouse.

Если вы используете официальные пакеты ClickHouse, вам необходимо установить следующий пакеты: `clickhouse-common-static-dbg`.

**Синтаксис**

``` sql
addressToLine(address_of_binary_instruction)
```

**Аргументы**

-   `address_of_binary_instruction` ([Тип UInt64](../../sql-reference/functions/introspection.md))- Адрес инструкции в запущенном процессе.

**Возвращаемое значение**

-   Имя файла исходного кода и номер строки в этом файле разделяются двоеточием.

        Например, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`, где `199` — номер строки.

-   Имя бинарного файла, если функция не может найти отладочную информацию.

-   Пустая строка, если адрес не является допустимым.

Тип: [String](../../sql-reference/functions/introspection.md).

**Пример**

Включение функций самоанализа:

``` sql
SET allow_introspection_functions=1;
```

Выбор первой строки из списка `trace_log` системная таблица:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
```

``` text
Row 1:
──────
event_date:              2019-11-19
event_time:              2019-11-19 18:57:23
revision:                54429
timer_type:              Real
thread_number:           48
query_id:                421b6855-1858-45a5-8f37-f383409d6d72
trace:                   [140658411141617,94784174532828,94784076370703,94784076372094,94784076361020,94784175007680,140658411116251,140658403895439]
```

То `trace` поле содержит трассировку стека в момент выборки.

Получение имени файла исходного кода и номера строки для одного адреса:

``` sql
SELECT addressToLine(94784076370703) \G;
```

``` text
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
```

Применение функции ко всему стектрейсу:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
\G
```

Функция [arrayMap](../../sql-reference/functions/array-functions.md#array-map) позволяет обрабатывать каждый отдельный элемент массива `trace` с помощью функции `addressToLine`. Результат этой обработки вы видите в виде `trace_source_code_lines` колонки выходных данных.

``` text
Row 1:
──────
trace_source_code_lines: /lib/x86_64-linux-gnu/libpthread-2.27.so
/usr/lib/debug/usr/bin/clickhouse
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.h:155
/usr/include/c++/9/bits/atomic_base.h:551
/usr/lib/debug/usr/bin/clickhouse
/lib/x86_64-linux-gnu/libpthread-2.27.so
/build/glibc-OTsEL5/glibc-2.27/misc/../sysdeps/unix/sysv/linux/x86_64/clone.S:97
```

## addressToSymbol {#addresstosymbol}

Преобразует адрес виртуальной памяти внутри серверного процесса ClickHouse в символ из объектных файлов ClickHouse.

**Синтаксис**

``` sql
addressToSymbol(address_of_binary_instruction)
```

**Аргументы**

-   `address_of_binary_instruction` ([Тип uint64](../../sql-reference/functions/introspection.md)) — адрес инструкции в запущенном процессе.

**Возвращаемое значение**

-   Символ из объектных файлов ClickHouse.
-   Пустая строка, если адрес не является допустимым.

Тип: [String](../../sql-reference/functions/introspection.md).

**Пример**

Включение функций самоанализа:

``` sql
SET allow_introspection_functions=1;
```

Выбор первой строки из списка `trace_log` системная таблица:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
```

``` text
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [94138803686098,94138815010911,94138815096522,94138815101224,94138815102091,94138814222988,94138806823642,94138814457211,94138806823642,94138814457211,94138806823642,94138806795179,94138806796144,94138753770094,94138753771646,94138753760572,94138852407232,140399185266395,140399178045583]
```

То `trace` поле содержит трассировку стека в момент выборки.

Получение символа для одного адреса:

``` sql
SELECT addressToSymbol(94138803686098) \G;
```

``` text
Row 1:
──────
addressToSymbol(94138803686098): _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
```

Применение функции ко всей трассировке стека:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
\G
```

То [arrayMap](../../sql-reference/functions/array-functions.md#array-map) функция позволяет обрабатывать каждый отдельный элемент системы. `trace` массив по типу `addressToSymbols` функция. Результат этой обработки вы видите в виде `trace_symbols` колонка выходных данных.

``` text
Row 1:
──────
trace_symbols: _ZNK2DB24IAggregateFunctionHelperINS_20AggregateFunctionSumImmNS_24AggregateFunctionSumDataImEEEEE19addBatchSinglePlaceEmPcPPKNS_7IColumnEPNS_5ArenaE
_ZNK2DB10Aggregator21executeWithoutKeyImplERPcmPNS0_28AggregateFunctionInstructionEPNS_5ArenaE
_ZN2DB10Aggregator14executeOnBlockESt6vectorIN3COWINS_7IColumnEE13immutable_ptrIS3_EESaIS6_EEmRNS_22AggregatedDataVariantsERS1_IPKS3_SaISC_EERS1_ISE_SaISE_EERb
_ZN2DB10Aggregator14executeOnBlockERKNS_5BlockERNS_22AggregatedDataVariantsERSt6vectorIPKNS_7IColumnESaIS9_EERS6_ISB_SaISB_EERb
_ZN2DB10Aggregator7executeERKSt10shared_ptrINS_17IBlockInputStreamEERNS_22AggregatedDataVariantsE
_ZN2DB27AggregatingBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB26ExpressionBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB26ExpressionBlockInputStream8readImplEv
_ZN2DB17IBlockInputStream4readEv
_ZN2DB28AsynchronousBlockInputStream9calculateEv
_ZNSt17_Function_handlerIFvvEZN2DB28AsynchronousBlockInputStream4nextEvEUlvE_E9_M_invokeERKSt9_Any_data
_ZN14ThreadPoolImplI20ThreadFromGlobalPoolE6workerESt14_List_iteratorIS0_E
_ZZN20ThreadFromGlobalPoolC4IZN14ThreadPoolImplIS_E12scheduleImplIvEET_St8functionIFvvEEiSt8optionalImEEUlvE1_JEEEOS4_DpOT0_ENKUlvE_clEv
_ZN14ThreadPoolImplISt6threadE6workerESt14_List_iteratorIS0_E
execute_native_thread_routine
start_thread
clone
```

## demangle {#demangle}

Преобразует символ, который вы можете получить с помощью [addressstosymbol](#addresstosymbol) функция имя функции C++.

**Синтаксис**

``` sql
demangle(symbol)
```

**Аргументы**

-   `symbol` ([Строка](../../sql-reference/functions/introspection.md)) - символ из объектного файла.

**Возвращаемое значение**

-   Имя функции C++.
-   Пустая строка, если символ не является допустимым.

Тип: [Строка](../../sql-reference/functions/introspection.md).

**Пример**

Включение функций самоанализа:

``` sql
SET allow_introspection_functions=1;
```

Выбор первой строки из списка `trace_log` системная таблица:

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G;
```

``` text
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [94138803686098,94138815010911,94138815096522,94138815101224,94138815102091,94138814222988,94138806823642,94138814457211,94138806823642,94138814457211,94138806823642,94138806795179,94138806796144,94138753770094,94138753771646,94138753760572,94138852407232,140399185266395,140399178045583]
```

То `trace` поле содержит трассировку стека в момент выборки.

Получение имени функции для одного адреса:

``` sql
SELECT demangle(addressToSymbol(94138803686098)) \G;
```

``` text
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
```

Применение функции ко всему стектрейсу:

``` sql
SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
```

Функция [arrayMap](../../sql-reference/functions/array-functions.md#array-map) позволяет обрабатывать каждый отдельный элемент массива `trace` с помощью функции `demangle`.

``` text
Row 1:
──────
trace_functions: DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
DB::Aggregator::executeWithoutKeyImpl(char*&, unsigned long, DB::Aggregator::AggregateFunctionInstruction*, DB::Arena*) const
DB::Aggregator::executeOnBlock(std::vector<COW<DB::IColumn>::immutable_ptr<DB::IColumn>, std::allocator<COW<DB::IColumn>::immutable_ptr<DB::IColumn> > >, unsigned long, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::executeOnBlock(DB::Block const&, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::execute(std::shared_ptr<DB::IBlockInputStream> const&, DB::AggregatedDataVariants&)
DB::AggregatingBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::AsynchronousBlockInputStream::calculate()
std::_Function_handler<void (), DB::AsynchronousBlockInputStream::next()::{lambda()#1}>::_M_invoke(std::_Any_data const&)
ThreadPoolImpl<ThreadFromGlobalPool>::worker(std::_List_iterator<ThreadFromGlobalPool>)
ThreadFromGlobalPool::ThreadFromGlobalPool<ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}>(ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}&&)::{lambda()#1}::operator()() const
ThreadPoolImpl<std::thread>::worker(std::_List_iterator<std::thread>)
execute_native_thread_routine
start_thread
clone
```

## tid {#tid}

Возвращает id потока, в котором обрабатывается текущий [Block](https://clickhouse.com/docs/ru/development/architecture/#block).

**Синтаксис**

``` sql
tid()
```

**Возвращаемое значение**

-   Id текущего потока. [Uint64](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Пример**

Запрос:

``` sql
SELECT tid();
```

Результат:

``` text
┌─tid()─┐
│  3878 │
└───────┘
```

## logTrace {#logtrace}

 Выводит сообщение в лог сервера для каждого [Block](https://clickhouse.com/docs/ru/development/architecture/#block).

**Синтаксис**

``` sql
logTrace('message')
```

**Аргументы**

-   `message` — сообщение, которое отправляется в серверный лог. [String](../../sql-reference/data-types/string.md#string).

**Возвращаемое значение**

-   Всегда возвращает 0.

**Пример**

Запрос:

``` sql
SELECT logTrace('logTrace message');
```

Результат:

``` text
┌─logTrace('logTrace message')─┐
│                            0 │
└──────────────────────────────┘
```

[Original article](https://clickhouse.com/docs/en/query_language/functions/introspection/) <!--hide-->
