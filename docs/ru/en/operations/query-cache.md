---
description: 'Руководство по использованию и настройке кэша запросов в ClickHouse'
sidebar_label: 'Кэш запросов'
sidebar_position: 65
slug: /operations/query-cache
title: 'Кэш запросов'
doc_type: 'guide'
---

Кэш запросов позволяет выполнять `SELECT`-запросы только один раз, а все последующие выполнения того же запроса обслуживать напрямую из кэша.
В зависимости от типа запросов это может значительно снизить задержку и потребление ресурсов сервера ClickHouse.

<div id="background-design-and-limitations">
  ## Предпосылки, устройство и ограничения
</div>

Кэши запросов в целом можно разделить на транзакционно согласованные и несогласованные.

* В транзакционно согласованных кэшах база данных инвалидирует (отбрасывает) кэшированные результаты запросов, если результат запроса `SELECT` изменяется
  или потенциально может измениться. В ClickHouse к операциям, изменяющим данные, относятся вставки/обновления/удаления в таблицах/из таблиц, а также схлопывающие
  слияния. Транзакционно согласованное кэширование особенно подходит для OLTP-баз данных, например
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (где кэш запросов был удалён начиная с v8.0) и
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
* В транзакционно несогласованных кэшах допускаются небольшие неточности в результатах запросов, исходя из предположения, что всем записям кэша
  назначается период действия, по истечении которого они устаревают (например, 1 минута), и что исходные данные за это время изменяются незначительно.
  Такой подход в целом лучше подходит для OLAP-баз данных. В качестве примера, где транзакционно несогласованного кэширования достаточно,
  можно привести почасовой отчёт о продажах в инструменте отчётности, к которому одновременно обращаются несколько пользователей. Данные о продажах обычно изменяются
  достаточно медленно, поэтому базе данных достаточно вычислить отчёт только один раз (это соответствует первому запросу `SELECT`). Последующие запросы могут
  обслуживаться напрямую из кэша запросов. В этом примере разумный период действия мог бы составлять 30 минут.

Транзакционно несогласованное кэширование традиционно обеспечивается клиентскими инструментами или прокси-пакетами (например,
[chproxy](https://www.chproxy.org/configuration/caching/)), взаимодействующими с базой данных. В результате одна и та же логика кэширования и
конфигурация часто дублируются. С кэшем запросов ClickHouse логика кэширования переносится на сторону сервера. Это снижает затраты на сопровождение
и позволяет избежать избыточности.

<div id="configuration-settings-and-usage">
  ## Настройки конфигурации и использование
</div>

:::note
В ClickHouse Cloud для изменения настроек кэша запросов необходимо использовать [настройки уровня запроса](/ru/operations/settings/query-level). Изменение [настроек уровня конфигурации](/ru/operations/configuration-files) пока не поддерживается.
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md) выполняет только один запрос за раз. Поскольку кэширование результатов запросов в этом случае не имеет смысла, кэш результатов запросов в clickhouse-local отключен.
:::

Параметр [use&#95;query&#95;cache](/ru/operations/settings/settings#use_query_cache) позволяет указать, должен ли конкретный запрос или все запросы
текущего сеанса использовать кэш запросов. Например, при первом выполнении запроса

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

сохранит результат запроса в кэше запросов. При последующих выполнениях того же запроса (также с параметром `use_query_cache = true`) будет
считываться уже вычисленный результат из кэша и сразу возвращаться.

:::note
Параметр `use_query_cache` и все остальные настройки, связанные с кэшем запросов, действуют только для отдельных операторов `SELECT`. В частности,
результаты `SELECT` для представлений, созданных с помощью `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true`, не кэшируются, если оператор `SELECT`
не выполняется с `SETTINGS use_query_cache = true`.
:::

Поведение кэша можно более детально настроить с помощью параметров [enable&#95;writes&#95;to&#95;query&#95;cache](/ru/operations/settings/settings#enable_writes_to_query_cache)
и [enable&#95;reads&#95;from&#95;query&#95;cache](/ru/operations/settings/settings#enable_reads_from_query_cache) (оба имеют значение `true` по умолчанию). Первый параметр
определяет, сохраняются ли результаты запросов в кэше, а второй — должна ли база данных пытаться получать результаты
запросов из кэша. Например, следующий запрос будет использовать кэш только пассивно, то есть пытаться читать из него данные, но не сохранять в нём свой
результат:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

Для максимального контроля обычно рекомендуется задавать настройки `use_query_cache`, `enable_writes_to_query_cache` и
`enable_reads_from_query_cache` только для конкретных запросов. Также можно включить кэширование на уровне пользователя или профиля
(например, через `SET use_query_cache = true`), но следует помнить, что в этом случае все запросы `SELECT` могут возвращать кэшированные результаты.

Кэш запросов можно очистить с помощью оператора `SYSTEM CLEAR QUERY CACHE`. Содержимое кэша запросов отображается в системной таблице
[system.query&#95;cache](system-tables/query_cache.md). Количество попаданий и промахов кэша запросов с момента запуска базы данных
отображается как события &quot;QueryCacheHits&quot; и &quot;QueryCacheMisses&quot; в системной таблице [system.events](system-tables/events.md). Оба счётчика
обновляются только для запросов `SELECT`, выполняемых с настройкой `use_query_cache = true`; другие запросы не влияют на &quot;QueryCacheMisses&quot;.
Поле `query_cache_usage` в системной таблице [system.query&#95;log](system-tables/query_log.md) показывает для каждого выполненного запроса,
был ли результат запроса записан в кэш запросов или прочитан из него. Метрики `QueryCacheEntries` и `QueryCacheBytes` в системной таблице
[system.metrics](system-tables/metrics.md) показывают, сколько записей и байтов сейчас содержит кэш запросов.

Кэш запросов существует в единственном экземпляре для каждого процесса сервера ClickHouse. Однако по умолчанию результаты в кэше не
разделяются между пользователями. Это можно изменить (см. ниже), но делать этого не рекомендуется по соображениям безопасности.

Результаты запросов в кэше запросов привязываются к [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
соответствующего запроса. Это означает, что кэширование не зависит от регистра: например, `SELECT 1` и `select 1` считаются одним и тем
же запросом. Чтобы сделать сопоставление более естественным, из AST удаляются все настройки уровня запроса, связанные с кэшем запросов и
[форматированием вывода](settings/settings-formats.md)).

Если запрос был прерван из-за Исключения или отменён пользователем, запись в кэш запросов не создаётся.

Размер кэша запросов в байтах, максимальное число записей кэша и максимальный размер отдельных записей кэша (в байтах и в
строках) можно настроить с помощью различных [параметров конфигурации сервера](/ru/operations/server-configuration-parameters/settings#query_cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

Также можно ограничить использование кэша для отдельных пользователей с помощью [профилей настроек](settings/settings-profiles.md) и [ограничений
на настройки](settings/constraints-on-settings.md). В частности, можно ограничить максимальный объём памяти (в байтах), который пользователь может
выделить в кэше запросов, а также максимальное количество сохраняемых результатов запросов. Для этого сначала укажите настройки
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/ru/operations/settings/settings#query_cache_max_size_in_bytes) и
[query&#95;cache&#95;max&#95;entries](/ru/operations/settings/settings#query_cache_max_entries) в профиле пользователя в `users.xml`, а затем сделайте обе настройки
только для чтения:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

Чтобы задать минимальное время выполнения запроса, после которого его результат можно кэшировать, используйте настройку
[query&#95;cache&#95;min&#95;query&#95;duration](/ru/operations/settings/settings#query_cache_min_query_duration). Например, результат запроса

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

кэшируется только в том случае, если запрос выполняется дольше 5 секунд. Также можно указать, сколько раз должен выполниться запрос, прежде чем его результат
будет кэширован — для этого используйте настройку [query&#95;cache&#95;min&#95;query&#95;runs](/ru/operations/settings/settings#query_cache_min_query_runs).

Записи в кэше запросов устаревают через определенный период времени (time-to-live). По умолчанию этот период составляет 60 секунд, но можно задать и другое
значение на уровне сеанса, профиля или запроса с помощью настройки [query&#95;cache&#95;ttl](/ru/operations/settings/settings#query_cache_ttl). Кэш запросов
вытесняет записи &quot;лениво&quot;, то есть когда запись устаревает, она не удаляется из кэша сразу. Вместо этого, когда в кэш запросов нужно
вставить новую запись, база данных проверяет, достаточно ли в кэше свободного места для нее. Если нет, база данных пытается удалить
все устаревшие записи. Если свободного места в кэше по-прежнему недостаточно, новая запись не вставляется.

Если запрос выполняется через HTTP, ClickHouse устанавливает заголовки `Age` и `Expires`, указывая возраст (в секундах) и временную метку истечения срока
действия кэшированной записи.

По умолчанию записи в кэше запросов сжимаются. Это уменьшает общее потребление памяти ценой более медленной записи в кэш запросов и чтения
из него. Чтобы отключить сжатие, используйте настройку [query&#95;cache&#95;compress&#95;entries](/ru/operations/settings/settings#query_cache_compress_entries).

Иногда бывает полезно хранить в кэше несколько результатов для одного и того же запроса. Этого можно добиться с помощью настройки
[query&#95;cache&#95;tag](/ru/operations/settings/settings#query_cache_tag), которая служит меткой (или пространством имен) для записей кэша запросов. Кэш запросов
считает результаты одного и того же запроса с разными тегами разными.

Пример создания трех разных записей в кэше запросов для одного и того же запроса:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

Чтобы удалить из кэша запросов только записи с тегом `tag`, используйте оператор `SYSTEM CLEAR QUERY CACHE TAG 'tag'`.

<div id="subquery-caching">
  ## Кэширование подзапросов
</div>

По умолчанию параметр `use_query_cache` во внешнем запросе не применяется к подзапросам. Это означает, что для каждого подзапроса кэширование нужно включать явно:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

В этом примере кэшируется только результат внутреннего подзапроса. Внешний запрос не кэшируется.

Чтобы включить кэширование для всех подзапросов сразу, используйте настройку `query_cache_for_subqueries`:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Чтобы явно отключить кэширование для конкретного подзапроса, когда включено массовое распространение, задайте для этого подзапроса `use_query_cache = false`:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Записи кэша подзапросов видны в [system.query&#95;cache](system-tables/query_cache.md) при `is_subquery = 1`. Настройка `query_cache_ttl` также применяется к записям кэша подзапросов и может задаваться для каждого подзапроса.

ClickHouse читает данные таблицы блоками по [max&#95;block&#95;size](/ru/operations/settings/settings#max_block_size) строк. Из-за фильтрации, агрегации
и т. д. результирующие блоки обычно значительно меньше, чем &#39;max&#95;block&#95;size&#39;, но в некоторых случаях они могут быть и гораздо больше. Настройка
[query&#95;cache&#95;squash&#95;partial&#95;results](/ru/operations/settings/settings#query_cache_squash_partial_results) (включена по умолчанию) определяет, будут ли результирующие блоки
объединяться (если они очень маленькие) или разбиваться (если они большие) на блоки размера &#39;max&#95;block&#95;size&#39; перед вставкой в кэш результатов
запросов. Это снижает производительность записи в кэш запросов, но улучшает степень сжатия записей кэша и обеспечивает более естественную
гранулярность блоков, когда результаты запросов впоследствии отдаются из кэша запросов.

В результате кэш запросов хранит для каждого запроса несколько частичных
результирующих блоков. Хотя такое поведение является хорошим вариантом по умолчанию, его можно отключить с помощью настройки
[query&#95;cache&#95;squash&#95;partial&#95;results](/ru/operations/settings/settings#query_cache_squash_partial_results).

Кроме того, результаты запросов с недетерминированными функциями по умолчанию не кэшируются. К таким функциям относятся

* функции для доступа к словарям: [`dictGet()`](/ru/sql-reference/functions/ext-dict-functions) и т. д.
* [пользовательские функции](../sql-reference/statements/create/function.md) без тега `<deterministic>true</deterministic>` в их XML
  определении,
* функции, возвращающие текущую дату или время: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) и т. д.,
* функции, возвращающие случайные значения: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) и т. д.,
* функции, результат которых зависит от размера, порядка или внутренних фрагментов, используемых для обработки запросов:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) и т. д.,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) и т. д.,
* функции, зависящие от окружения: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/ru/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) и т. д.

Чтобы принудительно кэшировать результаты запросов с недетерминированными функциями, используйте настройку
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/ru/operations/settings/settings#query_cache_nondeterministic_function_handling).

Результаты запросов, которые затрагивают системные таблицы (например, [system.processes](system-tables/processes.md)&#96; или
[information&#95;schema.tables](system-tables/information_schema.md)), по умолчанию не кэшируются. Чтобы принудительно кэшировать результаты запросов с
системными таблицами, используйте настройку [query&#95;cache&#95;system&#95;table&#95;handling](/ru/operations/settings/settings#query_cache_system_table_handling).

Наконец, записи в кэше запросов не используются совместно разными пользователями из соображений безопасности. Например, пользователь A не должен иметь возможности обойти
политику построчного доступа к таблице, выполнив тот же запрос, что и другой пользователь B, для которого такая политика не задана. Однако при необходимости записи кэша можно
сделать доступными для других пользователей (то есть общими), указав настройку
[query&#95;cache&#95;share&#95;between&#95;users](/ru/operations/settings/settings#query_cache_share_between_users).

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Знакомьтесь: кэш запросов ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)