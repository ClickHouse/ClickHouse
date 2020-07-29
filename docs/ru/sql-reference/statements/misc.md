# Прочие виды запросов {#prochie-vidy-zaprosov}

## ATTACH {#attach}

Запрос полностью аналогичен запросу `CREATE`, но:

-   вместо слова `CREATE` используется слово `ATTACH`;
-   запрос не создаёт данные на диске, а предполагает, что данные уже лежат в соответствующих местах, и всего лишь добавляет информацию о таблице на сервер. После выполнения запроса `ATTACH` сервер будет знать о существовании таблицы.

Если таблица перед этим была отсоединена (`DETACH`), т.е. её структура известна, можно использовать сокращенную форму записи без определения структуры.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Этот запрос используется при старте сервера. Сервер хранит метаданные таблиц в виде файлов с запросами `ATTACH`, которые он просто исполняет при запуске (за исключением системных таблиц, которые явно создаются на сервере).

## CHECK TABLE {#check-table}

Проверяет таблицу на повреждение данных.

``` sql
CHECK TABLE [db.]name
```

Запрос `CHECK TABLE` сравнивает текущие размеры файлов (в которых хранятся данные из колонок) с ожидаемыми значениями. Если значения не совпадают, данные в таблице считаются поврежденными. Искажение возможно, например, из-за сбоя при записи данных.

Ответ содержит колонку `result`, содержащую одну строку с типом [Boolean](../../sql-reference/data-types/boolean.md). Допустимые значения:

-   0 - данные в таблице повреждены;
-   1 - данные не повреждены.

Запрос `CHECK TABLE` поддерживает следующие движки таблиц:

-   [Log](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [Семейство MergeTree](../../engines/table-engines/mergetree-family/index.md)

При попытке выполнить запрос с таблицами с другими табличными движками, ClickHouse генерирует исключение.

В движках `*Log` не предусмотрено автоматическое восстановление данных после сбоя. Используйте запрос `CHECK TABLE`, чтобы своевременно выявлять повреждение данных.

Для движков из семейства `MergeTree` запрос `CHECK TABLE` показывает статус проверки для каждого отдельного куска данных таблицы на локальном сервере.

**Что делать, если данные повреждены**

В этом случае можно скопировать оставшиеся неповрежденные данные в другую таблицу. Для этого:

1.  Создайте новую таблицу с такой же структурой, как у поврежденной таблицы. Для этого выполните запрос `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  Установите значение параметра [max\_threads](../../operations/settings/settings.md#settings-max_threads) в 1. Это нужно для того, чтобы выполнить следующий запрос в одном потоке. Установить значение параметра можно через запрос: `SET max_threads = 1`.
3.  Выполните запрос `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. В результате неповрежденные данные будут скопированы в другую таблицу. Обратите внимание, будут скопированы только те данные, которые следуют до поврежденного участка.
4.  Перезапустите `clickhouse-client`, чтобы вернуть предыдущее значение параметра `max_threads`.

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Возвращает описание столбцов таблицы.

Результат запроса содержит столбцы (все столбцы имеют тип String):

-   `name` — имя столбца таблицы;
-   `type`— тип столбца;
-   `default_type` — в каком виде задано [выражение для значения по умолчанию](create.md#create-default-values): `DEFAULT`, `MATERIALIZED` или `ALIAS`. Столбец содержит пустую строку, если значение по умолчанию не задано.
-   `default_expression` — значение, заданное в секции `DEFAULT`;
-   `comment_expression` — комментарий к столбцу.

Вложенные структуры данных выводятся в «развёрнутом» виде. То есть, каждый столбец - по отдельности, с именем через точку.

## DETACH {#detach-statement}

Удаляет из сервера информацию о таблице name. Сервер перестаёт знать о существовании таблицы.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Но ни данные, ни метаданные таблицы не удаляются. При следующем запуске сервера, сервер прочитает метаданные и снова узнает о таблице.
Также, «отцепленную» таблицу можно прицепить заново запросом `ATTACH` (за исключением системных таблиц, для которых метаданные не хранятся).

Запроса `DETACH DATABASE` нет.

## DROP {#drop}

Запрос имеет два вида: `DROP DATABASE` и `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет таблицу.
Если указано `IF EXISTS` - не выдавать ошибку, если таблица не существует или база данных не существует.

## DROP USER {#drop-user-statement}

Удаляет пользователя.

### Синтаксис {#drop-user-syntax}

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP ROLE {#drop-role-statement}

Удаляет роль.

При удалении роль отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-role-syntax}

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

Удаляет политику доступа к строкам.

При удалении политика отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```


## DROP QUOTA {#drop-quota-statement}

Удаляет квоту.

При удалении квота отзывается у всех объектов системы доступа, которым она присвоена.

### Синтаксис {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

Удаляет профиль настроек.

При удалении профиль отзывается у всех объектов системы доступа, которым он присвоен.

### Синтаксис {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```


## EXISTS {#exists}

``` sql
EXISTS [TEMPORARY] TABLE [db.]name [INTO OUTFILE filename] [FORMAT format]
```

Возвращает один столбец типа `UInt8`, содержащий одно значение - `0`, если таблицы или БД не существует и `1`, если таблица в указанной БД существует.

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Пытается принудительно остановить исполняющиеся в данный момент запросы.
Запросы для принудительной остановки выбираются из таблицы system.processes с помощью условия, указанного в секции `WHERE` запроса `KILL`.

Примеры

``` sql
-- Принудительно останавливает все запросы с указанным query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Синхронно останавливает все запросы пользователя 'username':
KILL QUERY WHERE user='username' SYNC
```

Readonly-пользователи могут останавливать только свои запросы.

По умолчанию используется асинхронный вариант запроса (`ASYNC`), который не дожидается подтверждения остановки запросов.

Синхронный вариант (`SYNC`) ожидает остановки всех запросов и построчно выводит информацию о процессах по ходу их остановки.
Ответ содержит колонку `kill_status`, которая может принимать следующие значения:

1.  ‘finished’ - запрос был успешно остановлен;
2.  ‘waiting’ - запросу отправлен сигнал завершения, ожидается его остановка;
3.  остальные значения описывают причину невозможности остановки запроса.

Тестовый вариант запроса (`TEST`) только проверяет права пользователя и выводит список запросов для остановки.

## KILL MUTATION {#kill-mutation-statement}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Пытается остановить выполняющиеся в данные момент [мутации](alter.md#mutations). Мутации для остановки выбираются из таблицы [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) с помощью условия, указанного в секции `WHERE` запроса `KILL`.

Тестовый вариант запроса (`TEST`) только проверяет права пользователя и выводит список запросов для остановки.

Примеры:

``` sql
-- Останавливает все мутации одной таблицы:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Останавливает конкретную мутацию:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

Запрос полезен в случаях, когда мутация не может выполниться до конца (например, если функция в запросе мутации бросает исключение на данных таблицы).

Данные, уже изменённые мутацией, остаются в таблице (отката на старую версию данных не происходит).

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

Запрос пытается запустить внеплановый мёрж кусков данных для таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Другие движки таблиц не поддерживаются.

Если `OPTIMIZE` применяется к таблицам семейства [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md), ClickHouse создаёт задачу на мёрж и ожидает её исполнения на всех узлах (если активирована настройка `replication_alter_partitions_sync`).

-   Если `OPTIMIZE` не выполняет мёрж по любой причине, ClickHouse не оповещает об этом клиента. Чтобы включить оповещения, используйте настройку [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop).
-   Если указать `PARTITION`, то оптимизация выполняется только для указанной партиции. [Как задавать имя партиции в запросах](alter.md#alter-how-to-specify-part-expr).
-   Если указать `FINAL`, то оптимизация выполняется даже в том случае, если все данные уже лежат в одном куске.
-   Если указать `DEDUPLICATE`, то произойдет схлопывание полностью одинаковых строк (сравниваются значения во всех колонках), имеет смысл только для движка MergeTree.

!!! warning "Внимание"
    Запрос `OPTIMIZE` не может устранить причину появления ошибки «Too many parts».

## RENAME {#misc_operations-rename}

Переименовывает одну или несколько таблиц.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Переименовывание таблицы является лёгкой операцией. Если вы указали после `TO` другую базу данных, то таблица будет перенесена в эту базу данных. При этом, директории с базами данных должны быть расположены в одной файловой системе (иначе возвращается ошибка). В случае переименования нескольких таблиц в одном запросе — это неатомарная операция,  может выполнится частично, запросы в других сессиях могут получить ошибку `Table ... doesn't exist...`.

## SET {#query-set}

``` sql
SET param = value
```

Устанавливает значение `value` для [настройки](../../operations/settings/index.md) `param` в текущей сессии. [Конфигурационные параметры сервера](../../operations/server-configuration-parameters/settings.md) нельзя изменить подобным образом.

Можно одним запросом установить все настройки из заданного профиля настроек.

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

Подробности смотрите в разделе [Настройки](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

Активирует роли для текущего пользователя.

### Синтаксис {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

Устанавливает роли по умолчанию для пользователя.

Роли по умолчанию активируются автоматически при входе пользователя. Ролями по умолчанию могут быть установлены только ранее назначенные роли. Если роль не назначена пользователю, ClickHouse выбрасывает исключение.


### Синтаксис {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```


### Примеры {#set-default-role-examples}

Установить несколько ролей по умолчанию для пользователя:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Установить ролями по умолчанию все назначенные пользователю роли:

``` sql
SET DEFAULT ROLE ALL TO user
```

Удалить роли по умолчанию для пользователя:

``` sql
SET DEFAULT ROLE NONE TO user
```

Установить ролями по умолчанию все назначенные пользователю роли за исключением указанных:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

Удаляет все данные из таблицы. Если условие `IF EXISTS` не указано, запрос вернет ошибку, если таблицы не существует.

Запрос `TRUNCATE` не поддерживается для следующих движков: [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) и [Null](../../engines/table-engines/special/null.md).

## USE {#use}

``` sql
USE db
```

Позволяет установить текущую базу данных для сессии.
Текущая база данных используется для поиска таблиц, если база данных не указана в запросе явно через точку перед именем таблицы.
При использовании HTTP протокола запрос не может быть выполнен, так как понятия сессии не существует.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/misc/) <!--hide-->
