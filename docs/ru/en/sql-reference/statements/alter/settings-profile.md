---
description: 'Документация по SETTINGS PROFILE'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
doc_type: 'reference'
---

Изменяет профили настроек.

Синтаксис:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

Предложение `ON CLUSTER` позволяет изменять профили настроек в кластере; см. [Распределённый DDL](../../../sql-reference/distributed-ddl.md).

<div id="replacing-vs-modifying">
  ## Замена и изменение настроек
</div>

`ALTER SETTINGS PROFILE` поддерживает два разных способа изменения настроек и родительских профилей, от которых наследуется профиль. Они работают по-разному, поэтому важно выбрать подходящий вариант.

<div id="replacing-form">
  ### Форма замены: `SETTINGS` / `INHERIT` без `ADD` / `MODIFY` / `DROP`
</div>

Предложение `SETTINGS` без `ADD`, `MODIFY` или `DROP` **полностью заменяет список настроек и все родительские профили** профиля на те, которые вы явно указали. Всё, что было задано ранее, но не перечислено, будет тихо удалено — без предупреждения.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

:::warning
Поскольку форма `SETTINGS` без модификаторов выполняет полную замену, ее использование для &quot;переопределения одного параметра&quot; в уже заполненном базовом профиле приведет к удалению всех остальных параметров (и всех родительских профилей) этого профиля. Если вам нужно изменить только один параметр, сохранив остальные, используйте инкрементальную форму `MODIFY`/`ADD`/`DROP`, описанную ниже.
:::

Это то же поведение, что и у `SETTINGS` в [`CREATE SETTINGS PROFILE`](../create/settings-profile.md): эта конструкция задает полный список настроек.

<div id="incremental-form">
  ### Инкрементальная форма: `ADD` / `MODIFY` / `DROP`
</div>

Ключевые слова `ADD`, `MODIFY` и `DROP` изменяют отдельные записи, оставляя всё остальное в профиле без изменений:

* `ADD SETTINGS variable = value [constraints]` — добавляет настройку, которой ещё нет.
* `MODIFY SETTINGS variable = value [constraints]` — заменяет запись одной настройки. Вся запись (значение и ограничения) перезаписывается целиком, поэтому, если вы хотите их сохранить, заново укажите `MIN`/`MAX`/`READONLY`/и т. д.
* `DROP SETTINGS variable [,...]` — удаляет перечисленные настройки.
* `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — добавляют или удаляют родительские (наследуемые) профили.
* `DROP ALL SETTINGS` / `DROP ALL PROFILES` — удаляют все настройки или все родительские профили.

Несколько таких секций можно объединить в одном операторе, например `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` — псевдоним для `MODIFY SETTINGS variable = value`. Этот вариант предусмотрен потому, что `SET` выглядит естественно, а ошибочно ввести заменяющую секцию `SETTINGS`, когда требуется инкрементальное изменение, — распространённая ошибка.

<div id="examples">
  ## Примеры
</div>

Переопределите один параметр, сохранив остальные параметры в уже заполненном профиле:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

Добавьте новый параметр с ограничением и удалите другой:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

Инкрементально управляйте родительскими профилями:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

Всегда проверяйте результат командой [`SHOW CREATE SETTINGS PROFILE`](../show.md):

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```

<div id="incremental-vs-full-replacement">
  ## Инкрементальные изменения vs полная замена
</div>

:::warning
Предложение `SETTINGS` без модификаторов **удаляет из профиля все существующие настройки и все наследуемые (родительские) профили** перед применением новых.
:::

Чтобы изменить только один параметр и сохранить остальные, используйте `ADD SETTINGS` или `MODIFY SETTINGS` (см. примеры ниже).

<div id="add-vs-modify">
  ## ADD vs MODIFY
</div>

И `ADD SETTINGS`, и `MODIFY SETTINGS` сохраняют остальные настройки в профиле, но по-разному обрабатывают существующую запись для *одной и той же* настройки:

* `ADD SETTINGS variable = value ...` сначала удаляет любую существующую запись для `variable`, а затем выполняет вставку новой. Поэтому эта команда **заменяет значение вместе со всеми ограничениями** этой настройки. Любые ранее заданные `MIN`, `MAX` или флаги доступности записи (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) для `variable`, которые вы не повторите, будут отброшены.
* `MODIFY SETTINGS variable = value ...` **выполняет слияние по полям**: команда переопределяет только те поля, которые вы действительно указываете (значение, `MIN`, `MAX` или флаг доступности записи), и сохраняет остальные поля этой настройки без изменений.

:::tip
Кратко: используйте `MODIFY SETTINGS`, если хотите изменить только один аспект настройки (например, только значение, сохранив существующий `MAX`); используйте `ADD SETTINGS`, если хотите переопределить настройку с нуля.
:::

<div id="examples">
  ## Примеры
</div>

Создайте профиль, который будет использоваться в примерах ниже:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

<div id="example-modify-settings">
  ### MODIFY SETTINGS
</div>

Добавьте или измените один параметр, оставив остальные без изменений:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Поскольку `MODIFY` объединяет поля по отдельности, изменение только значения параметра сохраняет существующие ограничения:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

<div id="example-add-settings">
  ### ADD SETTINGS
</div>

Добавьте настройку (сохранив остальные); если она уже существует, она будет полностью переопределена:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

В отличие от `MODIFY`, при повторном выполнении `ADD` только со значением ранее заданные ограничения для этого параметра удаляются:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

<div id="example-drop-settings">
  ### DROP SETTINGS
</div>

Удаляет одну или несколько указанных настроек:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Удалите сразу все настройки:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

<div id="example-profiles">
  ### Работа с наследуемыми профилями
</div>

Добавляйте или удаляйте родительские (наследуемые) профили, не изменяя собственные настройки профиля:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```