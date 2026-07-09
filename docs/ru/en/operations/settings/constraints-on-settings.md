---
description: 'Ограничения на настройки можно задать в разделе `profiles` файла
  конфигурации `user.xml`; они запрещают пользователям изменять некоторые настройки
  с помощью запроса `SET`.'
sidebar_label: 'Ограничения на настройки'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: 'Ограничения на настройки'
doc_type: 'reference'
---

<div id="overview">
  ## Обзор
</div>

В ClickHouse «ограничения» для настроек — это ограничения и правила,
которые можно задавать для настроек. Эти ограничения помогают поддерживать
стабильность, безопасность и предсказуемое поведение базы данных.

<div id="defining-constraints">
  ## Определение ограничений
</div>

Ограничения на настройки можно задать в разделе `profiles` файла конфигурации `user.xml`.
Они не позволяют пользователям изменять некоторые настройки с помощью
оператора [`SET`](/ru/sql-reference/statements/set).

Ограничения задаются следующим образом:

```xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
      <setting_name_6>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <disallowed>value1</disallowed>
        <disallowed>value2</disallowed>
        <disallowed>value3</disallowed>
        <changeable_in_readonly/>
      </setting_name_6>
    </constraints>
  </user_name>
</profiles>
```

Если пользователь пытается нарушить ограничения, возникает исключение, и
настройка остаётся без изменений.

<div id="types-of-constraints">
  ## Типы ограничений
</div>

В ClickHouse поддерживается несколько типов ограничений:

* `min`
* `max`
* `disallowed`
* `readonly` (с alias `const`)
* `changeable_in_readonly`

Ограничения `min` и `max` задают верхнюю и нижнюю границы для числовой
настройки и могут использоваться как вместе, так и по отдельности.

Ограничение `disallowed` можно использовать, чтобы указать конкретные значения,
которые не должны быть разрешены для определенной настройки.

Ограничение `readonly` или `const` означает, что пользователь вообще не может изменять
соответствующую настройку.

Ограничение типа `changeable_in_readonly` позволяет пользователям изменять настройку
в пределах диапазона `min`/`max`, даже если настройка `readonly` установлена в `1`;
в противном случае в режиме `readonly=1` изменять настройки нельзя.

:::note
`changeable_in_readonly` поддерживается, только если `settings_constraints_replace_previous`
включен:

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

:::

<div id="multiple-constraint-profiles">
  ## Несколько профилей ограничений
</div>

Если для пользователя активно несколько профилей, ограничения объединяются.
Процесс объединения зависит от `settings_constraints_replace_previous`:

* **true** (рекомендуется): ограничения для одной и той же настройки заменяются при
  объединении, так что используется последнее ограничение, а все предыдущие игнорируются.
  Это касается и полей, которые не заданы в новом ограничении.
* **false** (по умолчанию): ограничения для одной и той же настройки объединяются так,
  что каждый незаданный тип ограничения берётся из предыдущего профиля, а каждый
  заданный тип ограничения заменяется значением из нового профиля.

<div id="read-only">
  ## Режим только для чтения
</div>

Режим только для чтения включается настройкой `readonly`; её не следует путать
с типом ограничения `readonly`:

* `readonly=0`: Ограничения только для чтения отсутствуют.
* `readonly=1`: Разрешены только запросы на чтение, а настройки нельзя изменять,
  если не задан `changeable_in_readonly`.
* `readonly=2`: Разрешены только запросы на чтение, но настройки можно изменять,
  кроме самой настройки `readonly`.

<div id="example-read-only">
  ### Пример
</div>

Пусть файл `users.xml` содержит следующие строки:

```xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

Все следующие запросы сгенерируют исключения:

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
Профиль `default` обрабатывается особым образом: все ограничения, заданные для
профиля `default`, становятся ограничениями по умолчанию и потому применяются ко всем пользователям,
пока для этих пользователей они не будут явно переопределены.
:::

<div id="constraints-on-merge-tree-settings">
  ## Ограничения на настройки MergeTree
</div>

Для [настроек MergeTree](merge-tree-settings.md) можно задавать ограничения.
Эти ограничения применяются при создании таблицы с движком MergeTree
или при изменении параметров ее хранилища.

При указании в разделе `<constraints>` к имени настройки MergeTree необходимо
добавлять префикс `merge_tree_`.

<div id="example-read-only">
  ### Пример
</div>

Вы можете запретить создание новых таблиц с явно заданной `storage_policy`

```xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```