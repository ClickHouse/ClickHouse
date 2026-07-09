---
description: 'Документация по политике маскирования'
sidebar_label: 'MASKING POLICY'
sidebar_position: 42
slug: /sql-reference/statements/create/masking-policy
title: 'CREATE MASKING POLICY'
doc_type: 'reference'
---

import CloudOnlyBadge from '@theme/badges/CloudOnlyBadge';

<CloudOnlyBadge />

Создаёт политику маскирования, которая позволяет динамически преобразовывать или маскировать значения столбцов для определённых пользователей или ролей при выполнении запроса к таблице.

:::tip
Политики маскирования обеспечивают защиту данных на уровне столбца, преобразуя конфиденциальные данные во время выполнения запроса без изменения хранимых данных.
:::

Синтаксис:

```sql
CREATE MASKING POLICY [IF NOT EXISTS | OR REPLACE] policy_name ON [database.]table
    UPDATE column1 = expression1 [, column2 = expression2 ...]
    [WHERE condition]
    TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}
    [PRIORITY priority_number]
```

<div id="update-clause">
  ## Предложение UPDATE
</div>

Клауза `UPDATE` определяет, какие столбцы нужно маскировать и как их преобразовывать. В одной политике можно маскировать несколько столбцов.

Примеры:

* Простая маскировка: `UPDATE email = '***masked***'`
* Частичная маскировка: `UPDATE email = concat(substring(email, 1, 3), '***@***.***')`
* Маскирование на основе хэша: `UPDATE email = concat('masked_', substring(hex(cityHash64(email)), 1, 8))`
* Несколько столбцов: `UPDATE email = '***@***.***', phone = '***-***-****'`

<div id="where-clause">
  ## Предложение WHERE
</div>

Необязательное предложение `WHERE` позволяет выполнять условное маскирование в зависимости от значений в строках. Маскирование будет применяться только к строкам, соответствующим условию.

Пример:

```sql
CREATE MASKING POLICY mask_high_salaries ON employees
UPDATE salary = 0
WHERE salary > 100000
TO analyst;
```

<div id="to-clause">
  ## Предложение TO
</div>

В разделе `TO` укажите, к каким пользователям и ролям должна применяться политика.

* `TO user1, user2`: Применяется к конкретным пользователям/ролям
* `TO ALL`: Применяется ко всем пользователям
* `TO ALL EXCEPT user1, user2`: Применяется ко всем пользователям, кроме указанных

:::note
В отличие от политик построчного доступа, политики маскирования не влияют на пользователей, к которым они не применяются. Если к пользователю не применяется ни одна политика маскирования, он видит исходные данные.
:::

<div id="priority-clause">
  ## Предложение PRIORITY
</div>

Когда к одному и тому же столбцу для пользователя применяются несколько политик маскирования, условие `PRIORITY` определяет порядок их применения. Политики применяются в порядке от более высокого приоритета к более низкому.

Приоритет по умолчанию — 0. Политики с одинаковым приоритетом применяются в неопределённом порядке.

Пример:

```sql
-- Applied second (lower priority)
CREATE MASKING POLICY mask1 ON users
UPDATE email = 'low@priority.com'
TO analyst
PRIORITY 1;

-- Applied first (higher priority)
CREATE MASKING POLICY mask2 ON users
UPDATE email = 'high@priority.com'
TO analyst
PRIORITY 10;

-- analyst sees 'low@priority.com' because it's applied last
```

:::note Вопросы производительности

* Политики маскирования могут влиять на производительность запросов в зависимости от сложности выражений
* Для таблиц с активными политиками маскирования некоторые оптимизации могут быть недоступны
  :::