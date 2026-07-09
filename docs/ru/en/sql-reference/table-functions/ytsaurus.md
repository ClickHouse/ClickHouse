---
description: 'Табличная функция позволяет читать данные из кластера YTsaurus.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # Табличная функция ytsaurus
</div>

<ExperimentalBadge />

Табличная функция позволяет читать данные из кластера YTsaurus.

<div id="syntax">
  ## Синтаксис
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
Это экспериментальная возможность, и в будущих версиях она может измениться несовместимым с предыдущими версиями образом.
Чтобы включить использование табличной функции YTsaurus,
используйте настройку [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/ru/operations/settings/settings#allow_experimental_ytsaurus_table_engine).
Введите команду `set allow_experimental_ytsaurus_table_function = 1`.
:::

<div id="arguments">
  ## Аргументы
</div>

* `http_proxy_url` — URL HTTP-прокси YTsaurus.
* `cypress_path` — путь Cypress к источнику данных.
* `oauth_token` — токен OAuth.
* `format` — [формат](/ru/interfaces/formats) источника данных.

**Возвращаемое значение**

Таблица с указанной структурой для чтения данных из указанного пути Cypress в кластере YTsaurus.

**См. также**

* [движок YTsaurus](/ru/engines/table-engines/integrations/ytsaurus.md)