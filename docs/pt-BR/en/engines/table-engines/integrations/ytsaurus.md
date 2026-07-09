---
description: 'Motor de tabela que permite importar dados de um cluster YTsaurus.'
sidebar_label: 'YTsaurus'
sidebar_position: 185
slug: /engines/table-engines/integrations/ytsaurus
title: 'Motor de tabela YTsaurus'
keywords: ['YTsaurus', 'motor de tabela']
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-engine">
  # motor de tabela YTsaurus
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

O motor de tabela YTsaurus permite importar dados de um cluster YTsaurus.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = YTsaurus('http_proxy_url', 'cypress_path', 'oauth_token')
```

:::info
Esta é uma funcionalidade experimental que pode mudar de maneiras incompatíveis com versões anteriores em lançamentos futuros.
Habilite o uso do mecanismo de tabela YTsaurus
com a configuração [`allow_experimental_ytsaurus_table_engine`](/pt-BR/operations/settings/settings#allow_experimental_ytsaurus_table_engine).

Você pode fazer isso usando:

`SET allow_experimental_ytsaurus_table_engine = 1`.
:::

**Parâmetros do mecanismo**

* `http_proxy_url` — URL do proxy HTTP do YTsaurus.
* `cypress_path` — caminho do Cypress para a origem dos dados.
* `oauth_token` — token OAuth.

<div id="usage-example">
  ## Exemplo de uso
</div>

Mostra uma consulta para criar a tabela YTsaurus:

```sql title="Query"
SHOW CREATE TABLE yt_saurus;
```

```sql title="Response"
CREATE TABLE yt_saurus
(
    `a` UInt32,
    `b` String
)
ENGINE = YTsaurus('http://localhost:8000', '//tmp/table', 'password')
```

Para obter os dados da tabela, execute:

```sql title="Query"
SELECT * FROM yt_saurus;
```

```response title="Response"
 ┌──a─┬─b──┐
 │ 10 │ 20 │
 └────┴────┘
```

<div id="data-types">
  ## Tipos de dados
</div>

<div id="primitive-data-types">
  ### Tipos de dados primitivos
</div>

| tipo de dado do YTsaurus   | tipo de dado do ClickHouse   |
| -------------------------- | ---------------------------- |
| `int8`                     | `Int8`                       |
| `int16`                    | `Int16`                      |
| `int32`                    | `Int32`                      |
| `int64`                    | `Int64`                      |
| `uint8`                    | `UInt8`                      |
| `uint16`                   | `UInt16`                     |
| `uint32`                   | `UInt32`                     |
| `uint64`                   | `UInt64`                     |
| `float`                    | `Float32`                    |
| `double`                   | `Float64`                    |
| `boolean`                  | `Bool`                       |
| `string`                   | `String`                     |
| `utf8`                     | `String`                     |
| `json`                     | `JSON`                       |
| `yson(type_v3)`            | `JSON`                       |
| `uuid`                     | `UUID`                       |
| `date32`                   | `Date`(ainda não compatível) |
| `datetime64`               | `Int64`                      |
| `timestamp64`              | `Int64`                      |
| `interval64`               | `Int64`                      |
| `date`                     | `Date`(ainda não compatível) |
| `datetime`                 | `DateTime`                   |
| `timestamp`                | `DateTime64(6)`              |
| `interval`                 | `UInt64`                     |
| `any`                      | `String`                     |
| `null`                     | `Nothing`                    |
| `void`                     | `Nothing`                    |
| `T` com `required = False` | `Nullable(T)`                |

<div id="composite-data-types">
  ### Tipos compostos
</div>

| Tipo de dados do YTsaurus | Tipo de dados do ClickHouse |
| ------------------------- | --------------------------- |
| `decimal`                 | `Decimal`                   |
| `optional`                | `Nullable`                  |
| `list`                    | `Array`                     |
| `struct`                  | `NamedTuple`                |
| `tuple`                   | `Tuple`                     |
| `variant`                 | `Variant`                   |
| `dict`                    | &#96;Array(Tuple(...))      |
| `tagged`                  | `T`                         |

**Ver também**

* função de tabela [ytsaurus](../../../sql-reference/table-functions/ytsaurus.md)
* [schema de dados do ytsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/static-schema)
* [tipos de dados do ytsaurus](https://ytsaurus.tech/docs/en/user-guide/storage/data-types)