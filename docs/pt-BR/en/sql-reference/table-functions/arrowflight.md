---
description: 'Permite ler e gravar dados expostos por meio de um servidor Apache Arrow Flight.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

Permite ler e gravar dados expostos por meio de um servidor [Apache Arrow Flight](/pt-BR/interfaces/arrowflight).

**Sintaxe**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Argumentos**

* `host:port` — Endereço do servidor Arrow Flight. Se a porta for omitida, será usada a porta padrão `8815`. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Nome do conjunto de dados ou descritor disponível no servidor Arrow Flight. [String](../../sql-reference/data-types/string.md).
* `username` — Nome de usuário para autenticação HTTP básica. [String](../../sql-reference/data-types/string.md).
* `password` — Senha para autenticação HTTP básica. [String](../../sql-reference/data-types/string.md).

Se `username` e `password` não forem especificados, a autenticação não será usada (isso só funciona se o servidor Arrow Flight permitir acesso não autenticado).

A função também oferece suporte a [coleções nomeadas](/pt-BR/operations/named-collections) — consulte o [motor de tabela ArrowFlight](/pt-BR/engines/table-engines/integrations/arrowflight#named-collections) para ver a lista de parâmetros compatíveis.

**Valor retornado**

Um objeto de tabela que representa o conjunto de dados remoto. O esquema é inferido a partir do servidor Arrow Flight.

**Configurações**

* `arrow_flight_request_descriptor_type` — Controla como o nome do conjunto de dados é enviado ao servidor Flight. Valores: `path` (padrão) ou `command`. Consulte o [motor de tabela ArrowFlight](/pt-BR/engines/table-engines/integrations/arrowflight#settings) para mais detalhes.

**Exemplos**

Leitura de um servidor Arrow Flight remoto:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserindo dados em um servidor remoto Arrow Flight:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Usando uma coleção nomeada:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**Veja também**

* [motor de tabela ArrowFlight](/pt-BR/engines/table-engines/integrations/arrowflight)
* [Interface Arrow Flight](/pt-BR/interfaces/arrowflight)
* [Especificação Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)