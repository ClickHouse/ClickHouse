---
description: 'Documentação do tipo de dado UUID no ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

Um Identificador Universalmente Único (UUID) é um valor de 16 bytes usado para identificar registros. Para informações detalhadas sobre UUIDs, consulte a [Wikipedia](https://en.wikipedia.org/wiki/Universally_unique_identifier).

Embora existam diferentes variantes de UUID, como UUIDv4 e UUIDv7 (veja [aqui](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), o ClickHouse não valida se os UUIDs inseridos estão em conformidade com uma variante específica.
Internamente, os UUIDs são tratados como uma sequência de 16 bytes aleatórios com representação [8-4-4-4-12](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) no nível SQL.

Exemplo de valor UUID:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

O UUID padrão é composto apenas por zeros. Ele é usado, por exemplo, quando um novo registro é inserido, mas nenhum valor é especificado para uma coluna UUID:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
Por motivos históricos, os UUIDs são ordenados pela segunda metade.

Embora isso seja aceitável para valores UUIDv4, pode prejudicar o desempenho de colunas UUIDv7 usadas em definições de índice primário (o uso em chaves de ordenação ou de partição não é um problema).
Mais especificamente, os valores UUIDv7 consistem em um timestamp na primeira metade e um contador na segunda metade.
Portanto, a ordenação de UUIDv7 em índices primários esparsos (ou seja, os primeiros valores de cada grânulo de índice) será feita pelo campo contador.
Supondo que os UUIDs fossem ordenados pela primeira metade (timestamp), espera-se que a etapa de análise do índice primário, no início das consultas, elimine todas as marcas de todas as partes, exceto uma.
No entanto, com a ordenação pela segunda metade (contador), espera-se que pelo menos uma marca seja retornada para todas as partes, o que leva a acessos desnecessários ao disco.
:::

Exemplo:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

Como alternativa, é possível converter o UUID em um timestamp extraído da metade final:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

Resultado (supondo que os mesmos dados sejam inseridos):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## Gerando UUIDs
</div>

O ClickHouse fornece a função [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) para gerar UUIDs aleatórios na versão 4.

<div id="usage-example">
  ## Exemplo de uso
</div>

**Exemplo 1**

Este exemplo demonstra a criação de uma tabela com uma coluna UUID e a inserção de um valor nessa tabela.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Exemplo 2**

Neste exemplo, nenhum valor é especificado para a coluna UUID quando o registro é inserido, ou seja, o valor UUID padrão é inserido:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## Restrições
</div>

O tipo de dado UUID oferece suporte apenas às funções que o tipo de dado [String](../../sql-reference/data-types/string.md) também suporta (por exemplo, [min](/pt-BR/sql-reference/aggregate-functions/reference/min), [max](/pt-BR/sql-reference/aggregate-functions/reference/max) e [count](/pt-BR/sql-reference/aggregate-functions/reference/count)).

O tipo de dado UUID não é compatível com operações aritméticas (por exemplo, [abs](/pt-BR/sql-reference/functions/arithmetic-functions#abs)) nem com funções de agregação, como [sum](/pt-BR/sql-reference/aggregate-functions/reference/sum) e [avg](/pt-BR/sql-reference/aggregate-functions/reference/avg).