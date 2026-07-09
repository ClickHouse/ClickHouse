---
description: 'Documentação das funções de conversão de tipos'
sidebar_label: 'Conversão de tipos'
slug: /sql-reference/functions/type-conversion-functions
title: 'Funções de conversão de tipos'
doc_type: 'referência'
---

<div id="common-issues-with-data-conversion">
  ## Problemas comuns com conversão de dados
</div>

O ClickHouse geralmente segue o [mesmo comportamento de programas em C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

As funções `to<type>` e [cast](#CAST) se comportam de maneira diferente em alguns casos, por exemplo no caso de [LowCardinality](../data-types/lowcardinality.md): [cast](#CAST) remove a propriedade [LowCardinality](../data-types/lowcardinality.md), enquanto as funções `to<type>` não. O mesmo acontece com [Nullable](../data-types/nullable.md); esse comportamento não é compatível com o padrão SQL e pode ser alterado usando a configuração [cast&#95;keep&#95;nullable](../../operations/settings/settings.md/#cast_keep_nullable).

:::note
Esteja ciente da possível perda de dados se valores de um tipo de dado forem convertidos para um tipo de dado menor (por exemplo, de `Int64` para `Int32`) ou entre
tipos de dados incompatíveis (por exemplo, de `String` para `Int`). Verifique cuidadosamente se o resultado é o esperado.
:::

Exemplo:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

<div id="to-string-functions">
  ## Notas sobre as funções `toString`
</div>

A família de funções `toString` permite converter entre números, strings (mas não strings de tamanho fixo), datas e datas com hora.
Todas essas funções aceitam um argumento.

* Ao converter para ou de uma string, o valor é formatado ou interpretado usando as mesmas regras do formato TabSeparated (e de quase todos os outros formatos de texto). Se a string não puder ser interpretada, uma exceção será gerada e a solicitação será cancelada.
* Ao converter datas em números ou vice-versa, a data corresponde ao número de dias desde o início do Unix epoch.
* Ao converter datas com hora em números ou vice-versa, a data com hora corresponde ao número de segundos desde o início do Unix epoch.
* A função `toString` para o argumento `DateTime` pode receber um segundo argumento String contendo o nome do fuso horário, por exemplo: `Europe/Amsterdam`. Nesse caso, a hora é formatada de acordo com o fuso horário especificado.

<div id="to-date-and-date-time-functions">
  ## Observações sobre as funções `toDate`/`toDateTime`
</div>

Os formatos de data e de data e hora das funções `toDate`/`toDateTime` são definidos da seguinte forma:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

Como exceção, ao converter tipos numéricos UInt32, Int32, UInt64 ou Int64 para Date, se o número for maior ou igual a 65536, ele será interpretado como um Unix timestamp (e não como um número de dias) e arredondado para a data.
Isso dá suporte ao caso comum de escrever `toDate(unix_timestamp)`, que, de outra forma, geraria um erro e exigiria a forma mais trabalhosa `toDate(toDateTime(unix_timestamp))`.

A conversão entre uma data e uma data com hora é feita de forma natural: adicionando uma hora nula ou removendo a hora.

A conversão entre tipos numéricos usa as mesmas regras das atribuições entre diferentes tipos numéricos em C++.

**Exemplo**

```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

Consulte também a função [`toUnixTimestamp`](/pt-BR/sql-reference/functions/date-time-functions#toUnixTimestamp).

{/* 
  O conteúdo interno das tags abaixo é substituído durante a compilação do framework de documentação por 
  documentação gerada a partir de system.functions. Não modifique nem remova as tags.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }