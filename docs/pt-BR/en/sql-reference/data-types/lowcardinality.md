---
description: 'Documentação da otimização LowCardinality para colunas String'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

Altera a representação interna de outros tipos de dados para usar codificação por dicionário.

<div id="syntax">
  ## Sintaxe
</div>

```sql
LowCardinality(data_type)
```

**Parâmetros**

* `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) e números, exceto [Decimal](../../sql-reference/data-types/decimal.md). `LowCardinality` não é eficiente para alguns tipos de dados; consulte a descrição da configuração [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types).

<div id="description">
  ## Descrição
</div>

`LowCardinality` é uma superestrutura que altera o método de armazenamento dos dados e as regras de processamento de dados. O ClickHouse aplica [codificação por dicionário](https://en.wikipedia.org/wiki/Dictionary_coder) às colunas `LowCardinality`. Trabalhar com dados codificados por dicionário aumenta significativamente o desempenho de consultas [SELECT](../../sql-reference/statements/select/index.md) em muitas aplicações.

A eficiência do uso do tipo de dado `LowCardinality` depende da diversidade dos dados. Se um dicionário contiver menos de 10.000 valores distintos, o ClickHouse geralmente apresenta maior eficiência na leitura e no armazenamento de dados. Se um dicionário contiver mais de 100.000 valores distintos, o ClickHouse pode ter um desempenho inferior em comparação com o uso de tipos de dados comuns.

Considere usar `LowCardinality` em vez de [Enum](../../sql-reference/data-types/enum.md) ao trabalhar com strings. `LowCardinality` oferece mais flexibilidade e, muitas vezes, apresenta a mesma eficiência ou até maior.

<div id="example">
  ## Exemplo
</div>

Crie uma tabela com uma coluna `LowCardinality`:

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## Configurações e funções relacionadas
</div>

Configurações:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/pt-BR/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

Funções:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Otimizando o ClickHouse com esquemas e codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* Blog: [Trabalhando com dados de séries temporais no ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [Otimização de String (vídeo da apresentação em russo)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [Slides em inglês](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)