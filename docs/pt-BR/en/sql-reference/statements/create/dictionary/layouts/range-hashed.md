---
slug: /sql-reference/statements/create/dictionary/layouts/range-hashed
title: 'tipos de layout de dicionário range_hashed'
sidebar_label: 'range_hashed'
sidebar_position: 5
description: 'Armazena um dicionário na memória usando uma tabela hash com intervalos ordenados de data/hora.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="range_hashed">
  ## range_hashed
</div>

O Dicionário é armazenado na memória na forma de uma tabela hash com um array ordenado de intervalos e seus respectivos valores.

Esse método de armazenamento funciona da mesma forma que o hashed e permite usar intervalos de data/hora (tipo numérico arbitrário) além da chave.

Exemplo: A tabela contém descontos para cada anunciante no formato:

```text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

Para usar uma amostra com intervalos de datas, defina os elementos `range_min` e `range_max` na [estrutura](../attributes.md#composite-key). Esses elementos devem incluir `name` e `type` (se `type` não for especificado, será usado o tipo padrão — Date). `type` pode ser qualquer tipo numérico (Date / DateTime / UInt64 / Int32 / outros).

:::note
Os valores de `range_min` e `range_max` devem caber no tipo `Int64`.
:::

Exemplo:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY discounts_dict (
        advertiser_id UInt64,
        discount_start_date Date,
        discount_end_date Date,
        amount Float64
    )
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'discounts'))
    LIFETIME(MIN 1 MAX 1000)
    LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
    RANGE(MIN discount_start_date MAX discount_end_date)
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
        <range_hashed>
            <!-- Estratégia para intervalos sobrepostos (min/max). Padrão: min (retorna um intervalo correspondente com o valor de min(range_min -> range_max)) -->
            <range_lookup_strategy>min</range_lookup_strategy>
        </range_hashed>
    </layout>
    <structure>
        <id>
            <name>advertiser_id</name>
        </id>
        <range_min>
            <name>discount_start_date</name>
            <type>Date</type>
        </range_min>
        <range_max>
            <name>discount_end_date</name>
            <type>Date</type>
        </range_max>
        ...
    ```
  </TabItem>
</Tabs>

<br />

Para trabalhar com esses dicionários, você precisa passar um argumento adicional para a função `dictGet`, com base no qual um intervalo é selecionado:

```sql
dictGet('dict_name', 'attr_name', id, date)
```

Exemplo de consulta:

```sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

Esta função retorna o valor dos `id`s especificados e o intervalo de datas que inclui a data informada.

Detalhes do algoritmo:

* Se o `id` não for encontrado ou se não for encontrado um intervalo para o `id`, retorna o valor padrão do tipo do atributo.
* Se houver intervalos sobrepostos e `range_lookup_strategy=min`, retorna um intervalo correspondente com o menor `range_min`; se vários intervalos forem encontrados, retorna um intervalo com o menor `range_max`; se ainda assim vários intervalos forem encontrados (vários intervalos tiverem o mesmo `range_min` e `range_max`), retorna um intervalo aleatório entre eles.
* Se houver intervalos sobrepostos e `range_lookup_strategy=max`, retorna um intervalo correspondente com o maior `range_min`; se vários intervalos forem encontrados, retorna um intervalo com o maior `range_max`; se ainda assim vários intervalos forem encontrados (vários intervalos tiverem o mesmo `range_min` e `range_max`), retorna um intervalo aleatório entre eles.
* Se `range_max` for `NULL`, o intervalo será aberto. `NULL` é tratado como o maior valor possível. Para `range_min`, `1970-01-01` ou `0` (-MAX&#95;INT) podem ser usados como valor aberto.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY somedict(
        Abcdef UInt64,
        StartTimeStamp UInt64,
        EndTimeStamp UInt64,
        XXXType String DEFAULT ''
    )
    PRIMARY KEY Abcdef
    RANGE(MIN StartTimeStamp MAX EndTimeStamp)
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <clickhouse>
        <dictionary>
            ...

            <layout>
                <range_hashed />
            </layout>

            <structure>
                <id>
                    <name>Abcdef</name>
                </id>
                <range_min>
                    <name>StartTimeStamp</name>
                    <type>UInt64</type>
                </range_min>
                <range_max>
                    <name>EndTimeStamp</name>
                    <type>UInt64</type>
                </range_max>
                <attribute>
                    <name>XXXType</name>
                    <type>String</type>
                    <null_value />
                </attribute>
            </structure>

        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Exemplo de configuração com intervalos sobrepostos e intervalos em aberto:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- two ranges are matching, range_min 2015-01-15 (0.2) is bigger than 2015-01-01 (0.1)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- two ranges are matching, range_min 2015-01-04 (0.4) is bigger than 2015-01-01 (0.3)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- two ranges are matching, range_min are equal, 2015-01-15 (0.5) is bigger than 2015-01-10 (0.6)
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- the only one range is matching: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- two ranges are matching, range_min 2015-01-01 (0.1) is less than 2015-01-15 (0.2)
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- two ranges are matching, range_min 2015-01-01 (0.3) is less than 2015-01-04 (0.4)
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- two ranges are matching, range_min are equal, 2015-01-10 (0.6) is less than 2015-01-15 (0.5)
└─────┘
```

<div id="complex_key_range_hashed">
  ## complex_key_range_hashed
</div>

O Dicionário é armazenado na memória na forma de uma tabela hash com um array ordenado de intervalos e seus respectivos valores (consulte [range&#95;hashed](#range_hashed)). Esse tipo de armazenamento é usado com [chaves](../attributes.md#composite-key) compostas.

Exemplo de configuração:

```sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```