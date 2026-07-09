---
description: 'Configuração da chave e dos atributos do dicionário'
sidebar_label: 'Atributos'
sidebar_position: 2
slug: /sql-reference/statements/create/dictionary/attributes
title: 'Atributos de dicionário'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

<CloudDetails />

A cláusula `structure` descreve a chave do dicionário e os campos disponíveis para consultas.

Descrição em XML:

```xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- Attribute parameters -->
        </attribute>

        ...

    </structure>
</dictionary>
```

Os atributos são descritos nos seguintes elementos:

* `<id>` — coluna-chave
* `<attribute>` — coluna de dados: pode haver vários atributos.

Consulta DDL:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

Os atributos são descritos no corpo da consulta:

* `PRIMARY KEY` — coluna-chave
* `AttrName AttrType` — coluna de dados. Pode haver múltiplos atributos.

<div id="key">
  ## Chave
</div>

O ClickHouse oferece suporte aos seguintes tipos de chaves:

* Chave numérica. `UInt64`. Definida na tag `<id>` ou usando a palavra-chave `PRIMARY KEY`.
* Chave composta. Conjunto de valores de tipos diferentes. Definida na tag `<key>` ou usando a palavra-chave `PRIMARY KEY`.

Uma estrutura XML pode conter `<id>` ou `<key>`. A consulta DDL deve conter apenas uma `PRIMARY KEY`.

:::note
Você não deve descrever a chave como um atributo.
:::

<div id="numeric-key">
  ### Chave numérica
</div>

Tipo: `UInt64`.

Exemplo de configuração:

```xml
<id>
    <name>Id</name>
</id>
```

Campos de configuração:

* `name` – O nome da coluna com as chaves.

Para a consulta DDL:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

* `PRIMARY KEY` – O nome da coluna que contém as chaves.

<div id="composite-key">
  ### Chave composta
</div>

A chave pode ser uma `tuple` composta por campos de qualquer tipo. O [layout](./layouts/) nesse caso deve ser `complex_key_hashed` ou `complex_key_cache`.

:::tip
Uma chave composta pode ser formada por um único elemento. Isso possibilita, por exemplo, usar uma string como chave.
:::

A estrutura da chave é definida no elemento `<key>`. Os campos da chave são especificados no mesmo formato dos [atributos](#attributes) do dicionário. Exemplo:

```xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

ou

```sql
CREATE DICTIONARY (
    field1 String,
    field2 UInt32
    ...
)
PRIMARY KEY field1, field2
...
```

Em uma consulta à função `dictGet*`, uma tupla é usada como chave. Exemplo: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`.

Quando a chave composta consiste em um único atributo, o valor da chave pode ser passado diretamente, sem envolvê-lo em `tuple`. Por exemplo, tanto `dictGetString('dict_name', 'attr_name', 'key')` quanto `dictGetString('dict_name', 'attr_name', tuple('key'))` são válidos.

<div id="attributes">
  ## Atributos
</div>

Exemplo de configuração:

```xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

or

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

Campos de configuração:

| Tag                                                | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Obrigatório |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `name`                                             | Nome da coluna.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Sim         |
| `type`                                             | Tipo de dado do ClickHouse: [UInt8](../../../data-types/int-uint.md), [UInt16](../../../data-types/int-uint.md), [UInt32](../../../data-types/int-uint.md), [UInt64](../../../data-types/int-uint.md), [Int8](../../../data-types/int-uint.md), [Int16](../../../data-types/int-uint.md), [Int32](../../../data-types/int-uint.md), [Int64](../../../data-types/int-uint.md), [Float32](../../../data-types/float.md), [Float64](../../../data-types/float.md), [UUID](../../../data-types/uuid.md), [Decimal32](../../../data-types/decimal.md), [Decimal64](../../../data-types/decimal.md), [Decimal128](../../../data-types/decimal.md), [Decimal256](../../../data-types/decimal.md),[Date](../../../data-types/date.md), [Date32](../../../data-types/date32.md), [DateTime](../../../data-types/datetime.md), [DateTime64](../../../data-types/datetime64.md), [String](../../../data-types/string.md), [Array](../../../data-types/array.md).<br />O ClickHouse tenta converter o valor do dicionário para o tipo de dado especificado. Por exemplo, no MySQL, o campo pode ser `TEXT`, `VARCHAR` ou `BLOB` na tabela de origem, mas pode ser carregado como `String` no ClickHouse.<br />Atualmente, [Nullable](../../../data-types/nullable.md) é compatível com os dicionários [Flat](./layouts/flat), [Hashed](./layouts/hashed), [ComplexKeyHashed](./layouts/hashed#complex_key_hashed), [Direct](./layouts/direct), [ComplexKeyDirect](./layouts/direct#complex_key_direct), [RangeHashed](./layouts/range-hashed), Polygon, [Cache](./layouts/cache), [ComplexKeyCache](./layouts/cache), [SSDCache](./layouts/ssd-cache), [SSDComplexKeyCache](./layouts/ssd-cache#complex_key_ssd_cache). Em dicionários [IPTrie](./layouts/ip-trie), tipos `Nullable` não são compatíveis. | Sim         |
| `null_value`                                       | Valor padrão para um elemento inexistente.<br />No exemplo, é uma string vazia. O valor [NULL](../../../syntax.md#null) pode ser usado somente com tipos `Nullable` (veja a linha anterior com a descrição dos tipos).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Sim         |
| `expression`                                       | [Expressão](../../../syntax.md#expressions) que o ClickHouse executa sobre o valor.<br />A expressão pode ser o nome de uma coluna no banco de dados SQL remoto. Assim, você pode usá-la para criar um alias para a coluna remota.<br /><br />Valor padrão: sem expressão.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Não         |
| <a name="hierarchical-dict-attr" /> `hierarchical` | Se `true`, o atributo contém o valor de uma chave pai da chave atual. Veja [Hierarchical Dictionaries](./layouts/hierarchical).<br /><br />Valor padrão: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Não         |
| `injective`                                        | Indicador que mostra se o mapeamento `id -> attribute` é [injective](https://en.wikipedia.org/wiki/Injective_function).<br />Se `true`, o ClickHouse pode posicionar automaticamente após a cláusula `GROUP BY` as consultas aos dicionários com atributos injetivos. Em geral, isso reduz significativamente a quantidade dessas consultas.<br /><br />Valor padrão: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Não         |
| `is_object_id`                                     | Indicador que mostra se a consulta é executada para um documento do MongoDB por `ObjectID`.<br /><br />Valor padrão: `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |             |