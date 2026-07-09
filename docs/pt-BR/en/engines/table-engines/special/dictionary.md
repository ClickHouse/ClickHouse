---
description: 'O motor `Dictionary` exibe os dados do dicionário como uma tabela do ClickHouse.'
sidebar_label: 'Dicionário'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Motor de tabela Dicionário'
doc_type: 'reference'
---

O motor `Dictionary` exibe os dados do [dicionário](../../../sql-reference/statements/create/dictionary/overview.md) como uma tabela do ClickHouse.

<div id="example">
  ## Exemplo
</div>

Por exemplo, considere um dicionário de `products` com a seguinte configuração:

```xml
<dictionaries>
    <dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</dictionaries>
```

Consulte os dados do dicionário:

```sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

```text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

Você pode usar as funções [dictGet*](/pt-BR/sql-reference/functions/ext-dict-functions) para obter os dados do dicionário nesse formato.

Essa visualização não é útil quando você precisa acessar dados brutos ou ao realizar uma operação `JOIN`. Nesses casos, você pode usar a engine `Dictionary`, que exibe os dados do dicionário em uma tabela.

Sintaxe:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Exemplo de uso:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

Ok

Veja o que há na tabela.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**Veja também**

* [Função de tabela Dicionário](/pt-BR/sql-reference/table-functions/dictionary)