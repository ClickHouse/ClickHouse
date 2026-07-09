---
description: 'Cria uma tabela temporária com a estrutura especificada usando o motor de tabela Null. A função é usada para facilitar a escrita de testes e demonstrações.'
sidebar_label: 'função null'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

Cria uma tabela temporária com a estrutura especificada usando o [Null](../../engines/table-engines/special/null.md) motor de tabela. De acordo com as propriedades do motor `Null`, os dados da tabela são ignorados e a própria tabela é descartada imediatamente após a execução da consulta. A função é usada para facilitar a escrita de testes e demonstrações.

<div id="syntax">
  ## Sintaxe
</div>

```sql
null('structure')
```

<div id="argument">
  ## Argumento
</div>

* `structure` — Uma lista de colunas e seus tipos. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela temporária com motor `Null` e estrutura especificada.

<div id="example">
  ## Exemplo
</div>

Consulta com a função `null`:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

pode substituir três consultas:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## Relacionado
</div>

* [Motor de tabela Null](../../engines/table-engines/special/null.md)