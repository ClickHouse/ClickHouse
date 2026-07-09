---
description: 'Documentação da cláusula PARALLEL WITH'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'Cláusula PARALLEL WITH'
doc_type: 'reference'
---

Permite executar várias instruções em paralelo.

<div id="syntax">
  ## Sintaxe
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

Executa as instruções `statement1`, `statement2`, `statement3`, ... em paralelo umas às outras. A saída dessas instruções é descartada.

Em muitos casos, executar instruções em paralelo pode ser mais rápido do que simplesmente executar uma sequência das mesmas instruções. Por exemplo, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3` provavelmente será mais rápido do que `statement1; statement2; statement3`.

<div id="examples">
  ## Exemplos
</div>

Cria duas tabelas em paralelo:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

Exclui duas tabelas em paralelo:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## Configurações
</div>

A configuração [max&#95;threads](../../operations/settings/settings.md#max_threads) controla quantas threads são criadas.

<div id="comparison-with-union">
  ## Comparação com UNION
</div>

A cláusula `PARALLEL WITH` é um pouco semelhante a [UNION](select/union.md), que também executa seus operandos em paralelo. No entanto, há algumas diferenças:

* `PARALLEL WITH` não retorna nenhum resultado da execução de seus operandos; ele só pode relançar uma exceção deles, se houver alguma;
* `PARALLEL WITH` não exige que seus operandos tenham o mesmo conjunto de colunas de resultado;
* `PARALLEL WITH` pode executar quaisquer instruções (não apenas `SELECT`).