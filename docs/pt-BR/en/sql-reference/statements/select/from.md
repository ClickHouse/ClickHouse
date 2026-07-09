---
description: 'Documentação da cláusula FROM'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: 'Cláusula FROM'
doc_type: 'reference'
---

A cláusula `FROM` especifica a fonte da qual os dados serão lidos:

* [Tabela](../../../engines/table-engines/index.md)
* [Subconsulta](../../../sql-reference/statements/select/index.md)
* [Função de tabela](/pt-BR/sql-reference/table-functions)

As cláusulas [JOIN](../../../sql-reference/statements/select/join.md) e [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) também podem ser usadas para ampliar a funcionalidade da cláusula `FROM`.

Uma subconsulta é outra consulta `SELECT` que pode ser especificada entre parênteses dentro da cláusula `FROM`.

Uma cláusula `VALUES` padrão do SQL também pode ser usada como expressão de tabela:

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

Consulte [função de tabela Values](/pt-BR/sql-reference/table-functions/values#sql-standard-values-clause) para mais detalhes.

`FROM` pode conter várias fontes de dados, separadas por vírgulas, o que equivale a executar [CROSS JOIN](../../../sql-reference/statements/select/join.md) entre elas.

`FROM` pode, opcionalmente, aparecer antes de uma cláusula `SELECT`. Esta é uma extensão específica do ClickHouse do SQL padrão, que facilita a leitura das instruções `SELECT`. Exemplo:

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## Modificador FINAL
</div>

Quando `FINAL` é especificado, o ClickHouse mescla completamente os dados antes de retornar o resultado. Isso também executa todas as transformações de dados que ocorrem durante as mesclagens para o motor de tabela em questão.

Ele se aplica à seleção de dados de tabelas que usam os seguintes motores de tabela:

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

As consultas `SELECT` com `FINAL` são executadas em paralelo. A configuração [max&#95;final&#95;threads](/pt-BR/operations/settings/settings#max_final_threads) limita o número de threads usados.

<div id="drawbacks">
  ### Desvantagens
</div>

Consultas que usam `FINAL` são executadas um pouco mais lentamente do que consultas semelhantes que não usam `FINAL` porque:

* Os dados são mesclados durante a execução da consulta.
* Consultas com `FINAL` podem ler colunas da chave primária além das colunas especificadas na consulta.

`FINAL` exige recursos adicionais de computação e memória porque o processamento que normalmente ocorreria no momento da mesclagem precisa ocorrer na memória no momento da consulta. No entanto, às vezes é necessário usar `FINAL` para produzir resultados precisos (já que os dados podem ainda não estar totalmente mesclados). Isso custa menos do que executar `OPTIMIZE` para forçar uma mesclagem.

Como alternativa ao uso de `FINAL`, às vezes é possível usar consultas diferentes que partem do pressuposto de que os processos em segundo plano do mecanismo `MergeTree` ainda não ocorreram e lidar com isso aplicando uma agregação (por exemplo, para descartar duplicatas). Se você precisar usar `FINAL` em suas consultas para obter os resultados necessários, não há problema em fazer isso, mas esteja ciente do processamento adicional exigido.

`FINAL` pode ser aplicado automaticamente usando a configuração [FINAL](../../../operations/settings/settings.md#final) a todas as tabelas de uma consulta por meio de uma sessão ou de um perfil de usuário.

<div id="example-usage">
  ### Exemplo de uso
</div>

Uso da palavra-chave `FINAL`

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

Uso de `FINAL` como configuração no nível da consulta

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

Uso de `FINAL` como configuração em nível de sessão

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### Aliases e FINAL
</div>

Quando uma tabela tem um alias, `FINAL` é posicionado após o alias. Isso fica mais evidente em consultas [`JOIN`](/pt-BR/sql-reference/statements/select/join), nas quais as tabelas geralmente recebem aliases:

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` é um modificador na referência à tabela, portanto deve vir após a expressão completa `table [AS alias]`. Colocá-lo antes do alias (`FROM table1 FINAL AS t1`) é um erro de sintaxe.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

Se a cláusula `FROM` for omitida, os dados serão lidos da tabela `system.one`.
A tabela `system.one` contém exatamente uma linha (essa tabela cumpre a mesma função da tabela DUAL encontrada em outros DBMSs).

Para executar uma consulta, todas as colunas listadas na consulta são extraídas da tabela apropriada. Quaisquer colunas que não sejam necessárias para a consulta externa são descartadas das subconsultas.
Se uma consulta não listar nenhuma coluna (por exemplo, `SELECT count() FROM t`), ainda assim alguma coluna será extraída da tabela (de preferência a menor), para calcular o número de linhas.