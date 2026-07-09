---
description: 'Documentação sobre PARTITION'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: 'Manipulação de partições e partes'
doc_type: 'referência'
---

As seguintes operações com [partições](/pt-BR/engines/table-engines/mergetree-family/custom-partitioning-key.md) estão disponíveis:

* [DESANEXAR PARTITION|PART](#detach-partitionpart) — Move uma partição ou parte para o diretório `detached` e a desanexa.
* [DROP PARTITION|PART](#drop-partitionpart) — Exclui uma partição ou parte.
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - Exclui uma parte ou todas as partes de uma partição de `detached`.
* [FORGET PARTITION](#forget-partition) — Exclui os metadados de uma partição do ZooKeeper se ela estiver vazia.
* [ATTACH PARTITION|PART](#attach-partitionpart) — Anexa uma partição ou parte do diretório `detached` à tabela.
* [ATTACH PARTITION FROM](#attach-partition-from) — Copia a partição de dados de uma tabela para outra e a anexa.
* [REPLACE PARTITION](#replace-partition) — Copia a partição de dados de uma tabela para outra e a substitui.
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — Move a partição de dados de uma tabela para outra.
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — Redefine o valor de uma coluna especificada em uma partição.
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — Redefine o índice secundário especificado em uma partição.
* [FREEZE PARTITION](#freeze-partition) — Cria um backup de uma partição.
* [UNFREEZE PARTITION](#unfreeze-partition) — Remove um backup de uma partição.
* [FETCH PARTITION|PART](#fetch-partitionpart) — Baixa uma parte ou partição de outro servidor.
* [MOVE PARTITION|PART](#move-partitionpart) — Move a partição/parte de dados para outro disco ou volume.
* [UPDATE IN PARTITION](#update-in-partition) — Atualiza os dados dentro da partição com base em uma condição.
* [DELETE IN PARTITION](#delete-in-partition) — Exclui os dados dentro da partição com base em uma condição.
* [REWRITE PARTS](#rewrite-parts) — Reescreve completamente as partes na tabela (ou em uma partição específica).

{/* */ }

<div id="detach-partitionpart">
  ## DESANEXAR PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

Mover todos os dados da partição especificada para o diretório `detached`. O servidor deixa de reconhecer a partição de dados desanexada, como se ela não existisse. O servidor não reconhecerá esses dados até que você execute a consulta [ATTACH](#attach-partitionpart).

Exemplo:

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

Leia sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

Depois que a consulta for executada, você poderá fazer o que quiser com os dados no diretório `detached` — excluí-los do sistema de arquivos ou simplesmente deixá-los lá.

Esta consulta é replicada — ela move os dados para o diretório `detached` em todas as réplicas. Observe que você só pode executar esta consulta em uma réplica líder. Para verificar se uma réplica é líder, execute a consulta `SELECT` na tabela [system.replicas](/pt-BR/operations/system-tables/replicas). Como alternativa, é mais fácil fazer uma consulta `DETACH` em todas as réplicas — todas as réplicas geram uma exceção, exceto as réplicas líderes (já que múltiplos líderes são permitidos).

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

Exclui a partição especificada da tabela. Esta consulta marca a partição como inativa e remove os dados por completo em cerca de 10 minutos.

Leia sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

A consulta é replicada – ela remove os dados em todas as réplicas.

Exemplo:

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

Remove de `detached` a parte especificada ou todas as partes da partição especificada.
Leia mais sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

Remove todos os metadados de uma partição vazia no ZooKeeper. A consulta falha se a partição não estiver vazia ou não existir. Certifique-se de executar isso apenas em partições que nunca mais serão usadas.

Leia sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

Exemplo:

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

Adiciona dados à tabela a partir do diretório `detached`. É possível adicionar dados de uma partição inteira ou de uma parte individual. Exemplos:

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

Leia mais sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

Esta consulta é replicada. A réplica iniciadora verifica se há dados no diretório `detached`.
Se houver dados, a consulta verifica a integridade deles. Se tudo estiver correto, a consulta adiciona os dados à tabela.

Se a réplica não iniciadora, ao receber o comando attach, encontrar a parte com os checksums corretos em sua própria pasta `detached`, ela anexa os dados sem precisar buscá-los em outras réplicas.
Se não houver uma parte com os checksums corretos, os dados serão baixados de qualquer réplica que tenha essa parte.

Você pode colocar dados no diretório `detached` de uma réplica e usar a consulta `ALTER ... ATTACH` para adicioná-los à tabela em todas as réplicas.

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

Esta consulta copia a partição de dados de `table1` para `table2`.

Observe que:

* Os dados não serão excluídos nem de `table1` nem de `table2`.
* `table1` pode ser uma tabela temporária.

Para que a consulta seja executada com sucesso, as seguintes condições devem ser atendidas:

* Ambas as tabelas devem ter a mesma estrutura.
* Ambas as tabelas devem ter a mesma chave de partição, a mesma chave ORDER BY e a mesma chave primária.
* Ambas as tabelas devem ter a mesma política de armazenamento.
* A tabela de destino deve incluir todos os índices e projeções da tabela de origem. Se a configuração `enforce_index_structure_match_on_partition_manipulation` estiver habilitada para a tabela de destino, os índices e as projeções deverão ser idênticos. Caso contrário, a tabela de destino pode ter um superconjunto dos índices e das projeções da tabela de origem.

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

Esta consulta copia a partição de dados de `table1` para `table2` e substitui a partição existente em `table2`. A operação é atômica.

Observe que:

* Os dados não serão excluídos de `table1`.
* `table1` pode ser uma tabela temporária.

Para que a consulta seja executada com sucesso, as seguintes condições devem ser atendidas:

* Ambas as tabelas devem ter a mesma estrutura.
* Ambas as tabelas devem ter a mesma chave de partição, a mesma chave de ORDER BY e a mesma chave primária.
* Ambas as tabelas devem ter a mesma política de armazenamento.
* A tabela de destino deve incluir todos os índices e projeções da tabela de origem. Se a configuração `enforce_index_structure_match_on_partition_manipulation` estiver habilitada para a tabela de destino, os índices e as projeções deverão ser idênticos. Caso contrário, a tabela de destino pode ter um superconjunto dos índices e das projeções da tabela de origem.

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

Esta consulta move a partição de dados de `table_source` para `table_dest`, excluindo os dados de `table_source`.

Para que a consulta seja executada com sucesso, as seguintes condições devem ser atendidas:

* Ambas as tabelas devem ter a mesma estrutura.
* Ambas as tabelas devem ter a mesma chave de partição, a mesma chave de ORDER BY e a mesma chave primária.
* Ambas as tabelas devem ter a mesma política de armazenamento.
* Ambas as tabelas devem pertencer à mesma família de engines (replicadas ou não replicadas).
* A tabela de destino deve incluir todos os índices e projeções da tabela de origem. Se a configuração `enforce_index_structure_match_on_partition_manipulation` estiver habilitada na tabela de destino, os índices e as projeções deverão ser idênticos. Caso contrário, a tabela de destino pode ter um superconjunto dos índices e das projeções da tabela de origem.

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

Redefine todos os valores da coluna especificada em uma partição. Se a cláusula `DEFAULT` tiver sido definida ao criar a tabela, esta consulta define o valor da coluna como o valor padrão especificado.

Exemplo:

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

Esta consulta cria um backup local de uma partição especificada. Se a cláusula `PARTITION` for omitida, a consulta cria o backup de todas as partições de uma só vez.

:::note
Todo o processo de backup é realizado sem interromper o servidor.
:::

Observe que, para tabelas no estilo antigo, você pode especificar o prefixo do nome da partição (por exemplo, `2019`) — nesse caso, a consulta cria o backup de todas as partições correspondentes. Leia sobre como definir a expressão de partição na seção [Como definir a expressão de partição](#how-to-set-partition-expression).

No momento da execução, para criar um snapshot dos dados, a consulta cria hardlinks para os dados da tabela. Os hardlinks são colocados no diretório `/var/lib/clickhouse/shadow/N/...`, onde:

* `/var/lib/clickhouse/` é o diretório de trabalho do ClickHouse especificado na configuração.
* `N` é o número incremental do backup.
* se o parâmetro `WITH NAME` for especificado, o valor do parâmetro `'backup_name'` será usado no lugar do número incremental.

:::note
Se você usar [um conjunto de discos para armazenamento de dados em uma tabela](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes), o diretório `shadow/N` aparecerá em cada disco, armazenando as partes de dados correspondentes à expressão `PARTITION`.
:::

A mesma estrutura de diretórios de `/var/lib/clickhouse/` é criada dentro do backup. A consulta executa `chmod` em todos os arquivos, impedindo a gravação neles.

Após criar o backup, você pode copiar os dados de `/var/lib/clickhouse/shadow/` para o servidor remoto e, em seguida, excluí-los do servidor local. Observe que a consulta `ALTER t FREEZE PARTITION` não é replicada. Ela cria um backup local apenas no servidor local.

A consulta cria o backup quase instantaneamente (mas primeiro espera que as consultas atuais à tabela correspondente terminem).

`ALTER TABLE t FREEZE PARTITION` copia apenas os dados, não os metadados da tabela. Para fazer um backup dos metadados da tabela, copie o arquivo `/var/lib/clickhouse/metadata/database/table.sql`

Para restaurar dados de um backup, faça o seguinte:

1. Crie a tabela, se ela não existir. Para ver a consulta, use o arquivo .sql (substitua `ATTACH` por `CREATE` nele).
2. Copie os dados do diretório `data/database/table/` dentro do backup para o diretório `/var/lib/clickhouse/data/database/table/detached/`.
3. Execute consultas `ALTER TABLE t ATTACH PARTITION` para adicionar os dados à tabela.

A restauração a partir de um backup não exige interromper o servidor.

A consulta processa partes em paralelo, e o número de threads é controlado pela configuração `max_threads`.

Para mais informações sobre backups e restauração de dados, consulte a seção [&quot;Backup e restauração no ClickHouse&quot;](/pt-BR/operations/backup/overview).

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

Remove do disco as partições `frozen` com o nome especificado. Se a cláusula `PARTITION` for omitida, a consulta remove o backup de todas as partições de uma só vez.

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

A consulta funciona de forma semelhante a `CLEAR COLUMN`, mas reinicia um índice em vez dos dados de uma coluna.

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

Baixa uma partição de outro servidor. Esta consulta funciona apenas com tabelas replicadas.

A consulta faz o seguinte:

1. Baixa a partição|parte do shard especificado. Em &#39;path-in-zookeeper&#39;, você deve especificar um caminho para o shard no ZooKeeper.
2. Em seguida, a consulta coloca os dados baixados no diretório `detached` da tabela `table_name`. Use a consulta [ATTACH PARTITION|PART](#attach-partitionpart) para adicionar os dados à tabela.

Por exemplo:

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

Observe que:

* A consulta `ALTER ... FETCH PARTITION|PART` não é replicada. Ela coloca a parte ou partição no diretório `detached` apenas no servidor local.
* A consulta `ALTER TABLE ... ATTACH` é replicada. Ela adiciona os dados a todas as réplicas. Os dados são adicionados a uma das réplicas a partir do diretório `detached` e, nas demais, a partir das réplicas vizinhas.

Antes do download, o sistema verifica se a partição existe e se a estrutura da tabela corresponde. A réplica mais adequada é selecionada automaticamente entre as réplicas saudáveis.

Embora a consulta se chame `ALTER TABLE`, ela não altera a estrutura da tabela nem modifica imediatamente os dados disponíveis na tabela.

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

Move partições ou partes de dados para outro volume ou disco em tabelas com o motor `MergeTree`. Consulte [Como usar vários dispositivos de bloco para armazenamento de dados](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes).

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

A consulta `ALTER TABLE t MOVE`:

* Não é replicada, porque réplicas diferentes podem ter políticas de armazenamento diferentes.
* Retorna um erro se o disco ou volume especificado não estiver configurado. A consulta também retorna um erro se as condições de movimentação de dados especificadas na política de armazenamento não puderem ser aplicadas.
* Pode retornar um erro caso os dados a serem movidos já tenham sido movidos por um processo em segundo plano, por uma consulta `ALTER TABLE t MOVE` concorrente ou como resultado da mesclagem de dados em segundo plano. O usuário não deve executar nenhuma ação adicional nesse caso.

Exemplo:

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

Manipula os dados da partição especificada que correspondem à expressão de filtro especificada. Implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

Sintaxe:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Exemplo
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### Veja também
</div>

* [UPDATE](/pt-BR/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

Exclui dados da partição especificada que correspondem à expressão de filtro especificada. Implementado como uma [mutação](/pt-BR/sql-reference/statements/alter/index.md#mutations).

Sintaxe:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### Exemplo
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

Isso reescreverá as partes do zero, usando todas as novas configurações. Isso faz sentido porque configurações no nível da tabela, como `use_const_adaptive_granularity`, por padrão são aplicadas apenas a partes recém-escritas.

<div id="example">
  ### Exemplo
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### Veja também
</div>

* [DELETE](/pt-BR/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## Como definir a expressão de partição
</div>

Você pode especificar a expressão de partição em consultas `ALTER ... PARTITION` de diferentes formas:

* Como um valor da coluna `partition` da tabela `system.parts`. Por exemplo, `ALTER TABLE visits DETACH PARTITION 201901`.
* Usando a palavra-chave `ALL`. Ela só pode ser usada com DROP/DETACH/ATTACH/ATTACH FROM. Por exemplo, `ALTER TABLE visits ATTACH PARTITION ALL`.
* Como uma tupla de expressões ou constantes que corresponda (em termos de tipos) à tupla de chaves de particionamento da tabela. No caso de uma chave de particionamento com um único elemento, a expressão deve ser envolvida pela função `tuple (...)`. Por exemplo, `ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`.
* Usando o ID da partição. O ID da partição é um identificador de string da partição (legível por humanos, quando possível) usado como nome das partições no sistema de arquivos e no ZooKeeper. O ID da partição deve ser especificado na cláusula `PARTITION ID`, entre aspas simples. Por exemplo, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
* Na consulta [ALTER ATTACH PART](#attach-partitionpart) e [DROP DETACHED PART](#drop-detached-partitionpart), para especificar o nome de uma parte, use um literal de string com um valor da coluna `name` da tabela [system.detached&#95;parts](/pt-BR/operations/system-tables/detached_parts). Por exemplo, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

O uso de aspas ao especificar a partição depende do tipo da expressão de partição. Por exemplo, para o tipo `String`, você precisa especificar o nome entre aspas (`'`). Para os tipos `Date` e `Int*`, não são necessárias aspas.

Todas as regras acima também se aplicam à consulta [OPTIMIZE](/pt-BR/sql-reference/statements/optimize.md). Se você precisar especificar a única partição ao otimizar uma tabela não particionada, defina a expressão `PARTITION tuple()`. Por exemplo:

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` especifica a partição à qual as expressões [UPDATE](/pt-BR/sql-reference/statements/alter/update) ou [DELETE](/pt-BR/sql-reference/statements/alter/delete) são aplicadas na consulta `ALTER TABLE`. Novas partes são criadas apenas a partir da partição especificada. Dessa forma, `IN PARTITION` ajuda a reduzir a carga quando a tabela está dividida em muitas partições e você só precisa atualizar os dados pontualmente.

Os exemplos de consultas `ALTER ... PARTITION` são mostrados nos testes [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) e [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).