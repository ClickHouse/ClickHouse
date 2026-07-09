---
description: 'Documentação da instrução INSERT INTO'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'Instrução INSERT INTO'
doc_type: 'reference'
---

Insere dados em uma tabela.

**Sintaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Você pode especificar uma lista de colunas para inserir usando `(c1, c2, c3)`. Também é possível usar uma expressão com [matcher](../../sql-reference/statements/select/index.md#asterisk) de coluna, como `*`, e/ou [modificadores](../../sql-reference/statements/select/index.md#select-modifiers), como [APPLY](/pt-BR/sql-reference/statements/select/apply-modifier), [EXCEPT](/pt-BR/sql-reference/statements/select/except-modifier), [REPLACE](/pt-BR/sql-reference/statements/select/replace-modifier).

Por exemplo, considere a tabela:

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

Se você quiser inserir dados em todas as colunas, exceto na coluna `b`, poderá fazer isso usando a palavra-chave `EXCEPT`. Com base na sintaxe acima, será necessário garantir que você insira tantos valores (`VALUES (v11, v13)`) quantas colunas (`(c1, c3)`) especificar:

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

Neste exemplo, vemos que a segunda linha inserida tem as colunas `a` e `c` preenchidas com os valores fornecidos, e `b` preenchida com o valor padrão. Também é possível usar a palavra-chave `DEFAULT` para inserir valores padrão:

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

Se uma lista de colunas não incluir todas as colunas existentes, as demais colunas serão preenchidas com:

* Os valores calculados a partir das expressões `DEFAULT` especificadas na definição da tabela.
* Zeros e strings vazias, se as expressões `DEFAULT` não estiverem definidas.

Os dados podem ser passados para o INSERT em qualquer [formato](/pt-BR/sql-reference/formats) compatível com o ClickHouse. O formato deve ser especificado explicitamente na consulta:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

Por exemplo, o formato de consulta a seguir é idêntico à versão básica de `INSERT ... VALUES`:

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse remove todos os espaços e uma quebra de linha (se houver) antes dos dados. Ao montar uma consulta, recomendamos colocar os dados em uma nova linha após os operadores da consulta, o que é importante se os dados começarem com espaços.

Exemplo:

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Você pode inserir dados separadamente da consulta usando o [cliente de linha de comando](/pt-BR/operations/utilities/clickhouse-local) ou a [interface HTTP](/pt-BR/interfaces/http).

:::note
Se quiser especificar `SETTINGS` para a consulta `INSERT`, faça isso *antes* da cláusula `FORMAT`, já que tudo depois de `FORMAT format_name` é tratado como dados. Por exemplo:

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## Restrições
</div>

Se uma tabela tiver [restrições](../../sql-reference/statements/create/table.md#constraints), suas expressões serão verificadas para cada linha de dados inseridos. Se alguma dessas restrições não for atendida, o servidor gerará uma exceção contendo o nome e a expressão da restrição, e a consulta será interrompida.

<div id="data-type-validation">
  ## Validação de tipos de dados
</div>

O ClickHouse valida os tipos de dados permitidos (controlados por configurações como `enable_time_time64_type`, `allow_suspicious_low_cardinality_types`, `allow_suspicious_fixed_string_types` etc.) apenas durante a criação de tabelas (`CREATE TABLE`) e a modificação do esquema (`ALTER TABLE`), não durante o `INSERT`.

Isso significa que, se já existir uma tabela com um tipo de dados não permitido, ainda será possível inserir dados nela, mesmo que a configuração correspondente esteja desabilitada no servidor. Isso é intencional — depois que uma tabela é criada, as inserções não devem ser bloqueadas por configurações que controlam a criação de tipos.

Por exemplo:

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
Como consequência, um cliente com uma versão mais recente (em que uma configuração está habilitada por padrão) pode inserir dados com tipos de dados não permitidos em um servidor com uma versão mais antiga (em que a configuração está desabilitada), desde que a tabela de destino já tenha os tipos de coluna correspondentes. A validação é aplicada no nível de DDL, não no nível de DML.
:::

<div id="inserting-the-results-of-select">
  ## Inserindo os resultados de SELECT
</div>

**Sintaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

As colunas são mapeadas de acordo com sua posição na cláusula `SELECT`. No entanto, os nomes delas na expressão `SELECT` e na tabela do `INSERT` podem ser diferentes. Se necessário, é feita conversão de tipo.

Nenhum dos formatos de dados, exceto o formato Values, permite definir valores como expressões, como `now()`, `1 + 2` e assim por diante. O formato Values permite o uso limitado de expressões, mas isso não é recomendado, porque, nesse caso, é usado código ineficiente para executá-las.

Outras consultas para modificar partes de dados não são suportadas: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
No entanto, você pode excluir dados antigos usando `ALTER TABLE ... DROP PARTITION`.

A cláusula `FORMAT` deve ser especificada no final da consulta se a cláusula `SELECT` contiver a função de tabela [input()](../../sql-reference/table-functions/input.md).

Para inserir um valor padrão em vez de `NULL` em uma coluna com um tipo de dado que não aceita `NULL`, habilite a configuração [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default).

`INSERT` também oferece suporte a CTE (expressão de tabela comum). Por exemplo, as duas instruções a seguir são equivalentes:

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## Inserindo dados a partir de um arquivo
</div>

**Sintaxe**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

Use a sintaxe acima para inserir dados de um ou mais arquivos armazenados no lado do **cliente**. `file_name` e `type` são literais de string. O [formato](../../interfaces/formats.md) do arquivo de entrada deve ser definido na cláusula `FORMAT`.

Arquivos comprimidos são compatíveis. O tipo de compressão é detectado pela extensão do nome do arquivo. Também é possível especificá-lo explicitamente em uma cláusula `COMPRESSION`. Os tipos compatíveis são: `'none'`, `'gzip'`, `'deflate'`, `'br'`, `'xz'`, `'zstd'`, `'lz4'`, `'bz2'`.

Essa funcionalidade está disponível no [cliente de linha de comando](../../interfaces/client.md) e no [clickhouse-local](../../operations/utilities/clickhouse-local.md).

**Exemplos**

<div id="single-file-with-from-infile">
  ### Um único arquivo com FROM INFILE
</div>

Execute as seguintes consultas usando o [cliente de linha de comando](../../interfaces/client.md):

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### Vários arquivos com FROM INFILE usando globs
</div>

Este exemplo é muito semelhante ao anterior, mas as inserções são feitas a partir de vários arquivos usando `FROM INFILE 'input_*.csv'`.

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
Além de selecionar vários arquivos com `*`, você pode usar intervalos (`{1,2}` ou `{1..9}`) e outras [substituições de glob](/pt-BR/sql-reference/table-functions/file.md/#globs-in-path). Estes três funcionam com o exemplo acima:

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## Inserindo com uma função de tabela
</div>

Os dados podem ser inseridos em tabelas referenciadas por [funções de tabela](../../sql-reference/table-functions/index.md).

**Sintaxe**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**Exemplo**

A função de tabela [remote](/pt-BR/sql-reference/table-functions/remote) é usada nas consultas a seguir:

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## Inserindo no ClickHouse Cloud
</div>

Por padrão, os serviços no ClickHouse Cloud oferecem múltiplas réplicas para alta disponibilidade. Quando você se conecta a um serviço, a conexão é estabelecida com uma dessas réplicas.

Depois que um `INSERT` é concluído com sucesso, os dados são gravados no armazenamento subjacente. No entanto, pode levar algum tempo até que as réplicas recebam essas atualizações. Portanto, se você usar uma conexão diferente que execute uma consulta `SELECT` em outra dessas réplicas, os dados atualizados talvez ainda não estejam refletidos.

É possível usar `select_sequential_consistency` para forçar a réplica a receber as atualizações mais recentes. Aqui está um exemplo de consulta `SELECT` usando essa configuração:

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

Observe que usar `select_sequential_consistency` aumentará a carga no ClickHouse Keeper (usado internamente pelo ClickHouse Cloud) e poderá resultar em um desempenho mais lento, dependendo da carga do serviço. Recomendamos não habilitar essa configuração, a menos que seja necessário. A abordagem recomendada é executar operações de leitura/gravação na mesma sessão ou usar um driver cliente que use o protocolo nativo (e, assim, ofereça suporte a conexões sticky).

<div id="inserting-into-a-replicated-setup">
  ## Inserindo em uma configuração replicada
</div>

Em uma configuração replicada, os dados ficarão visíveis em outras réplicas depois de serem replicados. Os dados começam a ser replicados (baixados em outras réplicas) imediatamente após um `INSERT`. Isso difere do ClickHouse Cloud, em que os dados são gravados imediatamente no armazenamento compartilhado e as réplicas acompanham as alterações de metadados.

Observe que, em configurações replicadas, `INSERTs` às vezes podem levar um tempo considerável (na ordem de um segundo), pois exigem commit no ClickHouse Keeper para alcançar consenso distribuído. O uso do S3 como armazenamento também adiciona latência.

<div id="performance-considerations">
  ## Considerações de desempenho
</div>

`INSERT` ordena os dados de entrada pela chave primária e os divide em partições com base na chave de partição. Se você inserir dados em várias partições de uma só vez, isso pode reduzir significativamente o desempenho da consulta `INSERT`. Para evitar isso:

* Adicione dados em lotes relativamente grandes, como 100.000 linhas por vez.
* Agrupe os dados pela chave de partição antes de enviá-los ao ClickHouse.

O desempenho não será afetado se:

* Os dados forem adicionados em tempo real.
* Você enviar dados que normalmente já vêm ordenados por tempo.

<div id="asynchronous-inserts">
  ### Inserções assíncronas
</div>

É possível inserir dados de forma assíncrona por meio de inserções pequenas, mas frequentes. Os dados dessas inserções são combinados em lotes e depois inseridos com segurança em uma tabela. Para usar inserções assíncronas, habilite a configuração [`async_insert`](/pt-BR/operations/settings/settings#async_insert).

Usar `async_insert` ou o [motor de tabela `Buffer`](/pt-BR/engines/table-engines/special/buffer) gera bufferização adicional.

<div id="large-or-long-running-inserts">
  ### Inserções grandes ou demoradas
</div>

Ao inserir grandes volumes de dados, o ClickHouse otimiza o desempenho de gravação por meio de um processo chamado &quot;compactação&quot;. Pequenos blocos de dados inseridos na memória são mesclados e compactados em blocos maiores antes de serem gravados em disco. A compactação reduz a sobrecarga associada a cada operação de gravação. Nesse processo, os dados inseridos ficam disponíveis para consulta depois que o ClickHouse conclui a gravação de cada [`max_insert_block_size`](/pt-BR/operations/settings/settings#max_insert_block_size) linhas.

**Veja também**

* [async&#95;insert](/pt-BR/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/pt-BR/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/pt-BR/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/pt-BR/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/pt-BR/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/pt-BR/operations/settings/settings#async_insert_max_data_size)