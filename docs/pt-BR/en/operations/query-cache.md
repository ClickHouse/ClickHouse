---
description: 'Guia para usar e configurar o recurso de cache de consultas no ClickHouse'
sidebar_label: 'Cache de consultas'
sidebar_position: 65
slug: /operations/query-cache
title: 'Cache de consultas'
doc_type: 'guide'
---

O cache de consultas permite executar consultas `SELECT` apenas uma vez e atender às execuções subsequentes da mesma consulta diretamente do cache.
Dependendo do tipo de consulta, isso pode reduzir drasticamente a latência e o consumo de recursos do servidor ClickHouse.

<div id="background-design-and-limitations">
  ## Contexto, design e limitações
</div>

Os caches de consultas geralmente podem ser vistos como transacionalmente consistentes ou inconsistentes.

* Em caches transacionalmente consistentes, o banco de dados invalida (descarta) resultados de consultas em cache se o resultado da consulta `SELECT` mudar
  ou puder mudar. No ClickHouse, as operações que alteram os dados incluem inserts/updates/deletes em/de tabelas ou merges de
  collapsing. O cache transacionalmente consistente é especialmente adequado para bancos de dados OLTP, por exemplo
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (que removeu o cache de consultas após a v8.0) e
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
* Em caches transacionalmente inconsistentes, pequenas imprecisões nos resultados das consultas são aceitas, partindo do pressuposto de que todas as entradas de cache recebem
  um período de validade após o qual expiram (por exemplo, 1 minuto) e de que os dados subjacentes mudam muito pouco durante esse período.
  Essa abordagem é, no geral, mais adequada para bancos de dados OLAP. Como exemplo de caso em que o cache transacionalmente inconsistente é suficiente,
  considere um relatório horário de vendas em uma ferramenta de relatórios acessada simultaneamente por vários usuários. Em geral, os dados de vendas mudam
  devagar o suficiente para que o banco de dados só precise calcular o relatório uma vez (representado pela primeira consulta `SELECT`). As consultas seguintes podem ser
  atendidas diretamente do cache de consultas. Neste exemplo, um período de validade razoável poderia ser de 30 min.

O cache transacionalmente inconsistente é tradicionalmente fornecido por ferramentas cliente ou pacotes proxy (por exemplo,
[chproxy](https://www.chproxy.org/configuration/caching/)) que interagem com o banco de dados. Como resultado, a mesma lógica de cache e
configuração costuma ser duplicada. Com o cache de consultas do ClickHouse, a lógica de cache passa para o lado do servidor. Isso reduz o esforço
de manutenção e evita redundância.

<div id="configuration-settings-and-usage">
  ## Configurações e uso
</div>

:::note
No ClickHouse Cloud, você deve usar [configurações no nível da consulta](/pt-BR/operations/settings/query-level) para editar as configurações do cache de consultas. Atualmente, não há suporte para editar [configurações no nível de configuração](/pt-BR/operations/configuration-files).
:::

:::note
O [clickhouse-local](utilities/clickhouse-local.md) executa uma única consulta por vez. Como não faz sentido armazenar em cache o resultado de consultas, o
cache de resultados de consultas é desativado no clickhouse-local.
:::

A configuração [use&#95;query&#95;cache](/pt-BR/operations/settings/settings#use_query_cache) pode ser usada para controlar se uma consulta específica ou todas as consultas da
sessão atual devem usar o cache de consultas. Por exemplo, a primeira execução da consulta

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

armazenará o resultado da consulta no cache de consultas. Execuções subsequentes da mesma consulta (também com o parâmetro `use_query_cache = true`) vão
ler do cache o resultado já calculado e retorná-lo imediatamente.

:::note
A configuração `use_query_cache`, assim como todas as demais configurações relacionadas ao cache de consultas, só tem efeito em instruções `SELECT` independentes. Em particular,
os resultados de `SELECT`s sobre views criadas por `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` não são armazenados em cache, a menos que a instrução `SELECT`
seja executada com `SETTINGS use_query_cache = true`.
:::

A forma como o cache é utilizado pode ser configurada com mais detalhes por meio das configurações [enable&#95;writes&#95;to&#95;query&#95;cache](/pt-BR/operations/settings/settings#enable_writes_to_query_cache)
e [enable&#95;reads&#95;from&#95;query&#95;cache](/pt-BR/operations/settings/settings#enable_reads_from_query_cache) (ambas `true` por padrão). A primeira configuração
controla se os resultados das consultas são armazenados no cache, enquanto a segunda determina se o banco de dados deve tentar recuperar resultados de consultas
do cache. Por exemplo, a consulta a seguir usará o cache apenas de forma passiva, ou seja, tentará ler dele, mas não armazenará
seu resultado nele:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

Para ter o máximo de controle, em geral recomenda-se definir as configurações `use_query_cache`, `enable_writes_to_query_cache` e
`enable_reads_from_query_cache` apenas em consultas específicas. Também é possível habilitar o cache no nível do usuário ou do perfil (por
exemplo, via `SET use_query_cache = true`), mas é preciso ter em mente que, nesse caso, todas as consultas `SELECT` podem retornar resultados em cache.

O cache de consultas pode ser limpo com a instrução `SYSTEM CLEAR QUERY CACHE`. O conteúdo do cache de consultas é exibido na tabela de sistema
[system.query&#95;cache](system-tables/query_cache.md). O número de acertos e falhas do cache de consultas desde a inicialização do banco de dados é mostrado como os eventos
&quot;QueryCacheHits&quot; e &quot;QueryCacheMisses&quot; na tabela de sistema [system.events](system-tables/events.md). Ambos os contadores só são atualizados para
consultas `SELECT` executadas com a configuração `use_query_cache = true`; outras consultas não afetam &quot;QueryCacheMisses&quot;. O campo `query_cache_usage`
na tabela de sistema [system.query&#95;log](system-tables/query_log.md) mostra, para cada consulta executada, se o resultado da consulta foi gravado no
cache de consultas ou lido dele. As métricas `QueryCacheEntries` e `QueryCacheBytes` na tabela de sistema
[system.metrics](system-tables/metrics.md) mostram quantas entradas / bytes o cache de consultas contém no momento.

Há uma instância do cache de consultas por processo do servidor ClickHouse. No entanto, por padrão, os resultados em cache não são compartilhados entre usuários. Isso pode ser
alterado (veja abaixo), mas isso não é recomendado por motivos de segurança.

Os resultados das consultas são referenciados no cache de consultas pela [Árvore de Sintaxe Abstrata (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) de
cada consulta. Isso significa que o cache não diferencia maiúsculas de minúsculas; por exemplo, `SELECT 1` e `select 1` são tratados como a mesma consulta. Para
tornar a correspondência mais natural, todas as configurações no nível da consulta relacionadas ao cache de consultas e à [formatação de saída](settings/settings-formats.md))
são removidas da AST.

Se a consulta foi abortada devido a uma exceção ou cancelamento pelo usuário, nenhuma entrada é gravada no cache de consultas.

O tamanho do cache de consultas em bytes, o número máximo de entradas no cache e o tamanho máximo de entradas individuais do cache (em bytes e em
registros) podem ser configurados usando diferentes [opções de configuração do servidor](/pt-BR/operations/server-configuration-parameters/settings#query_cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

Também é possível limitar o uso do cache por usuários específicos usando [perfis de configurações](settings/settings-profiles.md) e [restrições
de configurações](settings/constraints-on-settings.md). Mais especificamente, você pode restringir a quantidade máxima de memória (em bytes) que um usuário pode
alocar no cache de consultas e o número máximo de resultados de consultas armazenados. Para isso, primeiro defina as configurações
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/pt-BR/operations/settings/settings#query_cache_max_size_in_bytes) e
[query&#95;cache&#95;max&#95;entries](/pt-BR/operations/settings/settings#query_cache_max_entries) em um perfil de usuário no `users.xml` e, em seguida, torne ambas as configurações
somente leitura:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

Para definir por quanto tempo, no mínimo, uma consulta deve levar para ser executada para que seu resultado possa ser armazenado em cache, você pode usar a configuração
[query&#95;cache&#95;min&#95;query&#95;duration](/pt-BR/operations/settings/settings#query_cache_min_query_duration). Por exemplo, o resultado da consulta

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

só é armazenado em cache se a consulta levar mais de 5 segundos para ser executada. Também é possível especificar quantas vezes uma consulta precisa ser executada até que seu resultado seja
armazenado em cache — para isso, use a configuração [query&#95;cache&#95;min&#95;query&#95;runs](/pt-BR/operations/settings/settings#query_cache_min_query_runs).

As entradas no cache de consultas ficam obsoletas após um determinado período (time-to-live). Por padrão, esse período é de 60 segundos, mas um valor diferente
pode ser especificado no nível de sessão, perfil ou consulta usando a configuração [query&#95;cache&#95;ttl](/pt-BR/operations/settings/settings#query_cache_ttl). O cache de consultas
remove entradas de forma &quot;preguiçosa&quot;, ou seja, quando uma entrada fica obsoleta, ela não é removida imediatamente do cache. Em vez disso, quando uma nova entrada
vai ser inserida no cache de consultas, o banco de dados verifica se o cache tem espaço livre suficiente para a nova entrada. Se esse não for o
caso, o banco de dados tenta remover todas as entradas obsoletas. Se o cache ainda não tiver espaço livre suficiente, a nova entrada não será inserida.

Se a consulta for executada via HTTP, o ClickHouse define os headers `Age` e `Expires` com a idade (em segundos) e o timestamp de expiração da
entrada em cache.

As entradas no cache de consultas são comprimidas por padrão. Isso reduz o consumo geral de memória, ao custo de gravações mais lentas no cache e leituras mais lentas
dele. Para desativar a compressão, use a configuração [query&#95;cache&#95;compress&#95;entries](/pt-BR/operations/settings/settings#query_cache_compress_entries).

Às vezes, é útil manter vários resultados da mesma consulta armazenados em cache. Isso pode ser obtido usando a configuração
[query&#95;cache&#95;tag](/pt-BR/operations/settings/settings#query_cache_tag), que atua como um rótulo (ou espaço de nomes) para entradas do cache de consultas. O cache de consultas
considera diferentes os resultados da mesma consulta com tags diferentes.

Exemplo de criação de três entradas diferentes no cache de consultas para a mesma consulta:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

Para remover apenas as entradas com a tag `tag` do cache de consultas, você pode usar a instrução `SYSTEM CLEAR QUERY CACHE TAG 'tag'`.

<div id="subquery-caching">
  ## Cache de subconsultas
</div>

Por padrão, `use_query_cache` na consulta externa não se propaga para as subconsultas. Isso significa que cada subconsulta deve habilitar explicitamente o cache:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

Neste exemplo, apenas o resultado da subconsulta interna é armazenado em cache. A consulta externa não é armazenada em cache.

Para ativar o cache de todas as subconsultas de uma só vez, use a configuração `query_cache_for_subqueries`:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Para desativar explicitamente o cache de uma subconsulta específica enquanto a propagação em lote estiver habilitada, defina `use_query_cache = false` nessa subconsulta:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

As entradas de cache de subconsulta são visíveis em [system.query&#95;cache](system-tables/query_cache.md) com `is_subquery = 1`. A configuração `query_cache_ttl` também se aplica às entradas de cache de subconsulta e pode ser definida por subconsulta.

O ClickHouse lê os dados da tabela em blocos de [max&#95;block&#95;size](/pt-BR/operations/settings/settings#max_block_size) linhas. Devido à filtragem, agregação,
etc., os blocos de resultado normalmente são muito menores que &#39;max&#95;block&#95;size&#39;, mas também há casos em que são muito maiores. A configuração
[query&#95;cache&#95;squash&#95;partial&#95;results](/pt-BR/operations/settings/settings#query_cache_squash_partial_results) (habilitada por padrão) controla se os blocos de resultado
são compactados (se forem muito pequenos) ou divididos (se forem grandes) em blocos do tamanho de &#39;max&#95;block&#95;size&#39; antes de serem inseridos no cache de resultados de consultas.
Isso reduz o desempenho das gravações no cache de consultas, mas melhora a taxa de compressão das entradas de cache e proporciona uma granularidade de
bloco mais natural quando os resultados de consultas são posteriormente servidos a partir do cache de consultas.

Como resultado, o cache de consultas armazena, para cada consulta, múltiplos blocos de resultado
(parciais). Embora esse comportamento seja um bom padrão, ele pode ser desativado com a configuração
[query&#95;cache&#95;squash&#95;partial&#95;results](/pt-BR/operations/settings/settings#query_cache_squash_partial_results).

Além disso, os resultados de consultas com funções não determinísticas não são armazenados em cache por padrão. Essas funções incluem

* funções para acessar dicionários: [`dictGet()`](/pt-BR/sql-reference/functions/ext-dict-functions) etc.
* [Funções Definidas pelo Usuário](../sql-reference/statements/create/function.md) sem a tag `<deterministic>true</deterministic>` em sua definição
  XML,
* funções que retornam a data ou hora atual: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) etc.,
* funções que retornam valores aleatórios: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) etc.,
* funções cujo resultado depende do tamanho, da ordem ou dos fragmentos internos usados no processamento de consultas:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) etc.,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) etc.,
* funções que dependem do ambiente: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/pt-BR/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) etc.

Para forçar o armazenamento em cache dos resultados de consultas com funções não determinísticas ainda assim, use a configuração
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/pt-BR/operations/settings/settings#query_cache_nondeterministic_function_handling).

Os resultados de consultas que envolvem tabelas de sistema (por exemplo, [system.processes](system-tables/processes.md)&#96; ou
[information&#95;schema.tables](system-tables/information_schema.md)) não são armazenados em cache por padrão. Para forçar o armazenamento em cache dos resultados de consultas com
tabelas de sistema ainda assim, use a configuração [query&#95;cache&#95;system&#95;table&#95;handling](/pt-BR/operations/settings/settings#query_cache_system_table_handling).

Por fim, as entradas no cache de consultas não são compartilhadas entre usuários por motivos de segurança. Por exemplo, o usuário A não deve conseguir contornar uma
ROW POLICY em uma tabela executando a mesma consulta que outro usuário B, para o qual essa política não existe. No entanto, se necessário, as entradas de cache podem
ser marcadas como acessíveis a outros usuários (isto é, compartilhadas) ao especificar a configuração
[query&#95;cache&#95;share&#95;between&#95;users](/pt-BR/operations/settings/settings#query_cache_share_between_users).

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Conheça o cache de consultas do ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)