---
description: 'Mantém os dados em buffer para gravação na RAM, descarregando-os periodicamente em outra tabela. Durante a operação de leitura, os dados são lidos do buffer e da outra tabela simultaneamente.'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'motor de tabela Buffer'
doc_type: 'reference'
---

Mantém os dados em buffer para gravação na RAM, descarregando-os periodicamente em outra tabela. Durante a operação de leitura, os dados são lidos do buffer e da outra tabela simultaneamente.

:::note
Uma alternativa recomendada ao motor de tabela Buffer é habilitar [inserções assíncronas](/pt-BR/guides/best-practices/asyncinserts.md).
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### Parâmetros do motor
</div>

<div id="database">
  #### `database`
</div>

`database` – Nome do banco de dados. Você pode usar `currentDatabase()` ou outra expressão constante que retorne uma string.

<div id="table">
  #### `table`
</div>

`table` – Tabela para a qual os dados são descarregados.

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – Camada de paralelismo. Fisicamente, a tabela será representada por `num_layers` buffers independentes.

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes` e `max_bytes`
</div>

Condições para descarregar dados do buffer.

<div id="optional-engine-parameters">
  ### Parâmetros opcionais do motor
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, and `flush_bytes`
</div>

Condições para flush de dados do buffer em segundo plano (omitido ou zero significa que não há parâmetros `flush*`).

Os dados sofrem flush do buffer e são gravados na tabela de destino se todas as condições `min*` ou pelo menos uma condição `max*` forem atendidas.

Além disso, se pelo menos uma condição `flush*` for atendida, um flush será iniciado em segundo plano. Isso difere de `max*`, pois `flush*` permite configurar flushes em segundo plano separadamente, evitando adicionar latência às consultas `INSERT` em tabelas Buffer.

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time`, and `flush_time`
</div>

Condição para o intervalo de tempo, em segundos, a partir do momento da primeira gravação no buffer.

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

Condição para a quantidade de linhas no buffer.

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

Condição para a quantidade de bytes no buffer.

Durante a operação de gravação, os dados são inseridos em um ou mais buffers aleatórios (configurados com `num_layers`). Ou, se a parte de dados a ser inserida for grande o suficiente (maior que `max_rows` ou `max_bytes`), ela é gravada diretamente na tabela de destino, sem passar pelo buffer.

As condições para o flush dos dados são calculadas separadamente para cada um dos buffers de `num_layers`. Por exemplo, se `num_layers = 16` e `max_bytes = 100000000`, o consumo máximo de RAM é 1,6 GB.

Exemplo:

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Criando uma tabela `merge.hits_buffer` com a mesma estrutura de `merge.hits` e usando o motor Buffer. Ao gravar nessa tabela, os dados são mantidos em buffer na RAM e depois gravados na tabela &#39;merge.hits&#39;. Um único buffer é criado, e os dados sofrem flush se qualquer uma das condições abaixo ocorrer:

* 100 segundos tiverem se passado desde o último flush (`max_time`) ou
* 1 milhão de linhas tiver sido gravado (`max_rows`) ou
* 100 MB de dados tiverem sido gravados (`max_bytes`) ou
* 10 segundos tiverem se passado (`min_time`) e 10.000 linhas (`min_rows`) e 10 MB (`min_bytes`) de dados tiverem sido gravados

Por exemplo, se apenas uma linha tiver sido gravada, depois de 100 segundos ela sofrerá flush, independentemente de qualquer outra coisa. Mas, se muitas linhas tiverem sido gravadas, o flush dos dados acontecerá antes.

Quando o servidor é interrompido, com `DROP TABLE` ou `DETACH TABLE`, os dados em buffer também sofrem flush para a tabela de destino.

Você pode definir strings vazias entre aspas simples para o nome do banco de dados e da tabela. Isso indica a ausência de uma tabela de destino. Nesse caso, quando as condições de flush dos dados são atingidas, o buffer é simplesmente limpo. Isso pode ser útil para manter uma janela de dados na memória.

Ao ler de uma tabela Buffer, os dados são processados tanto do buffer quanto da tabela de destino (se houver).
Observe que a tabela Buffer não oferece suporte a índice. Em outras palavras, os dados no buffer são totalmente varridos, o que pode ser lento para buffers grandes. (Para dados em uma tabela subjacente, será usado o índice compatível com ela.)

Se o conjunto de colunas da tabela Buffer não corresponder ao conjunto de colunas de uma tabela subjacente, será inserido um subconjunto das colunas existentes em ambas as tabelas.

Se os tipos não corresponderem para uma das colunas da tabela Buffer e de uma tabela subjacente, uma mensagem de erro será registrada no log do servidor, e o buffer será limpo.
O mesmo acontece se a tabela subjacente não existir quando o buffer sofrer flush.

:::note
Executar ALTER na tabela Buffer em lançamentos anteriores a 26 de out. de 2021 causará um erro `Block structure mismatch` (consulte [#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) e [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565)), portanto excluir a tabela Buffer e recriá-la depois é a única opção. Verifique se esse erro foi corrigido no seu lançamento antes de tentar executar ALTER na tabela Buffer.
:::

Se o servidor for reiniciado de forma anormal, os dados no buffer serão perdidos.

`FINAL` e `SAMPLE` não funcionam corretamente para tabelas Buffer. Essas condições são passadas para a tabela de destino, mas não são usadas para processar os dados no buffer. Se esses recursos forem necessários, recomendamos usar a tabela Buffer apenas para gravação e ler a partir da tabela de destino.

Ao adicionar dados a uma tabela Buffer, um dos buffers é bloqueado. Isso causa atrasos se uma operação de leitura estiver sendo executada simultaneamente na tabela.

Os dados inseridos em uma tabela Buffer podem acabar na tabela subjacente em uma ordem diferente e em blocos diferentes. Por isso, é difícil usar corretamente uma tabela Buffer para gravar em uma CollapsingMergeTree. Para evitar problemas, você pode definir `num_layers` como 1.

Se a tabela de destino for replicada, algumas características esperadas de tabelas replicadas são perdidas ao gravar em uma tabela Buffer. As mudanças aleatórias na ordem das linhas e nos tamanhos das partes de dados fazem com que a desduplicação deixe de funcionar, o que significa que não é possível ter uma gravação confiável &#39;exactly once&#39; em tabelas replicadas.

Devido a essas desvantagens, só podemos recomendar o uso de uma tabela Buffer em casos raros.

Uma tabela Buffer é usada quando são recebidos INSERTs demais de um grande número de servidores em um determinado intervalo de tempo, e os dados não podem ser mantidos em buffer antes da inserção, o que significa que os INSERTs não conseguem ser executados com rapidez suficiente.

Observe que não faz sentido inserir dados uma linha por vez, mesmo em tabelas Buffer. Isso resultará em uma velocidade de apenas alguns milhares de linhas por segundo, enquanto inserir blocos maiores de dados pode alcançar mais de um milhão de linhas por segundo.