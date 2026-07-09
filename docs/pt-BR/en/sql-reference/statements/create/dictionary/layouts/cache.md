---
slug: /sql-reference/statements/create/dictionary/layouts/cache
title: 'layout de dicionário com cache'
sidebar_label: 'cache'
sidebar_position: 6
description: 'Armazena um dicionário em um cache em memória de tamanho fixo.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

O tipo de layout de dicionário `cached` armazena o dicionário em um cache com um número fixo de células.
Essas células contêm elementos usados com frequência.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

Ao buscar em um dicionário, o cache é consultado primeiro. Para cada bloco de dados, todas as chaves que não forem encontradas no cache ou que estiverem desatualizadas serão solicitadas da origem usando `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Os dados recebidos são então gravados no cache.

Se as chaves não forem encontradas no dicionário, uma tarefa de atualização do cache será criada e adicionada à fila de atualização. As propriedades da fila de atualização podem ser controladas com as configurações `max_update_queue_size`, `update_queue_push_timeout_milliseconds`, `query_wait_timeout_milliseconds`, `max_threads_for_updates`.

Para dicionários de cache, é possível definir o [ciclo de vida](../lifetime.md) de expiração dos dados no cache. Se tiver passado mais tempo do que `lifetime` desde o carregamento dos dados em uma célula, o valor da célula não será usado e a chave expirará. A chave será solicitada novamente na próxima vez em que precisar ser usada. Esse comportamento pode ser configurado com a configuração `allow_read_expired_keys`.

Esta é a menos eficaz de todas as formas de armazenar dicionários. A velocidade do cache depende fortemente das configurações corretas e do cenário de uso. Um dicionário do tipo cache tem bom desempenho apenas quando as taxas de acerto são altas o suficiente (recomenda-se 99% ou mais). Você pode ver a taxa média de acerto na tabela [system.dictionaries](/pt-BR/operations/system-tables/dictionaries.md).

Se a configuração `allow_read_expired_keys` estiver definida como 1, sendo 0 o valor padrão, o dicionário poderá oferecer suporte a atualizações assíncronas. Se um client solicitar chaves e todas elas estiverem no cache, mas algumas estiverem expiradas, o dicionário retornará as chaves expiradas ao client e as solicitará da origem de forma assíncrona.

Para melhorar o desempenho do cache, use uma subconsulta com `LIMIT` e chame a função com o dicionário externamente.

Todos os tipos de origem são compatíveis.

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
        <cache>
            <!-- O tamanho do cache, em número de células. Arredondado para cima para uma potência de dois. -->
            <size_in_cells>1000000000</size_in_cells>
            <!-- Permite ler chaves expiradas. -->
            <allow_read_expired_keys>0</allow_read_expired_keys>
            <!-- Tamanho máximo da fila de atualização. -->
            <max_update_queue_size>100000</max_update_queue_size>
            <!-- Tempo limite máximo em milissegundos para enviar a tarefa de atualização para a fila. -->
            <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
            <!-- Tempo limite máximo de espera em milissegundos para a conclusão da tarefa de atualização. -->
            <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
            <!-- Número máximo de threads para atualização do dicionário de cache. -->
            <max_threads_for_updates>4</max_threads_for_updates>
        </cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Defina um tamanho de cache grande o suficiente. Você precisará fazer testes para selecionar o número de células:

1. Defina um valor.
2. Execute consultas até que o cache esteja completamente cheio.
3. Avalie o consumo de memória usando a tabela `system.dictionaries`.
4. Aumente ou diminua o número de células até atingir o consumo de memória desejado.

:::note
O ClickHouse não é recomendado como origem para esse layout. As buscas no dicionário exigem leituras pontuais aleatórias, que não correspondem ao padrão de acesso para o qual o ClickHouse foi otimizado.
:::