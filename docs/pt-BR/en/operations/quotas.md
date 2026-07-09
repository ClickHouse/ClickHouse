---
description: 'Guia para configurar e gerenciar QUOTAS de uso de recursos no ClickHouse'
sidebar_label: 'QUOTAS'
sidebar_position: 51
slug: /operations/quotas
title: 'QUOTAS'
doc_type: 'guide'
---

:::note QUOTAS no ClickHouse Cloud
As QUOTAS são compatíveis com o ClickHouse Cloud, mas devem ser criadas usando a [sintaxe DDL](/pt-BR/sql-reference/statements/create/quota). A abordagem de configuração XML documentada abaixo **não tem suporte**.
:::

As QUOTAS permitem limitar o uso de recursos ao longo de um período ou acompanhar o consumo de recursos.
As QUOTAS são configuradas na configuração do usuário, que geralmente é &#39;users.xml&#39;.

O sistema também tem um recurso para limitar a complexidade de uma única consulta. Consulte a seção [Restrições à complexidade de consultas](../operations/settings/query-complexity.md).

Em contraste com as restrições de complexidade de consultas, as QUOTAS:

* Impõem restrições a um conjunto de consultas que podem ser executadas ao longo de um período, em vez de limitar uma única consulta.
* Contabilizam os recursos consumidos em todos os servidores remotos no processamento distribuído de consultas.

Vamos analisar a seção do arquivo &#39;users.xml&#39; que define as QUOTAS.

```xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <query_selects>0</query_selects>
            <query_inserts>0</query_inserts>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

Por padrão, a QUOTA monitora o consumo de recursos a cada hora, sem limitar o uso.
O consumo de recursos calculado para cada intervalo é registrado no log do servidor após cada requisição.

```xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <query_selects>100</query_selects>
        <query_inserts>100</query_inserts>
        <written_bytes>5000000</written_bytes>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
        <failed_sequential_authentications>5</failed_sequential_authentications>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <query_selects>10000</query_selects>
        <query_inserts>10000</query_inserts>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <result_bytes>160000000000</result_bytes>
        <read_rows>500000000000</read_rows>
        <result_bytes>16000000000000</result_bytes>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

Para a QUOTA &#39;statbox&#39;, as restrições são definidas para cada hora e para cada período de 24 horas (86.400 segundos). O intervalo de tempo é contado a partir de um momento fixo no tempo definido pela implementação. Em outras palavras, o intervalo de 24 horas não começa necessariamente à meia-noite.

Quando o intervalo termina, todos os valores coletados são zerados. Na hora seguinte, o cálculo da QUOTA recomeça.

Aqui estão as quantidades que podem ser restringidas:

`queries` – O número total de solicitações.

`query_selects` – O número total de solicitações SELECT.

`query_inserts` – O número total de solicitações INSERT.

`errors` – O número de consultas que geraram uma exceção.

`result_rows` – O número total de linhas retornadas como resultado.

`result_bytes` - O tamanho total das linhas retornadas como resultado.

`read_rows` – O número total de linhas de origem lidas das tabelas para executar a consulta em todos os servidores remotos.

`read_bytes` - O tamanho total lido das tabelas para executar a consulta em todos os servidores remotos.

`written_bytes` - O tamanho total de uma operação de gravação.

`execution_time` – O tempo total de execução da consulta, em segundos (tempo real decorrido).

`failed_sequential_authentications` - O número total de erros sequenciais de autenticação.

`queries_per_normalized_hash` – O número máximo de execuções de qualquer consulta normalizada. Consultas normalizadas são consultas com literais substituídos por placeholders, portanto `SELECT 1` e `SELECT 2` são consideradas a mesma consulta normalizada. Esse limite é acompanhado de forma independente para cada padrão de consulta normalizada distinto.

Se o limite for excedido em pelo menos um intervalo de tempo, uma exceção será lançada com um texto informando qual restrição foi excedida, para qual intervalo e quando o novo intervalo começa (quando as consultas podem ser enviadas novamente).

As QUOTAS podem usar o recurso &quot;quota key&quot; para informar o uso de recursos de várias chaves de forma independente. Veja um exemplo:

```xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip />, so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)

        Instead of <keyed_by_ip /> you can use <keyed_by_forwarded_ip />, so the address
        from the X-Forwarded-For header is used as the quota key.

        For both <keyed_by_ip /> and <keyed_by_forwarded_ip /> you can additionally specify
        <ipv4_prefix_bits> and <ipv6_prefix_bits> to group clients by subnet instead of by a
        single address: the IP address is masked to the given prefix length before being used
        as the quota key. For example, <ipv4_prefix_bits>24</ipv4_prefix_bits> shares one bucket
        across a /24 IPv4 subnet, and <ipv6_prefix_bits>64</ipv6_prefix_bits> across a /64 IPv6
        subnet. These elements can only be used together with <keyed_by_ip /> or
        <keyed_by_forwarded_ip />.
    -->
    <keyed />
```

Você também pode associar QUOTAS ao hash da consulta normalizada, para que cada padrão de consulta distinto tenha seu próprio bucket de QUOTA independente. Na configuração XML, isso é escrito como `<keyed_by_normalized_query_hash />`:

```xml
<my_quota>
    <keyed_by_normalized_query_hash />
    <interval>
        <duration>3600</duration>
        <queries>100</queries>
    </interval>
</my_quota>
```

O mesmo pode ser expresso usando a sintaxe DDL:

```sql
CREATE QUOTA my_quota KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO my_user;
```

Neste exemplo, o usuário pode executar até 100 ocorrências de cada consulta normalizada distinta por hora. `SELECT number FROM numbers(1)` e `SELECT number FROM numbers(2)` compartilham o mesmo bucket (porque têm a mesma forma normalizada), mas `SELECT number, number FROM numbers(1)` usa um bucket separado.

A QUOTA é atribuída aos usuários na seção &#39;users&#39; da configuração. Consulte a seção &quot;Direitos de acesso&quot;.

No processamento distribuído de consultas, os valores acumulados são armazenados no servidor solicitante. Portanto, se o usuário acessar outro servidor, a QUOTA lá vai &quot;começar do zero&quot;.

Quando o servidor é reiniciado, as QUOTAS são redefinidas.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Criando aplicativos de página única com ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)