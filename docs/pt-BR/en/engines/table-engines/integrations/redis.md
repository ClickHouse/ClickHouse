---
description: 'Este motor permite integrar o ClickHouse ao Redis.'
sidebar_label: 'Redis'
sidebar_position: 175
slug: /engines/table-engines/integrations/redis
title: 'Motor de tabela Redis'
doc_type: 'guide'
---

Este motor permite integrar o ClickHouse ao [Redis](https://redis.io/). Como o Redis usa o modelo chave-valor, recomendamos fortemente que você faça consultas nele apenas de forma pontual, como `where k=xx` ou `where k in (xx, xx)`.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = Redis({host:port[, db_index[, password[, pool_size]]] | named_collection[, option=value [,..]] })
PRIMARY KEY(primary_key_name);
```

**Parâmetros do motor**

* `host:port` — endereço do servidor Redis; você pode ignorar a porta, e a porta padrão do Redis, 6379, será usada.
* `db_index` — intervalo do índice do banco de dados Redis, de 0 a 15; o padrão é 0.
* `password` — senha do usuário; o padrão é uma string vazia.
* `pool_size` — tamanho máximo do pool de conexões do Redis; o padrão é 16.
* `primary_key_name` - qualquer nome de coluna na lista de colunas.

:::note Serialização
`PRIMARY KEY` aceita apenas uma coluna. A chave primária será serializada em binário como uma chave do Redis.
As colunas que não forem a chave primária serão serializadas em binário como valor do Redis, na ordem correspondente.
:::

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md). Nesse caso, `host` e `port` devem ser especificados separadamente. Essa abordagem é recomendada para o ambiente de produção. No momento, todos os parâmetros passados para o Redis usando coleções nomeadas são obrigatórios.

:::note Filtragem
Consultas com `key equals` ou `in filtering` serão otimizadas para buscas de múltiplas chaves no Redis. Se a consulta não tiver filtragem por chave, ocorrerá uma varredura completa da tabela, que é uma operação pesada.
:::

<div id="usage-example">
  ## Exemplo de uso
</div>

Crie uma tabela no ClickHouse usando o motor `Redis` com argumentos simples:

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis('redis1:6379') PRIMARY KEY(key);
```

Ou usando [coleções nomeadas](/pt-BR/operations/named-collections.md):

```xml
<named_collections>
    <redis_creds>
        <host>localhost</host>
        <port>6379</port>
        <password>****</password>
        <pool_size>16</pool_size>
        <db_index>0</db_index>
    </redis_creds>
</named_collections>
```

```sql title="Query"
CREATE TABLE redis_table
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = Redis(redis_creds) PRIMARY KEY(key);
```

Inserção:

```sql title="Query"
INSERT INTO redis_table VALUES('1', 1, '1', 1.0), ('2', 2, '2', 2.0);
```

```sql title="Query"
SELECT COUNT(*) FROM redis_table;
```

```text title="Response"
┌─count()─┐
│       2 │
└─────────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE key='1';
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 1   │  1 │ 1  │  1 │
└─────┴────┴────┴────┘
```

```sql title="Query"
SELECT * FROM redis_table WHERE v1=2;
```

```text title="Response"
┌─key─┬─v1─┬─v2─┬─v3─┐
│ 2   │  2 │ 2  │  2 │
└─────┴────┴────┴────┘
```

Atualização:

Observe que a chave primária não pode ser atualizada.

```sql title="Query"
ALTER TABLE redis_table UPDATE v1=2 WHERE key='1';
```

Excluir:

```sql title="Query"
ALTER TABLE redis_table DELETE WHERE key='1';
```

TRUNCATE:

Esvazia o Redis db de forma assíncrona. O `TRUNCATE` também oferece suporte ao modo SYNC.

```sql title="Query"
TRUNCATE TABLE redis_table SYNC;
```

Join:

Junção com outras tabelas.

```sql title="Query"
SELECT * FROM redis_table JOIN merge_tree_table ON merge_tree_table.key=redis_table.key;
```

<div id="limitations">
  ## Limitações
</div>

O motor Redis também oferece suporte a consultas de varredura, como `where k > xx`, mas tem algumas limitações:

1. Em casos muito raros, a consulta de varredura pode gerar chaves duplicadas durante o rehashing. Veja os detalhes em [Redis Scan](https://github.com/redis/redis/blob/e4d183afd33e0b2e6e8d1c79a832f678a04a7886/src/dict.c#L1186-L1269).
2. Durante a varredura, chaves podem ser criadas e excluídas, portanto o conjunto de dados resultante não representa um momento válido no tempo.