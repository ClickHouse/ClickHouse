---
description: 'Esta função de tabela permite a integração do ClickHouse ao Redis.'
sidebar_label: 'redis'
sidebar_position: 170
slug: /sql-reference/table-functions/redis
title: 'redis'
doc_type: 'reference'
---

Esta função de tabela permite a integração do ClickHouse ao [Redis](https://redis.io/).

<div id="syntax">
  ## Sintaxe
</div>

```sql
redis(host:port, key, structure[, db_index[, password[, pool_size]]])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento   | Descrição                                                                                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port` | Endereço do servidor Redis; você pode omitir a porta, e a porta padrão do Redis, 6379, será usada.                                                  |
| `key`       | qualquer nome de coluna na lista de colunas.                                                                                                        |
| `structure` | O esquema da tabela do ClickHouse retornada por esta função.                                                                                        |
| `db_index`  | Faixa de índice do banco Redis de 0 a 15; o padrão é 0.                                                                                             |
| `password`  | Senha do usuário; o padrão é uma string vazia.                                                                                                      |
| `pool_size` | Tamanho máximo do pool de conexões do Redis; o padrão é 16.                                                                                         |
| `primary`   | deve ser especificado; oferece suporte a apenas uma coluna na chave primária. A chave primária será serializada em binário como uma chave do Redis. |

* colunas diferentes da chave primária serão serializadas em binário como valor do Redis, na ordem correspondente.
* consultas com filtro pela chave usando equals ou in serão otimizadas para busca de múltiplas chaves no Redis. Se as consultas forem feitas sem filtro pela chave, ocorrerá uma varredura completa da tabela, que é uma operação pesada.

[Coleções nomeadas](/pt-BR/operations/named-collections.md) não têm suporte para a função de tabela `redis` no momento.

<div id="returned_value">
  ## Valor retornado
</div>

Um objeto de tabela, com a chave como chave do Redis e as demais colunas agrupadas como valor do Redis.

<div id="usage-example">
  ## Exemplo de uso
</div>

Leitura do Redis:

```sql
SELECT * FROM redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32'
)
```

Inserir no Redis:

```sql
INSERT INTO TABLE FUNCTION redis(
    'redis1:6379',
    'key',
    'key String, v1 String, v2 UInt32') values ('1', '1', 1);
```

<div id="related">
  ## Relacionados
</div>

* [O motor de tabela `Redis`](/pt-BR/engines/table-engines/integrations/redis.md)
* [Usando o Redis como fonte de dicionário](/pt-BR/sql-reference/statements/create/dictionary/sources/redis)