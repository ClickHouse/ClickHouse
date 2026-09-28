---
description: 'Uma técnica experimental destinada a permitir a definição de limites de memória
  mais flexíveis para consultas.'
slug: /operations/settings/memory-overcommit
title: 'Overcommit de memória'
doc_type: 'reference'
---

O overcommit de memória é uma técnica experimental destinada a permitir a definição de limites de memória mais flexíveis para consultas.

A ideia dessa técnica é introduzir configurações que representem a quantidade garantida de memória que uma consulta pode usar.
Quando o overcommit de memória está habilitado e o limite de memória é atingido, o ClickHouse seleciona a consulta com maior overcommit e tenta liberar memória interrompendo essa consulta.

Quando o limite de memória é atingido, qualquer consulta aguarda por algum tempo ao tentar alocar mais memória.
Se o tempo limite expirar e a memória for liberada, a consulta continua a execução.
Caso contrário, uma exceção será lançada e a consulta será interrompida.

A seleção da consulta a ser interrompida é realizada pelo rastreador de overcommit global ou pelo rastreador de overcommit do usuário, dependendo de qual limite de memória foi atingido.
Se o rastreador de overcommit não conseguir escolher uma consulta para interromper, a exceção MEMORY&#95;LIMIT&#95;EXCEEDED será lançada.

<div id="user-overcommit-tracker">
  ## Rastreador de overcommit do usuário
</div>

O rastreador de overcommit do usuário encontra a consulta com a maior razão de overcommit na lista de consultas do usuário.
A razão de overcommit de uma consulta é calculada dividindo-se o número de bytes alocados pelo valor da configuração `memory_overcommit_ratio_denominator_for_user`.

Se `memory_overcommit_ratio_denominator_for_user` da consulta for igual a zero, o rastreador de overcommit não escolherá essa consulta.

O tempo limite de espera é definido pela configuração `memory_usage_overcommit_max_wait_microseconds`.

**Exemplo**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator_for_user=4000, memory_usage_overcommit_max_wait_microseconds=500
```

<div id="global-overcommit-tracker">
  ## Rastreador global de overcommit
</div>

O rastreador global de overcommit encontra a consulta com a maior razão de overcommit na lista de todas as consultas.
Nesse caso, a razão de overcommit é calculada como o número de bytes alocados dividido pelo valor da configuração `memory_overcommit_ratio_denominator`.

Se o valor de `memory_overcommit_ratio_denominator` da consulta for igual a zero, o rastreador de overcommit não escolherá essa consulta.

O tempo limite de espera é definido pelo parâmetro `memory_usage_overcommit_max_wait_microseconds` no arquivo de configuração.