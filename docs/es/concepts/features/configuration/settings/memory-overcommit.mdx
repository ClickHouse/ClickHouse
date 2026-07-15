---
description: 'Una técnica experimental destinada a permitir definir límites de memoria
  más flexibles para las consultas.'
slug: /operations/settings/memory-overcommit
title: 'Sobreasignación de memoria'
doc_type: 'reference'
---

La sobreasignación de memoria es una técnica experimental destinada a permitir definir límites de memoria más flexibles para las consultas.

La idea de esta técnica es introducir configuraciones que puedan representar la cantidad garantizada de memoria que una consulta puede usar.
Cuando la sobreasignación de memoria está habilitada y se alcanza el límite de memoria, ClickHouse seleccionará la consulta más sobreasignada e intentará liberar memoria finalizando esa consulta.

Cuando se alcanza el límite de memoria, cualquier consulta esperará un tiempo mientras intenta asignar memoria nueva.
Si transcurre el timeout y se libera memoria, la consulta continúa ejecutándose.
De lo contrario, se lanzará una excepción y la consulta se finalizará.

La selección de la consulta que debe detenerse o finalizarse la realizan el rastreador de sobreasignación global o el de usuario, según qué límite de memoria se haya alcanzado.
Si el rastreador de sobreasignación no puede elegir una consulta para detener, se lanza la excepción MEMORY&#95;LIMIT&#95;EXCEEDED.

<div id="user-overcommit-tracker">
  ## Rastreador de sobreasignación por usuario
</div>

El rastreador de sobreasignación por usuario encuentra la consulta con el mayor ratio de overcommit en la lista de consultas del usuario.
El ratio de overcommit de una consulta se calcula como el número de bytes asignados dividido entre el valor de la configuración `memory_overcommit_ratio_denominator_for_user`.

Si `memory_overcommit_ratio_denominator_for_user` de la consulta es igual a cero, el rastreador de sobreasignación no elegirá esa consulta.

El tiempo de espera se establece mediante la configuración `memory_usage_overcommit_max_wait_microseconds`.

**Ejemplo**

```sql
SELECT number FROM numbers(1000) GROUP BY number SETTINGS memory_overcommit_ratio_denominator_for_user=4000, memory_usage_overcommit_max_wait_microseconds=500
```

<div id="global-overcommit-tracker">
  ## Rastreador global de sobreasignación
</div>

El rastreador global de sobreasignación encuentra la consulta con el mayor ratio de overcommit entre todas las consultas.
En este caso, el ratio de overcommit se calcula como el número de bytes asignados dividido por el valor de la configuración `memory_overcommit_ratio_denominator`.

Si `memory_overcommit_ratio_denominator` de la consulta es igual a cero, el rastreador de sobreasignación no seleccionará esta consulta.

El tiempo de espera se establece mediante el parámetro `memory_usage_overcommit_max_wait_microseconds` en el archivo de configuración.