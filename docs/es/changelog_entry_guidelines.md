---
title: directriz para las entradas del registro de cambios
---

<div id="changelog-entry-guidelines">
  # Directrices para las entradas del registro de cambios
</div>

Las buenas entradas del registro de cambios ayudan a los usuarios a entender rápidamente qué hay de nuevo y cómo les afecta. Pedimos a los colaboradores que redacten una entrada del registro de cambios pensada para el usuario, que se incluirá en el registro de cambios de cada lanzamiento.

A continuación, presentamos algunas directrices sencillas para redactar una buena entrada del registro de cambios.

<div id="write-with-the-user-in-mind-not-the-developer">
  ## Escribe pensando en el usuario, no en el desarrollador
</div>

La entrada del registro de cambios está pensada para comunicar el cambio al *usuario*, y no solo al *desarrollador*.
Al redactarla, cuando corresponda, intenta comunicar no solo ***qué*** cambió, sino también ***por qué*** ese cambio es útil para el usuario o ***cómo*** le afecta.

Por ejemplo, en lugar de:

> Añade la tabla `system.iceberg_history`

Escribe:

> Ahora los usuarios pueden ver snapshots históricas de tablas Iceberg con la nueva tabla `system.iceberg_history`.

En lugar de:

> Añade las funciones `stringBytesUniq` y `stringBytesEntropy` para buscar datos posiblemente aleatorios o cifrados.

Escribe:

> Ahora puedes detectar datos potencialmente cifrados o aleatorios en tus cadenas con las nuevas funciones `stringBytesUniq` y `stringBytesEntropy`, lo que ayuda a identificar problemas de calidad de los datos o cuestiones de seguridad.

<div id="keep-it-simple">
  ## Mantenlo simple
</div>

Evita la jerga técnica que un usuario podría no entender sin una explicación. Procura que tenga entre 1 y 5 frases, y
no tengas miedo de usar un LLM para detectar erratas, errores gramaticales o reformular la entrada de una
forma más clara para el usuario (¡no es hacer trampa, te lo prometo!)

En lugar de:

> Permitir subconsultas correlacionadas como argumento de la expresión `EXISTS`

Escribe:

> Ahora puedes usar subconsultas que hacen referencia a columnas de la consulta externa dentro de las cláusulas `EXISTS`.

Un ejemplo de una entrada clara y simple:

> Permite ajustar la configuración de la caché de páginas por consulta. Esto permite experimentar más rápido y ofrece la posibilidad de un ajuste avanzado para consultas de alto rendimiento y baja latencia.

<div id="follow-a-few-simple-formatting-guidelines">
  ## Sigue algunas pautas sencillas de formato
</div>

<div id="write-in-full-sentences-and-in-the-present-tense">
  ### Escribe con oraciones completas y en presente
</div>

En lugar de:

> Se corrigió un fallo: si se lanza una excepción al intentar eliminar un archivo temporal

Escribe:

> Corrige un fallo que se produce cuando se lanza una excepción al intentar eliminar un archivo temporal.

<div id="use-backticks-where-necessary">
  ### Usa comillas invertidas cuando sea necesario
</div>

Pon entre comillas invertidas los elementos de código, como settings, nombres de funciones, sentencias SQL, nombres de formatos y tipos de datos. En general,
todo lo que escribirías en `clickhouse-client` debería ir entre comillas invertidas. Esto ayuda a que las entradas del registro de cambios sean más legibles.

En lugar de:

> Settings use&#95;skip&#95;indexes&#95;if&#95;final y use&#95;skip&#95;indexes&#95;if&#95;final&#95;exact&#95;mode ahora tienen True como valor predeterminado

Escribe:

> Settings `use_skip_indexes_if_final` y `use_skip_indexes_if_final_exact_mode` ahora tienen `True` como valor predeterminado

<div id="try-to-follow-a-consistent-format">
  ### Intenta seguir un formato coherente
</div>

Procura seguir el mismo formato: Qué hace → Por qué es importante para el usuario → Cómo usarlo (si es necesario). Esto hace que las entradas sean fáciles de consultar y predecibles para los lectores.

Por ejemplo:

> Ahora puedes filtrar los resultados de la búsqueda vectorial antes o después de la operación de búsqueda, lo que te da un mayor control sobre el equilibrio entre rendimiento y exactitud. Usa la nueva configuración `vector_search_filter_mode` para elegir el enfoque que prefieras.