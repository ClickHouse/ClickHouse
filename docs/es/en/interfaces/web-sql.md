---
description: 'Documentación de la Interfaz web SQL (Play), la interfaz de consultas integrada en el navegador disponible en `/play`'
sidebar_label: 'Interfaz web SQL'
sidebar_position: 21
slug: /interfaces/web-sql
title: 'Interfaz web SQL (Play)'
doc_type: 'reference'
---

La Interfaz web SQL (Play) es la interfaz de consultas integrada en el navegador de ClickHouse. Está disponible en cualquier puerto HTTP de ClickHouse en la ruta `/play` (por ejemplo, `http://localhost:8123/play`). Permite escribir y ejecutar consultas, ver los resultados como una tabla o un gráfico, y compartir una consulta copiando su URL.

Toda la interfaz se encuentra en `programs/server/play.html`, una única página autocontenida servida directamente desde el binario de ClickHouse, sin frameworks ni proceso de compilación. La única excepción es el renderizado de gráficos: la biblioteca de gráficos `uPlot` se carga de forma diferida desde una CDN de terceros la primera vez que un resultado se muestra como gráfico, por lo que los gráficos no están disponibles en entornos sin conexión o con la salida restringida.

<div id="query-tabs">
  ## Pestañas de consulta
</div>

Las pestañas te permiten tener varias consultas lado a lado, en lugar de tener que manejarlas en un solo editor o depender del historial del navegador.

Cada pestaña tiene su propio texto de la consulta, título, parámetros de consulta y último resultado. La configuración de conexión (URL, usuario, contraseña) sigue siendo global y se comparte entre todas las pestañas.

<div id="when-the-tab-bar-appears">
  ### Cuándo aparece la barra de pestañas
</div>

La barra de pestañas aparece cuando se ha ejecutado una consulta o cuando hay más de una pestaña. Una única pestaña sin resultados se ve exactamente igual que la página antes de que existieran las pestañas, así que la barra de pestañas no se muestra hasta que hace falta.

La pestaña activa se integra visualmente en la página: su fondo adopta el color hash de la consulta (el mismo color que ya usa el fondo de la página), con un degradado más saturado en la parte superior en el tema claro y más brillante en la parte superior en el tema oscuro. Las pestañas inactivas se tiñen según el hash de su propio texto de consulta, por lo que las distintas pestañas se distinguen automáticamente por color.

<div id="creating-closing-and-renaming-tabs">
  ### Crear, cerrar y renombrar pestañas
</div>

* Crea una pestaña nueva con el botón `[+]` situado a la derecha de las pestañas.
* Cierra una pestaña con el icono `x` de la pestaña.
* Las pestañas nuevas reciben nombres predeterminados como `Query A`, `Query B`, y así sucesivamente.
* Haz clic en el título de la pestaña activa para editarlo directamente; el campo de edición se expande para ajustarse al texto.

<div id="switching-tabs">
  ### Cambiar entre pestañas
</div>

* Haz clic en una pestaña inactiva para cambiar a ella.
* Desplaza la rueda del ratón sobre el panel de pestañas para cambiar de pestaña: al desplazarte hacia arriba, pasarás a la pestaña de la izquierda; al desplazarte hacia abajo, a la pestaña de la derecha (si existen). Funciona tanto el desplazamiento vertical como el horizontal de la rueda.

La barra de pestañas permanece fija horizontalmente —se mantiene a la izquierda durante el desplazamiento horizontal de la página, como el logotipo de ClickHouse en la parte inferior— y se desplaza verticalmente junto con el resto de la página.

<div id="persistence-and-browser-history">
  ### Persistencia e historial del navegador
</div>

El espacio de trabajo —las pestañas, sus títulos, la pestaña activa, su orden y pequeñas instantáneas de resultados— se guarda en IndexedDB y se restaura al recargar. La persistencia se realiza en la medida de lo posible: si IndexedDB no está disponible, el espacio de trabajo pasa a un estado en memoria durante la sesión actual.

Las pestañas también se integran con la History API del navegador y la URL:

* El estado del historial incluye la pestaña activa, por lo que los botones de retroceso y avance del navegador cambian de pestaña.
* La URL incorpora un parámetro `tab=<name>`. Al cargar, la consulta de la URL y el parámetro `tab` se sincronizan con las pestañas guardadas: se reutiliza una pestaña existente con ese nombre (y se sustituye su consulta), o se crea una nueva si no se encuentra ese nombre o no tenía nombre. Esto permite abrir una URL con una consulta nueva mientras se conservan las pestañas guardadas.

<div id="limitations">
  ### Limitaciones
</div>

Cambiar de pestaña mientras una consulta se está ejecutando descarta el estado de ejecución de esa consulta.

Solo se crean instantáneas de resultados pequeños para poder restaurarlos. Un resultado grande (por encima del límite de tamaño de la instantánea) o un resultado en forma de imagen no se guarda: después de cambiar de pestaña o recargar, la pestaña conserva su consulta, pero no el resultado mostrado, y volver a ejecutar la consulta lo reproduce. Esto se aplica tanto a los resultados de una sola consulta como a la salida combinada de una ejecución de &quot;Run all&quot; (multiconsulta).