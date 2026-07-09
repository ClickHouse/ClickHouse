---
description: 'Documentación sobre los modos de codificación por colores por columna en la interfaz web de SQL integrada (`/play`)'
sidebar_label: 'Codificación por colores en la interfaz web'
sidebar_position: 23
slug: /interfaces/web-ui-color-coding
title: 'Codificación por colores en la interfaz web'
doc_type: 'reference'
sidebar: false
---

La interfaz web de SQL integrada (`play.html`, disponible en la ruta [`/play`](/es/interfaces/http) de cualquier puerto HTTP de ClickHouse) puede colorear las celdas de los resultados para facilitar la detección de patrones en una columna de un vistazo. Cada columna tiene su propio modo de codificación por colores, que puede activarse o cambiarse de forma independiente.

<div id="switching-the-mode">
  ## Cambiar el modo
</div>

A la derecha del encabezado de cada columna aparece un icono 🌈. Haz clic en él para ir cambiando la columna entre los modos disponibles. En dispositivos con un puntero que admite hover (un ratón), el icono solo se muestra al pasar el cursor sobre el encabezado, por lo que no molesta el resto del tiempo; en dispositivos táctiles y otros dispositivos con punteros imprecisos, que no admiten hover, el icono siempre se muestra para que pueda tocarse directamente.

El conjunto de modos que ofrece una columna depende de su tipo:

* Las columnas numéricas y las columnas `Date`/`DateTime`/`Date32`/`DateTime64` van alternando entre `bar` → `heatmap` → `categorical` → `none`.
* Todas las demás columnas alternan entre `none` y `categorical`.

El modo predeterminado es `bar` para las columnas numéricas y `none` para todas las demás columnas, incluidas las columnas de fecha y hora.

<div id="modes">
  ## Modos
</div>

* **`bar`** — dibuja una barra horizontal en la celda proporcional al valor. En las columnas numéricas, la barra crece a partir de una línea base de cero; en las columnas `Date`/`DateTime`, en cambio, abarca el rango `min`..`max` de la columna, ya que una línea base de cero no tiene sentido para las marcas de tiempo.
* **`heatmap`** — rellena todo el fondo de la celda con un color que codifica el valor, escalado entre el mínimo y el máximo de la columna.
* **`categorical`** — rellena el fondo de la celda con un color derivado del hash del valor de la celda, de modo que los valores iguales reciben el mismo color y los distintos reciben colores diferentes. Esto funciona con cualquier tipo de columna.
* **`none`** — sin codificación por colores.

Las columnas `Date`, `DateTime`, `Date32` y `DateTime64` se colorean según su valor temporal, interpretado en UTC para que la escala sea independiente de la zona horaria del navegador.

Los colores de fondo de `heatmap` y `categorical` usan el espacio de color `oklch`, variando solo el matiz y manteniendo fijas la luminosidad y la croma según el tema, para que el texto de la celda siga siendo legible tanto en el tema claro como en el oscuro. El fondo rellena toda la celda incluso cuando una fila ocupa más de una línea.

<div id="categorical-emphasis">
  ## Énfasis categórico en la selección
</div>

En el modo `categorical`, al seleccionar una celda se destacan las demás celdas que comparten el mismo valor, mostrándose con una fuente de mayor grosor y un color de texto de contraste total (blanco puro en el tema oscuro, negro puro en el tema claro). La celda seleccionada no se destaca. Esto facilita ver en qué otros lugares aparece un valor concreto en la columna.

<div id="persistence">
  ## Persistencia
</div>

Los modos elegidos se guardan por columna en la URL de la página y en el historial del navegador, por lo que al recargar la página, compartir el enlace o navegar hacia atrás y hacia delante, se conservan. Solo se almacenan las opciones que no son las predeterminadas, para mantener compactos la URL y el estado del historial.

<div id="limitations">
  ## Limitaciones
</div>

* El diseño vertical (transpuesto) de una sola fila no muestra ninguna codificación por colores.
* Las diferencias de `DateTime64(9)` inferiores a un microsegundo no se distinguen en la escala de colores, ya que visualmente no tienen sentido en un degradado.