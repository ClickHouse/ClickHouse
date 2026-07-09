---
description: 'Diccionarios geobase incorporados en ClickHouse'
sidebar_label: 'Diccionarios incorporados'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: 'Diccionarios incorporados (geobase)'
doc_type: 'referencia'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse contiene una funcionalidad integrada para trabajar con una geobase.

Esto permite:

* Usar el ID de una región para obtener su nombre en el idioma deseado.
* Usar el ID de una región para obtener el ID de una ciudad, área, distrito federal, país o continente.
* Comprobar si una región forma parte de otra región.
* Obtener una cadena de regiones padre.

Todas las funciones admiten la &quot;translocalidad&quot;, es decir, la capacidad de usar simultáneamente distintas perspectivas sobre la pertenencia de una región. Para obtener más información, consulte la sección &quot;Funciones para trabajar con diccionarios de analítica web&quot;.

Los diccionarios internos están deshabilitados en el paquete predeterminado.
Para habilitarlos, quite el comentario de los parámetros `path_to_regions_hierarchy_file` y `path_to_regions_names_files` en el archivo de configuración del servidor.

La geobase se carga a partir de archivos de texto.

Coloque los archivos `regions_hierarchy*.txt` en el directorio `path_to_regions_hierarchy_file`. Este parámetro de configuración debe contener la ruta al archivo `regions_hierarchy.txt` (la jerarquía regional predeterminada), y los demás archivos (`regions_hierarchy_ua.txt`) deben estar ubicados en el mismo directorio.

Coloque los archivos `regions_names_*.txt` en el directorio `path_to_regions_names_files`.

También puede crear estos archivos usted mismo. El formato del archivo es el siguiente:

`regions_hierarchy*.txt`: TabSeparated (sin encabezado), columnas:

* ID de región (`UInt32`)
* ID de la región padre (`UInt32`)
* tipo de región (`UInt8`): 1 - continente, 3 - país, 4 - distrito federal, 5 - región, 6 - ciudad; los demás tipos no tienen valores
* población (`UInt32`) — columna opcional

`regions_names_*.txt`: TabSeparated (sin encabezado), columnas:

* ID de región (`UInt32`)
* nombre de la región (`String`) — No puede contener tabulaciones ni saltos de línea, ni siquiera escapados.

Se usa un array plano para almacenarlo en RAM. Por este motivo, los ID no deben superar un millón.

Los diccionarios se pueden actualizar sin reiniciar el servidor. Sin embargo, el conjunto de diccionarios disponibles no se actualiza.
Para las actualizaciones, se comprueban las fechas de modificación de los archivos. Si un archivo ha cambiado, el diccionario se actualiza.
El intervalo para comprobar cambios se configura en el parámetro `builtin_dictionaries_reload_interval`.
Las actualizaciones de los diccionarios (distintas de la carga en el primer uso) no bloquean las consultas. Durante las actualizaciones, las consultas usan las versiones anteriores de los diccionarios. Si se produce un error durante una actualización, el error se escribe en el log del servidor y las consultas siguen usando la versión anterior de los diccionarios.

Recomendamos actualizar periódicamente los diccionarios de la geobase. Durante una actualización, genere archivos nuevos y escríbalos en una ubicación independiente. Cuando todo esté listo, cámbieles el nombre para que coincidan con los archivos que usa el servidor.

También hay funciones para trabajar con identificadores de sistemas operativos y motores de búsqueda, pero no deberían usarse.