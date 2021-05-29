---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: Diccionarios internos
---

# Diccionarios internos {#internal_dicts}

ClickHouse contiene una función integrada para trabajar con una geobase.

Esto le permite:

-   Utilice el ID de una región para obtener su nombre en el idioma deseado.
-   Utilice el ID de una región para obtener el ID de una ciudad, área, distrito federal, país o continente.
-   Compruebe si una región es parte de otra región.
-   Obtener una cadena de regiones principales.

Todas las funciones de apoyo “translocality,” la capacidad de utilizar simultáneamente diferentes perspectivas sobre la propiedad de la región. Para obtener más información, consulte la sección “Functions for working with Yandex.Metrica dictionaries”.

Los diccionarios internos están deshabilitados en el paquete predeterminado.
Para habilitarlos, descomente los parámetros `path_to_regions_hierarchy_file` y `path_to_regions_names_files` en el archivo de configuración del servidor.

La geobase se carga desde archivos de texto.

Coloque el `regions_hierarchy*.txt` archivos en el `path_to_regions_hierarchy_file` directorio. Este parámetro de configuración debe contener la ruta `regions_hierarchy.txt` archivo (la jerarquía regional predeterminada), y los otros archivos (`regions_hierarchy_ua.txt`) debe estar ubicado en el mismo directorio.

Ponga el `regions_names_*.txt` archivos en el `path_to_regions_names_files` directorio.

También puede crear estos archivos usted mismo. El formato de archivo es el siguiente:

`regions_hierarchy*.txt`: TabSeparated (sin encabezado), columnas:

-   ID de la región (`UInt32`)
-   ID de región padre (`UInt32`)
-   tipo de región (`UInt8`): 1 - continente, 3 - país, 4 - distrito federal, 5 - región, 6 - ciudad; otros tipos no tienen valores
-   población (`UInt32`) — optional column

`regions_names_*.txt`: TabSeparated (sin encabezado), columnas:

-   ID de la región (`UInt32`)
-   nombre de la región (`String`) — Can't contain tabs or line feeds, even escaped ones.

Una matriz plana se usa para almacenar en RAM. Por esta razón, los ID no deberían ser más de un millón.

Los diccionarios se pueden actualizar sin reiniciar el servidor. Sin embargo, el conjunto de diccionarios disponibles no se actualiza.
Para las actualizaciones, se comprueban los tiempos de modificación de archivos. Si un archivo ha cambiado, el diccionario se actualiza.
El intervalo para comprobar si hay cambios se configura en el `builtin_dictionaries_reload_interval` parámetro.
Las actualizaciones del diccionario (aparte de la carga al primer uso) no bloquean las consultas. Durante las actualizaciones, las consultas utilizan las versiones anteriores de los diccionarios. Si se produce un error durante una actualización, el error se escribe en el registro del servidor y las consultas continúan utilizando la versión anterior de los diccionarios.

Recomendamos actualizar periódicamente los diccionarios con la geobase. Durante una actualización, genere nuevos archivos y escríbalos en una ubicación separada. Cuando todo esté listo, cambie el nombre a los archivos utilizados por el servidor.

También hay funciones para trabajar con identificadores de sistema operativo y Yandex.Motores de búsqueda Metrica, pero no deben ser utilizados.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
