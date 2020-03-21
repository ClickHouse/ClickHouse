# Historial de ClickHouse {#clickhouse-history}

ClickHouse fue desarrollado originalmente para alimentar [El Yandex.Métrica](https://metrica.yandex.com/), [la segunda plataforma de análisis web más grande del mundo](http://w3techs.com/technologies/overview/traffic_analysis/all), y sigue siendo el componente central de este sistema. Con más de 13 billones de registros en la base de datos y más de 20 mil millones de eventos diarios, ClickHouse permite generar informes personalizados sobre la marcha directamente a partir de datos no agregados. Este artículo cubre brevemente los objetivos de ClickHouse en las primeras etapas de su desarrollo.

El Yandex.Metrica construye informes personalizados sobre la marcha basados en hits y sesiones, con segmentos arbitrarios definidos por el usuario. Esto a menudo requiere la creación de agregados complejos, como el número de usuarios únicos. Los nuevos datos para crear un informe se reciben en tiempo real.

A partir de abril de 2014, Yandex.Metrica estaba rastreando alrededor de 12 mil millones de eventos (vistas de páginas y clics) diariamente. Todos estos eventos deben almacenarse para crear informes personalizados. Una sola consulta puede requerir escanear millones de filas en unos pocos cientos de milisegundos, o cientos de millones de filas en solo unos segundos.

## Uso en el Yandex.Metrica y otros servicios de Yandex {#usage-in-yandex-metrica-and-other-yandex-services}

ClickHouse se utiliza para múltiples propósitos en Yandex.Métrica.
Su tarea principal es crear informes en modo en línea utilizando datos no agregados. Utiliza un clúster de 374 servidores, que almacenan más de 20,3 billones de filas en la base de datos. El volumen de datos comprimidos, sin contar la duplicación y la replicación, es de aproximadamente 2 PB. El volumen de datos sin comprimir (en formato TSV) sería de aproximadamente 17 PB.

ClickHouse también se utiliza para:

-   Almacenamiento de datos para Session Replay de Yandex.Métrica.
-   Procesamiento de datos intermedios.
-   Creación de informes globales con Analytics.
-   Ejecutar consultas para depurar el Yandex.Motor Metrica.
-   Análisis de registros desde la API y la interfaz de usuario.

ClickHouse tiene al menos una docena de instalaciones en otros servicios de Yandex: en verticales de búsqueda, Market, Direct, análisis de negocios, desarrollo móvil, AdFox, servicios personales y otros.

## Datos agregados y no agregados {#aggregated-and-non-aggregated-data}

Existe una opinión popular de que para calcular efectivamente las estadísticas, debe agregar datos ya que esto reduce el volumen de datos.

Pero la agregación de datos es una solución muy limitada, por las siguientes razones:

-   Debe tener una lista predefinida de informes que el usuario necesitará.
-   El usuario no puede hacer informes personalizados.
-   Al agregar una gran cantidad de claves, el volumen de datos no se reduce y la agregación es inútil.
-   Para un gran número de informes, hay demasiadas variaciones de agregación (explosión combinatoria).
-   Al agregar claves con alta cardinalidad (como las URL), el volumen de datos no se reduce en mucho (menos del doble).
-   Por esta razón, el volumen de datos con agregación podría crecer en lugar de reducirse.
-   Los usuarios no ven todos los informes que generamos para ellos. Una gran parte de esos cálculos es inútil.
-   La integridad lógica de los datos puede ser violada para varias agregaciones.

Si no agregamos nada y trabajamos con datos no agregados, esto podría reducir el volumen de cálculos.

Sin embargo, con la agregación, una parte significativa del trabajo se desconecta y se completa con relativa calma. Por el contrario, los cálculos en línea requieren calcular lo más rápido posible, ya que el usuario está esperando el resultado.

El Yandex.Metrica tiene un sistema especializado para agregar datos llamado Metrage, que se utiliza para la mayoría de los informes.
A partir de 2009, Yandex.Metrica también utilizó una base de datos OLAP especializada para datos no agregados llamada OLAPServer, que anteriormente se usaba para el generador de informes.
OLAPServer funcionó bien para datos no agregados, pero tenía muchas restricciones que no permitían que se utilizara para todos los informes según lo deseado. Estos incluyeron la falta de soporte para tipos de datos (solo números) y la incapacidad de actualizar datos de forma incremental en tiempo real (solo se podía hacer reescribiendo datos diariamente). OLAPServer no es un DBMS, sino una base de datos especializada.

Para eliminar las limitaciones de OLAPServer y resolver el problema de trabajar con datos no agregados para todos los informes, desarrollamos el DBMS ClickHouse.

[Artículo Original](https://clickhouse.tech/docs/es/introduction/history/) <!--hide-->
