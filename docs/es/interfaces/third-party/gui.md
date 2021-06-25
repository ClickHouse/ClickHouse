---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 28
toc_title: Interfaces Visuales
---

# Interfaces visuales de desarrolladores de terceros {#visual-interfaces-from-third-party-developers}

## De código abierto {#open-source}

### Tabix {#tabix}

Interfaz web para ClickHouse en el [Tabix](https://github.com/tabixio/tabix) proyecto.

Función:

-   Funciona con ClickHouse directamente desde el navegador, sin la necesidad de instalar software adicional.
-   Editor de consultas con resaltado de sintaxis.
-   Autocompletado de comandos.
-   Herramientas para el análisis gráfico de la ejecución de consultas.
-   Opciones de esquema de color.

[Documentación de Tabix](https://tabix.io/doc/).

### Sistema abierto {#houseops}

[Sistema abierto.](https://github.com/HouseOps/HouseOps) Es una interfaz de usuario / IDE para OSX, Linux y Windows.

Función:

-   Generador de consultas con resaltado de sintaxis. Ver la respuesta en una tabla o vista JSON.
-   Exportar resultados de consultas como CSV o JSON.
-   Lista de procesos con descripciones. Modo de escritura. Capacidad de parar (`KILL`) proceso.
-   Gráfico de base de datos. Muestra todas las tablas y sus columnas con información adicional.
-   Una vista rápida del tamaño de la columna.
-   Configuración del servidor.

Las siguientes características están planificadas para el desarrollo:

-   Gestión de bases de datos.
-   Gestión de usuarios.
-   Análisis de datos en tiempo real.
-   Supervisión de clúster.
-   Gestión de clústeres.
-   Monitoreo de tablas replicadas y Kafka.

### Faro {#lighthouse}

[Faro](https://github.com/VKCOM/lighthouse) Es una interfaz web ligera para ClickHouse.

Función:

-   Lista de tablas con filtrado y metadatos.
-   Vista previa de la tabla con filtrado y clasificación.
-   Ejecución de consultas de sólo lectura.

### Redash {#redash}

[Redash](https://github.com/getredash/redash) es una plataforma para la visualización de datos.

Admite múltiples fuentes de datos, incluido ClickHouse, Redash puede unir los resultados de consultas de diferentes fuentes de datos en un conjunto de datos final.

Función:

-   Potente editor de consultas.
-   Explorador de base de datos.
-   Herramientas de visualización, que le permiten representar datos en diferentes formas.

### DBeaver {#dbeaver}

[DBeaver](https://dbeaver.io/) - Cliente de base de datos de escritorio universal con soporte ClickHouse.

Función:

-   Desarrollo de consultas con resaltado de sintaxis y autocompletado.
-   Lista de tablas con filtros y búsqueda de metadatos.
-   Vista previa de datos de tabla.
-   Búsqueda de texto completo.

### Sistema abierto {#clickhouse-cli}

[Sistema abierto.](https://github.com/hatarist/clickhouse-cli) es un cliente de línea de comandos alternativo para ClickHouse, escrito en Python 3.

Función:

-   Autocompletado.
-   Resaltado de sintaxis para las consultas y la salida de datos.
-   Soporte de buscapersonas para la salida de datos.
-   Comandos similares a PostgreSQL personalizados.

### Sistema abierto {#clickhouse-flamegraph}

[Sistema abierto.](https://github.com/Slach/clickhouse-flamegraph) es una herramienta especializada para visualizar el `system.trace_log` como [Flamegraph](http://www.brendangregg.com/flamegraphs.html).

### Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica {#clickhouse-plantuml}

[Método de codificación de datos:](https://pypi.org/project/clickhouse-plantuml/) es un script para generar [PlantUML](https://plantuml.com/) diagrama de esquemas de tablas.

## Comercial {#commercial}

### DataGrip {#datagrip}

[DataGrip](https://www.jetbrains.com/datagrip/) Es un IDE de base de datos de JetBrains con soporte dedicado para ClickHouse. También está integrado en otras herramientas basadas en IntelliJ: PyCharm, IntelliJ IDEA, GoLand, PhpStorm y otros.

Función:

-   Finalización de código muy rápida.
-   Resaltado de sintaxis de ClickHouse.
-   Soporte para características específicas de ClickHouse, por ejemplo, columnas anidadas, motores de tablas.
-   Editor de datos.
-   Refactorizaciones.
-   Búsqueda y navegación.

### Yandex DataLens {#yandex-datalens}

[Yandex DataLens](https://cloud.yandex.ru/services/datalens) es un servicio de visualización y análisis de datos.

Función:

-   Amplia gama de visualizaciones disponibles, desde simples gráficos de barras hasta paneles complejos.
-   Los paneles podrían ponerse a disposición del público.
-   Soporte para múltiples fuentes de datos, incluyendo ClickHouse.
-   Almacenamiento de datos materializados basados en ClickHouse.

Nivel de Cifrado WEP [disponible de forma gratuita](https://cloud.yandex.com/docs/datalens/pricing) para proyectos de baja carga, incluso para uso comercial.

-   [Documentación de DataLens](https://cloud.yandex.com/docs/datalens/).
-   [Tutorial](https://cloud.yandex.com/docs/solutions/datalens/data-from-ch-visualization) en la visualización de datos de una base de datos ClickHouse.

### Software de Holística {#holistics-software}

[Holística](https://www.holistics.io/) es una plataforma de datos de pila completa y una herramienta de inteligencia de negocios.

Función:

-   Correo electrónico automatizado, Slack y horarios de informes de Google Sheet.
-   Editor SQL con visualizaciones, control de versiones, autocompletado, componentes de consulta reutilizables y filtros dinámicos.
-   Análisis integrado de informes y cuadros de mando a través de iframe.
-   Preparación de datos y capacidades ETL.
-   Soporte de modelado de datos SQL para mapeo relacional de datos.

### Mirador {#looker}

[Mirador](https://looker.com) Es una plataforma de datos y una herramienta de inteligencia de negocios con soporte para más de 50 dialectos de bases de datos, incluido ClickHouse. Bravo está disponible como una plataforma SaaS y auto-organizada. Los usuarios pueden utilizar Looker a través del navegador para explorar datos, crear visualizaciones y paneles, programar informes y compartir sus conocimientos con colegas. Looker proporciona un amplio conjunto de herramientas para incrustar estas características en otras aplicaciones y una API
para integrar datos con otras aplicaciones.

Función:

-   Desarrollo fácil y ágil utilizando LookML, un lenguaje que soporta curado
    [Modelado de datos](https://looker.com/platform/data-modeling) para apoyar a los redactores de informes y a los usuarios finales.
-   Potente integración de flujo de trabajo a través de Looker's [Acciones de datos](https://looker.com/platform/actions).

[Cómo configurar ClickHouse en Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/third-party/gui/) <!--hide-->
