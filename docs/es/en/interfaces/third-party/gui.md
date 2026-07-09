---
description: 'Lista de herramientas GUI y aplicaciones de terceros para trabajar con ClickHouse'
sidebar_label: 'Interfaces visuales'
sidebar_position: 28
slug: /interfaces/third-party/gui
title: 'Interfaces visuales de terceros'
doc_type: 'reference'
---

<div id="open-source">
  ## Código abierto
</div>

<div id="agx">
  ### agx
</div>

[agx](https://github.com/agnosticeng/agx) es una aplicación de escritorio creada con Tauri y SvelteKit que ofrece una interfaz moderna para explorar y consultar datos mediante el motor de base de datos embebido de ClickHouse (chdb).

* Aprovecha chdb al ejecutar la aplicación nativa.
* Puede conectarse a una instancia de ClickHouse al ejecutar la versión web.
* Editor Monaco para que te sientas como en casa.
* Múltiples visualizaciones de datos en constante evolución.

<div id="ch-ui">
  ### ch-ui
</div>

[ch-ui](https://github.com/caioricciuti/ch-ui) es una interfaz de aplicación sencilla en React.js para bases de datos ClickHouse, diseñada para ejecutar consultas y visualizar datos. Desarrollada con React y el Client de ClickHouse para la web, ofrece una UI moderna y fácil de usar para interactuar con la base de datos.

Características:

* Integración con ClickHouse: administra conexiones y ejecuta consultas fácilmente.
* Gestión adaptable de pestañas: maneja dinámicamente varias pestañas, como las de consultas y tablas.
* Optimizaciones de rendimiento: utiliza Indexed DB para un almacenamiento en caché y una gestión del estado eficientes.
* Almacenamiento local de datos: todos los datos se almacenan localmente en el navegador, lo que garantiza que no se envíen a ningún otro lugar.

<div id="chartdb">
  ### ChartDB
</div>

[ChartDB](https://chartdb.io) es una herramienta gratuita y de código abierto para visualizar y diseñar esquemas de bases de datos, incluido ClickHouse, con una sola consulta. Desarrollada con React, ofrece una experiencia fluida y fácil de usar, sin necesidad de credenciales de base de datos ni de registrarse para empezar.

Características:

* Visualización de esquemas: Importa y visualiza al instante tu esquema de ClickHouse, incluidos diagramas ER con vistas materializadas y vistas estándar, que muestran referencias a las tablas.
* Exportación de DDL con IA: Genera scripts DDL fácilmente para mejorar la gestión y la documentación de esquemas.
* Compatibilidad con múltiples dialectos SQL: Compatible con diversos dialectos SQL, lo que la hace versátil para distintos entornos de bases de datos.
* Sin necesidad de registro ni credenciales: Todas las funciones están disponibles directamente en el navegador, de forma sencilla y segura.

[Código fuente de ChartDB](https://github.com/chartdb/chartdb).

<div id="datastoria">
  ### DataStoria
</div>

[DataStoria](https://github.com/FrankChen021/datastoria) es una aplicación de consola web con IA que permite gestionar varios clústeres de ClickHouse desde un único lugar.

Características:

* **Inteligencia basada en IA**: Usa lenguaje natural para explorar datos, optimizar y corregir consultas SQL, y visualizar tus datos.
* **Integración oficial con ClickHouse Agent Skills**: Aprovecha las [buenas prácticas oficiales](https://github.com/ClickHouse/agent-skills) para pedir a la IA optimizaciones y sugerencias para bases de datos.
* **Diagnóstico inteligente de errores**: Localiza errores de sintaxis al instante con un resaltado preciso de líneas y columnas, y obtén sugerencias de corrección basadas en IA con un solo clic.
* **Inspección de tablas del sistema**: Profundiza en `system.query_log`, `system.query_views_log`, `system.zookeeper`, `system.ddl_distributed_queue`, `system.part_log` y `system.processes` con un potente panel de visualización y filtros para comprender rápidamente tu clúster.
* **Explain en un clic**: Comprende al instante los planes de ejecución de consultas con vistas visuales del AST y del pipeline.
* **Grafo de dependencias**: Visualiza las relaciones entre tablas y rastrea los flujos de datos a través de vistas materializadas, tablas distribuidas y sistemas externos.
* **Monitorización de clúster**: Supervisa todos los nodos con métricas en tiempo real, operaciones de merge, estado de la replicación, rendimiento de las consultas y mucho más.
* **Privacidad y seguridad**: Todas las consultas SQL se ejecutan directamente desde tu navegador contra tu servidor ClickHouse, lo que garantiza una privacidad total.

[Documentación de DataStoria](https://docs.datastoria.app).

<div id="datapup">
  ### DataPup
</div>

[DataPup](https://github.com/DataPupOrg/DataPup) es un Client de base de datos moderno, multiplataforma y asistido por IA, con compatibilidad nativa con ClickHouse.

Características:

* Asistencia con consultas SQL impulsada por IA, con sugerencias inteligentes
* Compatibilidad con conexiones nativas de ClickHouse y gestión segura de credenciales
* Interfaz atractiva y accesible con varios temas (claro, oscuro y variantes coloridas)
* Filtrado y exploración avanzados de resultados de consulta
* Compatibilidad multiplataforma (macOS, Windows, Linux)
* Rendimiento rápido y ágil
* De código abierto y con licencia MIT

<div id="dory">
  ### Dory
</div>

[Dory](https://github.com/dorylab/dory), espacio de trabajo SQL nativo de IA con soporte de primer nivel para ClickHouse e IA integrada.

Características:

* Copiloto de IA para generación, explicación y depuración de SQL
* Gestión y consulta de múltiples clústeres de ClickHouse desde un espacio de trabajo unificado
* Autocompletado SQL con reconocimiento de esquemas y espacio de trabajo de consultas con múltiples pestañas
* Exploración interactiva de resultados de consultas con filtrado y visualización
* Resúmenes de tablas impulsados por IA para comprender conjuntos de datos
* Conexiones directas a ClickHouse con soporte para túneles SSH
* Interfaz moderna y pensada para desarrolladores, con soporte para modo claro, oscuro y temas
* Aplicación de escritorio multiplataforma (macOS, Windows, Linux) y soporte para Docker
* De código abierto y con licencia MIT

<div id="clickhouse-schemaflow-visualizer">
  ### Visualizador de flujo del esquema de ClickHouse
</div>

[ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer) es una aplicación web de código abierto para visualizar las relaciones entre tablas de ClickHouse.
Se conecta a una instancia de ClickHouse, analiza los metadatos de `system.tables` (tipos de motor, dependencias y `SELECT` de vistas materializadas) y genera diagramas interactivos de flujo de datos a nivel de tabla junto con relaciones a nivel de columna, con la expresión de transformación etiquetada en cada arista. Los diagramas se organizan con Dagre y se renderizan como SVG inline sencillos; no se carga ningún runtime de diagramación en el cliente.

Características:

* Explora bases de datos y tablas de ClickHouse con una barra lateral intuitiva
* Vista Data Flow: fuentes upstream a nivel de tabla y vistas materializadas downstream
* Vista Relationships: relaciones a nivel de columna con la expresión de transformación analizada en cada arista (por ejemplo, `toStartOfHour(scheduled_departure)`, `avgState(delay_minutes)`)
* Iconos según el motor y codificación por colores para `MergeTree`, `Replicated*`, `Distributed`, `MaterializedView` y `Dictionary`
* Haz clic en una columna en la vista Relationships para resaltar toda su ruta de datos a través del pipeline
* Filtro dinámico en la barra lateral y una paleta de comandos `Ctrl+K` / `⌘K` para saltar a cualquier tabla, columna o motor
* Superposición opcional de metadatos que muestra el recuento de filas y el tamaño en disco por tabla
* Exporta el diagrama actual como un archivo HTML autocontenido
* Conexión TLS a ClickHouse, con omisión opcional de verificación y certificados personalizados de CA y de cliente

[Código fuente de ClickHouse Schema Flow Visualizer](https://github.com/FulgerX2007/clickhouse-schemaflow-visualizer)

<div id="tabix">
  ### Tabix
</div>

Interfaz web para ClickHouse del proyecto [Tabix](https://github.com/tabixio/tabix).

Características:

* Funciona con ClickHouse directamente desde el navegador, sin necesidad de instalar software adicional.
* Editor de consultas con resaltado de sintaxis.
* Autocompletado de comandos.
* Herramientas para el análisis gráfico de la ejecución de consultas.
* Opciones de esquemas de color.

[Documentación de Tabix](https://tabix.io/doc/).

<div id="houseops">
  ### HouseOps
</div>

[HouseOps](https://github.com/HouseOps/HouseOps) es una UI/IDE para OSX, Linux y Windows.

Características:

* Constructor de consultas con resaltado de sintaxis. Permite ver la respuesta en una tabla o en formato JSON.
* Exportación de resultados de consultas en CSV o JSON.
* Lista de procesos con descripciones. Modo de edición. Posibilidad de detener un proceso (`KILL`).
* Grafo de la base de datos. Muestra todas las tablas y sus columnas con información adicional.
* Vista rápida del tamaño de las columnas.
* Configuración del servidor.

Las siguientes características están planificadas:

* Gestión de bases de datos.
* Gestión de usuarios.
* Análisis de datos en tiempo real.
* Monitorización de clúster.
* Gestión del clúster.
* Monitorización de tablas replicadas y tablas de Kafka.

<div id="lighthouse">
  ### LightHouse
</div>

[LightHouse](https://github.com/VKCOM/lighthouse) es una interfaz web ligera para ClickHouse.

Características:

* Lista de tablas con filtrado y metadatos.
* Vista previa de tablas con filtrado y ordenación.
* Ejecución de consultas en modo de solo lectura.

<div id="redash">
  ### Redash
</div>

[Redash](https://github.com/getredash/redash) es una plataforma de visualización de datos.

Admite varias fuentes de datos, entre ellas ClickHouse, y puede combinar los resultados de consultas de distintas fuentes de datos en un único conjunto de datos final.

Características:

* Potente editor de consultas.
* Explorador de bases de datos.
* Herramienta de visualización que permite representar los datos de diferentes formas.

<div id="grafana">
  ### Grafana
</div>

[Grafana](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) es una plataforma de monitorización y visualización.

&quot;Grafana le permite consultar, visualizar, crear alertas y comprender sus métricas, sin importar dónde estén almacenadas. Cree, explore y comparta dashboards con su equipo, y fomente una cultura basada en los datos. Cuenta con la confianza y el respaldo de la comunidad&quot; — grafana.com.

El plugin del origen de datos de ClickHouse ofrece compatibilidad con ClickHouse como base de datos subyacente.

<div id="qryn">
  ### qryn
</div>

[qryn](https://metrico.in) es una pila de observabilidad políglota y de alto rendimiento para ClickHouse *(anteriormente cLoki)*, con integraciones nativas de Grafana que permiten a los usuarios ingestar y analizar logs, métricas y trazas de telemetría desde cualquier agente compatible con Loki/LogQL, Prometheus/PromQL, OTLP/Tempo, Elastic, InfluxDB y muchos más.

Características:

* UI de Explore integrada y CLI de LogQL para consultar, extraer y visualizar datos
* Compatibilidad con las API nativas de Grafana para consultar, procesar, ingestar, rastrear y generar alertas sin plugins
* Pipeline potente para buscar, filtrar y extraer datos dinámicamente de logs, eventos, trazas y más
* API de ingestión y PUSH compatibles de forma transparente con LogQL, PromQL, InfluxDB, Elastic y muchos más
* Listo para usar con agentes como Promtail, Grafana-Agent, Vector, Logstash, Telegraf y muchos otros

<div id="dbeaver">
  ### DBeaver
</div>

[DBeaver](https://dbeaver.io/) - cliente de bases de datos de escritorio universal con soporte para ClickHouse.

Características:

* Desarrollo de consultas con resaltado de sintaxis y autocompletado.
* Lista de tablas con filtros y búsqueda de metadatos.
* Vista previa de los datos de la tabla.
* Búsqueda de texto completo.

De forma predeterminada, DBeaver no se conecta mediante una sesión (la CLI, por ejemplo, sí lo hace). Si necesita compatibilidad con sesiones (por ejemplo, para configurar ajustes para su sesión), edite las propiedades de conexión del driver y establezca `session_id` en una cadena aleatoria (utiliza una conexión HTTP internamente). Después, podrá usar cualquier ajuste desde la ventana de consulta.

<div id="clickhouse-cli">
  ### clickhouse-cli
</div>

[clickhouse-cli](https://github.com/hatarist/clickhouse-cli) es un Client de línea de comandos alternativo para ClickHouse, escrito en Python 3.

Características:

* Autocompletado.
* Resaltado de sintaxis para las consultas y la salida de datos.
* Compatibilidad con paginador para la salida de datos.
* Comandos personalizados al estilo de PostgreSQL.

<div id="clickhouse-flamegraph">
  ### clickhouse-flamegraph
</div>

[clickhouse-flamegraph](https://github.com/Slach/clickhouse-flamegraph) es una herramienta especializada para visualizar `system.trace_log` en forma de [flamegraph](http://www.brendangregg.com/flamegraphs.html).

<div id="clickhouse-plantuml">
  ### clickhouse-plantuml
</div>

[cickhouse-plantuml](https://pypi.org/project/clickhouse-plantuml/) es un script para generar diagramas de [PlantUML](https://plantuml.com/) de esquemas de tablas.

<div id="clickhouse-table-graph">
  ### ClickHouse table graph
</div>

[ClickHouse table graph](https://github.com/mbaksheev/clickhouse-table-graph) es una sencilla herramienta CLI para visualizar las dependencias entre tablas de ClickHouse. Esta herramienta obtiene las conexiones entre tablas a partir de la tabla `system.tables` y genera un diagrama de flujo de dependencias en formato [mermaid](https://mermaid.js.org/syntax/flowchart.html).  Con esta herramienta, puedes visualizar fácilmente las dependencias entre tablas y comprender el flujo de datos en tu base de datos de ClickHouse. Gracias a mermaid, el diagrama de flujo resultante tiene un aspecto atractivo y puede añadirse fácilmente a tu documentación en Markdown.

<div id="xeus-clickhouse">
  ### xeus-clickhouse
</div>

[xeus-clickhouse](https://github.com/wangfenjin/xeus-clickhouse) es un kernel de Jupyter para ClickHouse que permite consultar datos de ClickHouse usando SQL en Jupyter.

<div id="mindsdb">
  ### MindsDB Studio
</div>

[MindsDB](https://mindsdb.com/) es una capa de IA de código abierto para bases de datos, incluido ClickHouse, que le permite desarrollar, entrenar y desplegar fácilmente modelos de aprendizaje automático de última generación. MindsDB Studio(GUI) le permite entrenar nuevos modelos a partir de datos de la base de datos, interpretar las predicciones del modelo, identificar posibles sesgos en los datos, y evaluar y visualizar la precisión del modelo mediante la función de IA explicable para adaptar y ajustar sus modelos de aprendizaje automático con mayor rapidez.

<div id="dbm">
  ### DBM
</div>

[DBM](https://github.com/devlive-community/dbm) ¡DBM es una herramienta visual de gestión para ClickHouse!

Características:

* Permite consultar el historial de consultas (paginación, borrar todo, etc.)
* Permite realizar consultas con cláusulas SQL seleccionadas
* Permite finalizar consultas
* Permite gestionar tablas (metadatos, eliminación, vista previa)
* Permite gestionar bases de datos (eliminación, creación)
* Permite consultas personalizadas
* Permite gestionar múltiples fuentes de datos (prueba de conexión, monitoreo)
* Permite el monitoreo (procesador, conexión, consulta)
* Permite migrar datos

<div id="bytebase">
  ### Bytebase
</div>

[Bytebase](https://bytebase.com) es una herramienta web de código abierto para la gestión de cambios de esquema y el control de versiones para equipos. Es compatible con varias bases de datos, incluido ClickHouse.

Características:

* Revisión de esquemas entre desarrolladores y administradores de bases de datos.
* Database-as-Code: controla las versiones del esquema en sistemas VCS como GitLab y activa la implementación al hacer commit del código.
* Implementación optimizada con políticas por entorno.
* Historial completo de migraciones.
* Detección de schema drift.
* Copia de seguridad y restauración.
* RBAC.

<div id="zeppelin-interpreter-for-clickhouse">
  ### Zeppelin-Interpreter-for-ClickHouse
</div>

[Zeppelin-Interpreter-for-ClickHouse](https://github.com/SiderZhang/Zeppelin-Interpreter-for-ClickHouse) es un intérprete de [Zeppelin](https://zeppelin.apache.org) para ClickHouse. En comparación con el intérprete JDBC, puede ofrecer un mejor control del tiempo de espera para las consultas de larga duración.

<div id="clickcat">
  ### ClickCat
</div>

[ClickCat](https://github.com/clickcat-project/ClickCat) es una interfaz de usuario intuitiva que te permite buscar, explorar y visualizar tus datos de ClickHouse.

Características:

* Un editor SQL en línea que puede ejecutar tu código SQL sin necesidad de instalar nada.
* Puedes observar todos los procesos y mutaciones. En el caso de los procesos que no han finalizado, puedes terminarlos desde la UI.
* Las métricas incluyen análisis de clúster, análisis de datos y análisis de consultas.

<div id="clickvisual">
  ### ClickVisual
</div>

[ClickVisual](https://clickvisual.net/) ClickVisual es una plataforma ligera de código abierto para consultar, analizar y visualizar registros y alertas.

Características:

* Admite la creación con un solo clic de bibliotecas de análisis de registros
* Admite la gestión de la configuración de recopilación de registros
* Admite la configuración de índices definida por el usuario
* Admite la configuración de alertas
* Admite la configuración granular de permisos a nivel de biblioteca y de tabla

<div id="clickmate">
  ### ClickHouse-Mate
</div>

[ClickHouse-Mate](https://github.com/metrico/clickhouse-mate) es un Client web en Angular con interfaz de usuario para buscar y explorar datos en ClickHouse.

Características:

* Autocompletado de consultas de ClickHouse SQL
* Navegación rápida por el árbol de bases de datos y tablas
* Filtrado y ordenación avanzados de resultados
* Documentación integrada de ClickHouse SQL
* Preajustes e historial de consultas
* 100 % basado en navegador, sin servidor ni backend

El Client está disponible para su uso inmediato a través de GitHub Pages: https://metrico.github.io/clickhouse-mate/

<div id="uptrace">
  ### Uptrace
</div>

[Uptrace](https://github.com/uptrace/uptrace) es una herramienta de APM que proporciona tracing distribuido y métricas con OpenTelemetry y ClickHouse.

Características:

* [Tracing de OpenTelemetry](https://uptrace.dev/opentelemetry/distributed-tracing.html), métricas y logs.
* Notificaciones por correo electrónico/Slack/PagerDuty mediante AlertManager.
* Lenguaje de consulta similar a SQL para agregar spans.
* Lenguaje similar a PromQL para consultar métricas.
* Dashboards de métricas predefinidos.
* Varios usuarios/proyectos mediante configuración YAML.

<div id="clickhouse-monitoring">
  ### clickhouse-monitoring
</div>

[clickhouse-monitoring](https://github.com/duyet/clickhouse-monitoring) es un dashboard sencillo de Next.js que utiliza las tablas `system.*` para ayudar a supervisar y ofrecer una visión general de tu clúster de ClickHouse.

Características:

* Monitor de consultas: consultas actuales, historial de consultas, recursos de las consultas (memoria, partes leídas, file&#95;open, ...), consultas más costosas, tablas o columnas más usadas, etc.
* Monitor del clúster: uso total de memoria/CPU, cola distribuida, configuraciones globales, configuraciones de MergeTree, métricas, etc.
* Información sobre tablas y partes: tamaño, recuento de filas, compresión, tamaño de las partes, etc., con detalle a nivel de columna.
* Herramientas útiles: exploración de datos de ZooKeeper, EXPLAIN de consultas, cancelar consultas, etc.
* Gráficos de métricas: consultas y uso de recursos, número de merges/mutaciones, rendimiento de los merges, rendimiento de las consultas, etc.

<div id="ckibana">
  ### CKibana
</div>

[CKibana](https://github.com/TongchengOpenSource/ckibana) es un servicio ligero que te permite buscar, explorar y visualizar fácilmente datos de ClickHouse mediante la UI nativa de Kibana.

Características:

* Traduce las solicitudes de gráficos de la UI nativa de Kibana a la sintaxis de consulta de ClickHouse.
* Admite funciones avanzadas, como el muestreo y el almacenamiento en caché, para mejorar el rendimiento de las consultas.
* Reduce al mínimo la curva de aprendizaje para los usuarios tras migrar de ElasticSearch a ClickHouse.

<div id="telescope">
  ### Telescope
</div>

[Telescope](https://iamtelescope.net/) es una interfaz web moderna para explorar logs almacenados en ClickHouse. Proporciona una UI intuitiva para consultar, visualizar y gestionar datos de logs con control de acceso detallado.

Características:

* UI limpia y adaptable, con filtros potentes y selección de campos personalizable.
* Sintaxis FlyQL para un filtrado de logs intuitivo y expresivo.
* Gráfico temporal con soporte para group-by, incluidos campos JSON anidados, Map y Array.
* Soporte opcional para consultas `WHERE` en SQL sin procesar para filtrado avanzado (con comprobaciones de permisos).
* Vistas guardadas: permiten conservar y compartir configuraciones personalizadas de la UI para consultas y diseño.
* Control de acceso basado en roles (RBAC) e integración con la autenticación de GitHub.
* No se requieren agentes ni componentes adicionales del lado de ClickHouse.

[Código fuente de Telescope](https://github.com/iamtelescope/telescope) · [Demo en vivo](https://demo.iamtelescope.net)

<div id="clicklens">
  ### ClickLens
</div>

[ClickLens](https://ntk148v.github.io/clicklens/) es una interfaz web moderna, potente y fácil de usar para gestionar y supervisar bases de datos de ClickHouse. Ofrece un completo conjunto de herramientas para que desarrolladores, analistas y administradores interactúen con sus clústeres de ClickHouse de forma eficiente. ClickHouse es una increíble base de datos analítica, pero gestionarla desde la CLI o con herramientas básicas puede resultar complicado. ClickLens cubre esa carencia al ofrecer:

* Discover - Exploración de datos flexible, al estilo de Kibana, para cualquier tabla
* SQL Console - Escribe, ejecuta y analiza consultas con resaltado de sintaxis y resultados en streaming
* Monitorización en tiempo real - Vigila el estado de tu clúster, el rendimiento de las consultas y el uso de recursos
* Schema Explorer - Recorre bases de datos, tablas, columnas, partes y mucho más
* Control de acceso - Administra usuarios y roles directamente desde la UI
* RBAC nativo - Los permisos de la UI se derivan directamente de tus grants de ClickHouse

[Código fuente de ClickLens](https://github.com/ntk148v/clicklens)

<div id="chouse-ui">
  ### CHouse UI
</div>

[CHouse UI](https://chouse-ui.com) es una interfaz web de ClickHouse de código abierto y self-hosted, creada para **equipos que ejecutan ClickHouse en producción**. La mayoría de las herramientas resuelven bien una sola necesidad — un espacio de trabajo para consultas, un dashboard, un asistente de IA, un monitor de clústeres; CHouse UI es la *combinación*: una capa de acceso para equipos junto con monitorización de flotas multiclúster y un SRE de IA autónomo de solo lectura. A diferencia de los clientes que requieren credenciales directas de la base de datos, las almacena cifradas del lado del servidor y controla el acceso con su propia capa de **Control de acceso basado en roles (RBAC)**, por lo que el navegador nunca ve una contraseña de ClickHouse.

Características:

* **Acceso de equipos y seguridad** - RBAC a nivel de aplicación (roles predefinidos + roles personalizados, reglas granulares de acceso a datos por base de datos/tabla), registro de auditoría con contexto real de sesión y credenciales cifradas del lado del servidor con AES-256-GCM.
* **Flota multiclúster** - Supervise cada clúster configurado en un único panel (estado, memoria, consultas activas, excepciones, minigráficos de tendencias), con sondeo independiente de cada tarjeta y respaldo de un sondeador de instantáneas en el backend.
* **Chouse AI — Fleet Doctor** - Un SRE de IA autónomo y de solo lectura: analiza la flota con una herramienta `SELECT` protegida y limitada a `system.*` (ClickHouse `readonly=1`), detecta las causas raíz y redacta un informe estructurado con un análisis en profundidad de las consultas costosas y reescrituras sugeridas. Nunca modifica el clúster.
* **IA en las pestañas de monitorización** - &quot;Optimize with Chouse AI&quot; en una fila de Query Logs (reescritura + estimación `EXPLAIN` antes→después + abrir en el espacio de trabajo SQL), además de &quot;Diagnose&quot; con un clic en una fila de `system.errors` o en una entrada del registro de partes.
* **Alertas de umbral** - Reglas sobre % de memoria del nodo, memoria por consulta y consultas de larga duración, enviadas a Slack y correo electrónico — con un análisis autónomo de causa raíz adjunto cuando se supera el umbral.
* **Espacio de trabajo completo** - Editor SQL Monaco, explorador de esquemas, vista de consultas en vivo con capacidad para finalizar consultas, monitorización nativa de ClickHouse (desglose de memoria, partes/fusiones, retraso de réplica, percentiles de latencia) e importación/exportación de datos.

Código abierto (Apache 2.0), con prioridad para entornos on-premises — todas las funciones se incluyen de serie, sin nivel de pago.

[CHouse UI código fuente](https://github.com/daun-gatal/chouse-ui)

<div id="clickhouse-flow">
  ### clickhouse-flow
</div>

[clickhouse-flow](https://github.com/MikeAmputer/clickhouse-flow) es una herramienta de código abierto para visualizar flujos de datos y dependencias entre tablas, vistas y vistas materializadas de ClickHouse.

Características:

* Genera automáticamente un grafo del esquema a partir de los metadatos de ClickHouse.
* Visualiza los flujos de datos a través de vistas materializadas.
* UI interactiva para explorar la estructura del esquema.
* Exporta diagramas en PDF o SVG para documentación y para compartirlos.
* Despliegue basado en Docker para una puesta en marcha rápida en entornos de desarrollo.

<div id="commercial">
  ## Comerciales
</div>

<div id="datagrip">
  ### DataGrip
</div>

[DataGrip](https://www.jetbrains.com/datagrip/) es un IDE de bases de datos de JetBrains con soporte específico para ClickHouse. También está integrado en otras herramientas basadas en IntelliJ: PyCharm, IntelliJ IDEA, GoLand, PhpStorm y otras.

Características:

* Autocompletado de código muy rápido.
* Resaltado de sintaxis de ClickHouse.
* Soporte para funcionalidades específicas de ClickHouse, por ejemplo, columnas anidadas y motores de tabla.
* Editor de datos.
* Refactorizaciones.
* Búsqueda y navegación.

<div id="yandex-datalens">
  ### Yandex DataLens
</div>

[Yandex DataLens](https://yandex.cloud/en/services/datalens) es un servicio de visualización de datos y analítica.

Características:

* Amplia variedad de visualizaciones disponibles, desde gráficos de barras sencillos hasta dashboards complejos.
* Los dashboards pueden hacerse públicos.
* Compatibilidad con múltiples fuentes de datos, incluido ClickHouse.
* Almacenamiento de datos materializados basado en ClickHouse.

DataLens está [disponible gratuitamente](https://yandex.cloud/en/docs/datalens/pricing) para proyectos de baja carga, incluso para uso comercial.

* [Documentación de DataLens](https://yandex.cloud/en/docs/datalens/).
* [Tutorial](https://yandex.cloud/en/docs/solutions/datalens/data-from-ch-visualization) sobre cómo visualizar datos de una base de datos de ClickHouse.

<div id="holistics-software">
  ### Holistics Software
</div>

[Holistics](https://www.holistics.io/) es una plataforma de datos full-stack y una herramienta de inteligencia empresarial.

Características:

* Programación automatizada de informes por correo electrónico, Slack y Google Sheets.
* Editor SQL con visualizaciones, control de versiones, autocompletado, componentes de consulta reutilizables y filtros dinámicos.
* Analítica embebida de informes y dashboards mediante iframe.
* Capacidades de preparación de datos y ETL.
* Soporte para el modelado de datos en SQL para mapear datos relacionales.

<div id="looker">
  ### Looker
</div>

[Looker](https://looker.com) es una plataforma de datos y una herramienta de business intelligence compatible con más de 50 dialectos de bases de datos, incluido ClickHouse. Looker está disponible como plataforma SaaS y autogestionada. Los usuarios pueden usar Looker desde el navegador para explorar datos, crear visualizaciones y dashboards, programar informes y compartir sus hallazgos con sus colegas. Looker proporciona un amplio conjunto de herramientas para incorporar estas funcionalidades en otras aplicaciones, y una API
para integrar datos con otras aplicaciones.

Características:

* Desarrollo fácil y ágil con LookML, un lenguaje compatible con [Data Modeling](https://looker.com/platform/data-modeling) seleccionado
  para ayudar a quienes crean informes y a los usuarios finales.
* Potente integración de workflows mediante [Data Actions](https://looker.com/platform/actions) de Looker.

[Cómo configurar ClickHouse en Looker.](https://docs.looker.com/setup-and-management/database-config/clickhouse)

<div id="seektable">
  ### SeekTable
</div>

[SeekTable](https://www.seektable.com) es una herramienta de BI de autoservicio para la exploración de datos y la generación de informes operativos. Está disponible tanto como servicio en la nube como en una versión autohospedada. Los informes de SeekTable pueden integrarse en cualquier aplicación web.

Características:

* Constructor de informes fácil de usar para usuarios de negocio.
* Potentes parámetros de informe para el filtrado SQL y la personalización de consultas específicas de cada informe.
* Puede conectarse a ClickHouse tanto mediante un endpoint nativo TCP/IP como a través de una interfaz HTTP(S) (2 drivers diferentes).
* Es posible aprovechar toda la potencia del dialecto ClickHouse SQL en las definiciones de dimensiones/medidas.
* [Web API](https://www.seektable.com/help/web-api-integration) para la generación automatizada de informes.
* Admite flujos de desarrollo de informes con [backup/restore](https://www.seektable.com/help/self-hosted-backup-restore) de los datos de la cuenta; la configuración de los modelos de datos (cubos) y los informes está en XML legible para humanos y puede almacenarse en un sistema de control de versiones.

SeekTable es [gratuito](https://www.seektable.com/help/cloud-pricing) para uso personal/individual.

[Cómo configurar la conexión de ClickHouse en SeekTable.](https://www.seektable.com/help/clickhouse-pivot-table)

<div id="chadmin">
  ### Chadmin
</div>

[Chadmin](https://github.com/bun4uk/chadmin) es una UI sencilla donde puedes visualizar las consultas que se están ejecutando actualmente en tu clúster de ClickHouse, ver información sobre ellas y finalizarlas si quieres.

<div id="tablum_io">
  ### TABLUM.IO
</div>

[TABLUM.IO](https://tablum.io/) — una herramienta en línea de consulta y analítica para ETL y visualización. Permite conectarse a ClickHouse, consultar datos mediante una versátil consola SQL, así como cargar datos desde archivos estáticos y servicios de terceros. TABLUM.IO puede visualizar los resultados en forma de gráficos y tablas.

Características:

* ETL: carga de datos desde bases de datos populares, archivos locales y remotos e invocaciones de API.
* Versátil consola SQL con resaltado de sintaxis y constructor visual de consultas.
* Visualización de datos mediante gráficos y tablas.
* Materialización de datos y subconsultas.
* Envío de informes de datos a Slack, Telegram o correo electrónico.
* Canalización de datos mediante una API propietaria.
* Exportación de datos en formatos JSON, CSV, SQL y HTML.
* Interfaz web.

TABLUM.IO puede ejecutarse como una solución autogestionada (como una imagen de Docker) o en la nube.
Licencia: producto [comercial](https://tablum.io/pricing) con un período de prueba gratuito de 3 meses.

Pruébelo gratis [en la nube](https://tablum.io/try).
Obtenga más información sobre el producto en [TABLUM.IO](https://tablum.io/)

<div id="ckman">
  ### CKMAN
</div>

[CKMAN](https://www.github.com/housepower/ckman) es una herramienta para gestionar y supervisar clústeres de ClickHouse.

Características:

* Implementación automatizada rápida y sencilla de clústeres mediante una interfaz web
* Los clústeres pueden escalarse o reducirse
* Balanceo de carga de los datos del clúster
* Actualización en línea del clúster
* Modificación de la configuración del clúster desde la página
* Proporciona monitorización de los nodos del clúster y de ZooKeeper
* Monitorización del estado de las tablas y las particiones, así como de las sentencias SQL lentas
* Proporciona una página fácil de usar para ejecutar SQL

<div id="1bench">
  ### 1bench
</div>

[1bench](https://1bench.dev) es una interfaz gráfica de escritorio nativa para múltiples bases de datos con soporte de primer nivel para ClickHouse, que abarca la vista general del servidor, la gestión de esquemas, la búsqueda vectorial y la exploración de grandes conjuntos de resultados.

Características:

* Vista general del servidor al conectarse: versión, uptime, consultas en ejecución, merges activos, partes y tamaños de almacenamiento, estado de la réplica, clústeres y nodos de un vistazo.
* Constructor visual de consultas (selectores de columnas, filtros, ordenación, límite) junto con un editor SQL Monaco con syntax highlighting e historial de consultas por conexión.
* Asistente visual de `CREATE TABLE` con soporte para variantes de `MergeTree`, `ORDER BY`, `PARTITION BY`, `SETTINGS` y encapsulación automática en `Nullable()`.
* Manejo nativo de tipos de ClickHouse: `Nullable`, `Array`, `LowCardinality` y objetos anidados.
* Soporte para búsqueda vectorial: columnas de embedding `Array(Float32)` representadas como celdas vectoriales compactas, visualización de embeddings en 2D y opción de buscar similares mediante `cosineDistance`.
* Edición de datos Inline en tablas de resultados con guardado por lotes, además de exportación e importación en CSV/JSON/SQL mediante los formatos nativos de ClickHouse.
* Opciones de conexión: HTTP/HTTPS, túnel SSH para clústeres privados detrás de un firewall y modo opcional de solo lectura para explorar de forma segura entornos de production.
* Funciona con ClickHouse Cloud y entornos autogestionados.