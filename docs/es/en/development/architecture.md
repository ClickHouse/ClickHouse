---
description: 'Una visión general completa de la arquitectura de ClickHouse y su diseño
  orientado a columna'
sidebar_label: 'Visión general de la arquitectura'
sidebar_position: 50
slug: /development/architecture
title: 'Visión general de la arquitectura'
doc_type: 'reference'
---

ClickHouse es un DBMS verdaderamente orientado a columna. Los datos se almacenan por columnas y, durante la ejecución, se procesan en arrays (vectores o fragmentos de columnas).
Siempre que es posible, las operaciones se ejecutan sobre arrays en lugar de sobre valores individuales.
A esto se le llama &quot;ejecución vectorizada de consultas&quot; y ayuda a reducir el coste real del procesamiento de datos.

Esta idea no es nueva.
Se remonta a `APL` (un lenguaje de programación, 1957) y sus descendientes: `A +` (dialecto de APL), `J` (1990), `K` (1993) y `Q` (lenguaje de programación de Kx Systems, 2003).
La programación con arrays se utiliza en el procesamiento de datos científicos. Tampoco es una idea nueva en las bases de datos relacionales. Por ejemplo, se utiliza en el sistema `VectorWise` (también conocido como Actian Vector Analytic Database de Actian Corporation).

Existen dos enfoques distintos para acelerar el procesamiento de consultas: la ejecución vectorizada de consultas y la generación de código en tiempo de ejecución. Este último elimina toda indirección y el despacho dinámico. Ninguno de estos enfoques es estrictamente mejor que el otro. La generación de código en tiempo de ejecución puede ser mejor cuando fusiona muchas operaciones, aprovechando así al máximo las unidades de ejecución de la CPU y el pipeline. La ejecución vectorizada de consultas puede ser menos práctica porque implica vectores temporales que deben escribirse en la caché y volver a leerse. Si los datos temporales no caben en la caché L2, esto se convierte en un problema. Pero la ejecución vectorizada de consultas aprovecha más fácilmente las capacidades SIMD de la CPU. Un [artículo de investigación](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) escrito por nuestros amigos muestra que lo mejor es combinar ambos enfoques. ClickHouse utiliza ejecución vectorizada de consultas y tiene compatibilidad inicial limitada con la generación de código en tiempo de ejecución.

<div id="columns">
  ## Columnas
</div>

La interfaz `IColumn` se utiliza para representar columnas en memoria (en realidad, fragmentos de columnas). Esta interfaz proporciona métodos auxiliares para implementar varios operadores relacionales. Casi todas las operaciones son inmutables: no modifican la columna original, sino que crean una nueva columna modificada. Por ejemplo, el método `IColumn :: filter` acepta una máscara de bytes de filtrado. Se utiliza para los operadores relacionales `WHERE` y `HAVING`. Otros ejemplos: el método `IColumn :: permute` para dar soporte a `ORDER BY` y el método `IColumn :: cut` para dar soporte a `LIMIT`.

Varias implementaciones de `IColumn` (`ColumnUInt8`, `ColumnString`, etc.) se encargan de la organización en memoria de las columnas. Esta organización suele ser un array contiguo. En el caso de las columnas de tipo entero, es simplemente un array contiguo, como `std :: vector`. En el caso de las columnas `String` y `Array`, hay dos vectores: uno para todos los elementos del array, almacenados de forma contigua, y otro para los desplazamientos hasta el inicio de cada array. También existe `ColumnConst`, que almacena un único valor en memoria, pero tiene el aspecto de una columna.

<div id="field">
  ## Field
</div>

No obstante, también es posible trabajar con valores individuales. Para representar un valor individual, se usa `Field`. `Field` es simplemente una unión discriminada de `UInt64`, `Int64`, `Float64`, `String` y `Array`. `IColumn` tiene el método `operator []` para obtener el enésimo valor como un `Field`, y el método `insert` para añadir un `Field` al final de una columna. Estos métodos no son muy eficientes, porque requieren manejar objetos temporales `Field` que representan un valor individual. Hay métodos más eficientes, como `insertFrom`, `insertRangeFrom`, etc.

`Field` no contiene suficiente información sobre el tipo de dato específico de una tabla. Por ejemplo, `UInt8`, `UInt16`, `UInt32` y `UInt64` se representan todos como `UInt64` en un `Field`.

<div id="leaky-abstractions">
  ## Abstracciones con fugas
</div>

`IColumn` tiene métodos para transformaciones relacionales comunes de datos, pero no cubren todas las necesidades. Por ejemplo, `ColumnUInt64` no tiene un método para calcular la suma de dos columnas, y `ColumnString` no tiene un método para realizar una búsqueda de subcadenas. Estas innumerables rutinas se implementan fuera de `IColumn`.

Varias funciones sobre columnas pueden implementarse de forma genérica y poco eficiente usando métodos de `IColumn` para extraer valores `Field`, o de forma especializada aprovechando el conocimiento de la distribución interna en memoria de los datos en una implementación concreta de `IColumn`. Esto se logra convirtiendo a un tipo específico de `IColumn` y accediendo directamente a la representación interna. Por ejemplo, `ColumnUInt64` tiene el método `getData`, que devuelve una referencia a un array interno; después, una rutina independiente lee o rellena ese array directamente. Tenemos &quot;abstracciones con fugas&quot; para permitir especializaciones eficientes de varias rutinas.

<div id="data_types">
  ## Tipos de datos
</div>

`IDataType` se encarga de la serialización y deserialización: leer y escribir fragmentos de columnas o valores individuales en formato binario o de texto. `IDataType` se corresponde directamente con los tipos de datos de las tablas. Por ejemplo, existen `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString`, etc.

`IDataType` e `IColumn` solo guardan una relación débil entre sí. Distintos tipos de datos pueden representarse en memoria mediante las mismas implementaciones de `IColumn`. Por ejemplo, tanto `DataTypeUInt32` como `DataTypeDateTime` se representan mediante `ColumnUInt32` o `ColumnConstUInt32`. Además, un mismo tipo de datos puede representarse mediante distintas implementaciones de `IColumn`. Por ejemplo, `DataTypeUInt8` puede representarse mediante `ColumnUInt8` o `ColumnConstUInt8`.

`IDataType` solo almacena metadatos. Por ejemplo, `DataTypeUInt8` no almacena nada en absoluto (salvo el puntero virtual `vptr`) y `DataTypeFixedString` solo almacena `N` (el tamaño de las cadenas de longitud fija).

`IDataType` tiene métodos auxiliares para varios formatos de datos. Algunos ejemplos son métodos para serializar un valor con posible entrecomillado, para serializar un valor para JSON y para serializar un valor como parte del formato XML. No existe una correspondencia directa con los formatos de datos. Por ejemplo, los distintos formatos de datos `Pretty` y `TabSeparated` pueden usar el mismo método auxiliar `serializeTextEscaped` de la interfaz `IDataType`.

<div id="block">
  ## Bloque
</div>

Un `Block` es un contenedor que representa un subconjunto (fragmento) de una tabla en memoria. No es más que un conjunto de triples: `(IColumn, IDataType, nombre de la columna)`. Durante la ejecución de una consulta, los datos se procesan en `Block`s. Si tenemos un `Block`, tenemos datos (en el objeto `IColumn`), tenemos información sobre su tipo (en `IDataType`), que nos indica cómo tratar esa columna, y tenemos el nombre de la columna. Puede ser el nombre original de la columna de la tabla o algún nombre artificial asignado para obtener resultados temporales de cálculos.

Cuando calculamos alguna función sobre columnas de un bloque, añadimos al bloque otra columna con su resultado, y no tocamos las columnas que actúan como argumentos de la función porque las operaciones son inmutables. Más adelante, las columnas innecesarias pueden eliminarse del bloque, pero no modificarse. Esto resulta conveniente para eliminar subexpresiones comunes.

Los bloques se crean para cada fragmento de datos procesado. Tenga en cuenta que, para el mismo tipo de cálculo, los nombres y tipos de las columnas se mantienen iguales en distintos bloques, y solo cambian los datos de las columnas. Es mejor separar los datos del bloque de la cabecera del bloque, porque los bloques pequeños tienen una sobrecarga elevada por las cadenas temporales necesarias para copiar `shared_ptr`s y nombres de columnas.

<div id="processors">
  ## Procesadores
</div>

Consulte la descripción en [https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/IProcessor.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/IProcessor.h).

<div id="formats">
  ## Formatos
</div>

Los formatos de datos se implementan mediante procesadores.

<div id="io">
  ## E/S
</div>

Para la entrada/salida orientada a bytes, existen las clases abstractas `ReadBuffer` y `WriteBuffer`. Se usan en lugar de los `iostream` de C++. No se preocupe: todo proyecto maduro de C++ usa algo distinto de `iostream`, y por buenas razones.

`ReadBuffer` y `WriteBuffer` no son más que un búfer contiguo y un cursor que apunta a una posición dentro de ese búfer. Las implementaciones pueden poseer o no la memoria del búfer. Hay un método virtual para llenar el búfer con los datos siguientes (en `ReadBuffer`) o para vaciar el búfer en algún destino (en `WriteBuffer`). Los métodos virtuales rara vez se llaman.

Las implementaciones de `ReadBuffer`/`WriteBuffer` se usan para trabajar con archivos, descriptores de archivo y sockets de red, para implementar compresión (`CompressedWriteBuffer` se inicializa con otro `WriteBuffer` y realiza la compresión antes de escribir los datos en él), y para otros fines; los nombres `ConcatReadBuffer`, `LimitReadBuffer` y `HashingWriteBuffer` se explican por sí solos.

`ReadBuffer` y `WriteBuffer` solo trabajan con bytes. Los archivos de encabezado `ReadHelpers` y `WriteHelpers` incluyen funciones auxiliares para dar formato a la entrada/salida. Por ejemplo, hay funciones auxiliares para escribir un número en formato decimal.

Veamos qué ocurre cuando quiere escribir un conjunto de resultados en formato `JSON` en stdout.
Tiene un conjunto de resultados listo para recuperarse desde un `QueryPipeline` en modo pulling.
Primero, crea un `WriteBufferFromFileDescriptor(STDOUT_FILENO)` para escribir bytes en stdout.
A continuación, conecta el resultado de la canalización de consulta a `JSONRowOutputFormat`, que se inicializa con ese `WriteBuffer`, para escribir filas en formato `JSON` en stdout.
Esto puede hacerse mediante el método `complete`, que convierte un `QueryPipeline` en modo pulling en un `QueryPipeline` completado.
Internamente, `JSONRowOutputFormat` escribirá varios delimitadores de JSON y llamará al método `IDataType::serializeTextJSON` con una referencia a `IColumn` y el número de fila como argumentos. En consecuencia, `IDataType::serializeTextJSON` llamará a un método de `WriteHelpers.h`: por ejemplo, `writeText` para tipos numéricos y `writeJSONString` para `DataTypeString`.

<div id="tables">
  ## Tablas
</div>

La interfaz `IStorage` representa tablas. Las distintas implementaciones de esa interfaz son distintos motores de tabla. Algunos ejemplos son `StorageMergeTree`, `StorageMemory`, etc. Las instancias de estas clases son simplemente tablas.

Los métodos clave de `IStorage` son `read` y `write`, junto con otros como `alter`, `rename` y `drop`. El método `read` acepta los siguientes argumentos: un conjunto de columnas para leer de una tabla, el `AST` de la consulta que se debe tener en cuenta y el número deseado de flujos. Devuelve un `Pipe`.

En la mayoría de los casos, el método `read` solo se encarga de leer las columnas especificadas de una tabla, no de ningún procesamiento posterior de los datos.
Todo el procesamiento posterior de los datos lo gestiona otra parte de la canalización, que queda fuera de la responsabilidad de `IStorage`.

Pero hay excepciones importantes:

* El `AST` de la consulta se pasa al método `read`, y el motor de tabla puede usarlo para determinar el uso de índices y leer menos datos de una tabla.
* A veces, el motor de tabla puede procesar los datos por sí mismo hasta una etapa específica. Por ejemplo, `StorageDistributed` puede enviar una consulta a servidores remotos, pedirles que procesen los datos hasta una etapa en la que los datos de distintos servidores remotos puedan fusionarse y devolver esos datos preprocesados. Después, el intérprete de consultas termina de procesar los datos.

El método `read` de la tabla puede devolver un `Pipe` compuesto por varios `Processors`. Estos `Processors` pueden leer de una tabla en paralelo.
Después, puedes conectar estos procesadores con otras transformaciones (como la evaluación de expresiones o el filtrado), que pueden calcularse de forma independiente.
Y luego crear un `QueryPipeline` sobre ellos y ejecutarlo mediante `PipelineExecutor`.

También existen las `TableFunction`. Son funciones que devuelven un objeto temporal `IStorage` para usarlo en la cláusula `FROM` de una consulta.

Para hacerte una idea rápida de cómo implementar tu motor de tabla, mira algo sencillo, como `StorageMemory` o `StorageTinyLog`.

> Como resultado del método `read`, `IStorage` devuelve `QueryProcessingStage`: información sobre qué partes de la consulta ya se han calculado dentro del almacenamiento.

<div id="parsers">
  ## Analizadores sintácticos
</div>

Un analizador sintáctico descendente recursivo escrito a mano analiza una consulta. Por ejemplo, `ParserSelectQuery` simplemente llama recursivamente a los analizadores subyacentes de las distintas partes de la consulta. Los analizadores crean un `AST`. El `AST` se representa mediante nodos, que son instancias de `IAST`.

> No se utilizan generadores de analizadores sintácticos por razones históricas.

<div id="interpreters">
  ## Intérpretes
</div>

Los intérpretes se encargan de crear el pipeline de ejecución de consultas a partir de un AST. Hay intérpretes simples, como `InterpreterExistsQuery` e `InterpreterDropQuery`, así como otros más sofisticados, como `InterpreterSelectQuery`.

El pipeline de ejecución de consultas es una combinación de procesadores que pueden consumir y producir fragmentos (conjuntos de columnas con tipos específicos).
Un procesador se comunica mediante puertos y puede tener varios puertos de entrada y varios puertos de salida.
Puede encontrarse una descripción más detallada en [src/Processors/IProcessor.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/IProcessor.h).

Por ejemplo, el resultado de interpretar la consulta `SELECT` es un `QueryPipeline` &quot;pulling&quot; que tiene un puerto de salida especial desde el que leer el conjunto de resultados.
El resultado de la consulta `INSERT` es un `QueryPipeline` &quot;pushing&quot; con un puerto de entrada para escribir los datos que se van a insertar.
Y el resultado de interpretar la consulta `INSERT SELECT` es un `QueryPipeline` &quot;completado&quot; que no tiene entradas ni salidas, pero copia datos de `SELECT` a `INSERT` simultáneamente.

`InterpreterSelectQuery` utiliza la infraestructura de `ExpressionAnalyzer` y `ExpressionActions` para el análisis y las transformaciones de consultas. Aquí es donde se realiza la mayor parte de las optimizaciones de consultas basadas en reglas. `ExpressionAnalyzer` es bastante desordenado y debería reescribirse: varias transformaciones y optimizaciones de consultas deberían extraerse a clases independientes para permitir transformaciones modulares de la consulta.

Para resolver los problemas existentes en los intérpretes, se ha desarrollado un nuevo `InterpreterSelectQueryAnalyzer`. Se trata de una nueva versión de `InterpreterSelectQuery` que no utiliza `ExpressionAnalyzer` e introduce una capa adicional de abstracción entre `AST` y `QueryPipeline`, llamada `QueryTree`. Está totalmente listo para su uso en production, pero, por si acaso, puede desactivarse estableciendo el valor de la configuración `enable_analyzer` en `false`.

<div id="functions">
  ## Funciones
</div>

Hay funciones ordinarias y funciones de agregación. Para las funciones de agregación, consulte la siguiente sección.

Las funciones ordinarias no cambian el número de filas: funcionan como si procesaran cada fila de forma independiente. En realidad, las funciones no se llaman para filas individuales, sino para `bloque` de datos, a fin de implementar la ejecución vectorizada de consultas.

Hay algunas funciones diversas, como [blockSize](/es/sql-reference/functions/other-functions#blockSize), [rowNumberInBlock](/es/sql-reference/functions/other-functions#rowNumberInBlock) y [runningAccumulate](/es/sql-reference/functions/other-functions#runningAccumulate), que aprovechan el procesamiento por bloques y rompen la independencia de las filas.

ClickHouse tiene tipado estricto, por lo que no hay conversión implícita de tipos. Si una función no admite una combinación específica de tipos, lanza una excepción. Pero las funciones pueden funcionar (estar sobrecargadas) con muchas combinaciones distintas de tipos. Por ejemplo, la función `plus` (para implementar el operador `+`) funciona con cualquier combinación de tipos numéricos: `UInt8` + `Float32`, `UInt16` + `Int8`, etcétera. Además, algunas funciones variádicas pueden aceptar cualquier número de argumentos, como la función `concat`.

Implementar una función puede resultar algo incómodo, porque una función resuelve explícitamente los tipos de datos compatibles y los `IColumns` compatibles. Por ejemplo, la función `plus` tiene código generado mediante la instanciación de una plantilla de C++ para cada combinación de tipos numéricos, y para argumentos izquierdo y derecho constantes o no constantes.

Este es un lugar excelente para implementar generación de código en tiempo de ejecución y evitar así la proliferación de código de plantillas. Además, permite añadir funciones fusionadas, como fused multiply-add, o realizar varias comparaciones en una sola iteración del bucle.

Debido a la ejecución vectorizada de consultas, las funciones no usan evaluación de cortocircuito. Por ejemplo, si escribe `WHERE f(x) AND g(y)`, ambos lados se calculan, incluso en las filas en las que `f(x)` es cero (excepto cuando `f(x)` es una expresión constante igual a cero). Pero si la selectividad de la condición `f(x)` es alta y el cálculo de `f(x)` es mucho más barato que el de `g(y)`, es mejor implementar un cálculo en varias pasadas. Primero se calcularía `f(x)`, luego se filtrarían las columnas según el resultado y, después, se calcularía `g(y)` solo para fragmentos de datos más pequeños y filtrados.

<div id="aggregate-functions">
  ## Funciones de agregación
</div>

Las funciones de agregación son funciones con estado. Acumulan los valores que se les pasan en un estado y permiten obtener resultados a partir de él. Se gestionan mediante la interfaz `IAggregateFunction`. Los estados pueden ser bastante simples (el estado de `AggregateFunctionCount` es simplemente un valor `UInt64`) o bastante complejos (el estado de `AggregateFunctionUniqCombined` es una combinación de un array lineal, una tabla hash y la estructura de datos probabilística `HyperLogLog`).

Los estados se asignan en `Arena` (un pool de memoria) para manejar múltiples estados mientras se ejecuta una consulta `GROUP BY` de alta cardinalidad. Los estados pueden tener un constructor y un destructor no triviales: por ejemplo, los estados de agregación complejos pueden asignar memoria adicional por sí mismos. Esto exige prestar cierta atención a la creación y destrucción de los estados, así como a transferir correctamente su propiedad y respetar el orden de destrucción.

Los estados de agregación pueden serializarse y deserializarse para transmitirse por la red durante la ejecución distribuida de consultas o para escribirse en disco cuando no hay suficiente RAM. Incluso pueden almacenarse en una tabla con `DataTypeAggregateFunction` para permitir la agregación incremental de datos.

> El formato de datos serializados de los estados de las funciones de agregación no está versionado actualmente. No hay problema si los estados de agregación solo se almacenan temporalmente. Pero contamos con el motor de tabla `AggregatingMergeTree` para la agregación incremental, y la gente ya lo está usando en producción. Por eso, en el futuro será necesario mantener la compatibilidad con versiones anteriores al cambiar el formato serializado de cualquier función de agregación.

<div id="server">
  ## Servidor
</div>

El servidor implementa varias interfaces diferentes:

* Una interfaz HTTP para cualquier cliente externo.
* Una interfaz TCP para el client nativo de ClickHouse y para la comunicación entre servidores durante la ejecución distribuida de consultas.
* Una interfaz para transferir datos para la replicación.

Internamente, no es más que un servidor multihilo primitivo, sin corrutinas ni fibras. Como el servidor no está diseñado para procesar una alta tasa de consultas simples, sino una tasa relativamente baja de consultas complejas, cada una de ellas puede procesar una enorme cantidad de datos para analítica.

El servidor inicializa la clase `Context` con el entorno necesario para la ejecución de consultas: la lista de bases de datos disponibles, usuarios y derechos de acceso, configuraciones, clústeres, la lista de procesos, el registro de consultas, etc. Intérpretes usan este entorno.

Mantenemos total compatibilidad retroactiva y prospectiva para el protocolo TCP del servidor: los client antiguos pueden comunicarse con servidores nuevos, y los client nuevos pueden comunicarse con servidores antiguos. Pero no queremos mantenerla eternamente, y eliminamos la compatibilidad con las versiones antiguas después de aproximadamente un año.

:::note
Para la mayoría de las aplicaciones externas, recomendamos usar la interfaz HTTP porque es simple y fácil de usar. El protocolo TCP está más estrechamente vinculado a las estructuras de datos internas: usa un formato interno para transmitir bloques de datos y un mecanismo de framing personalizado para los datos comprimidos.
:::

<div id="configuration">
  ## Configuración
</div>

El servidor de ClickHouse se basa en las bibliotecas POCO C++ y utiliza `Poco::Util::AbstractConfiguration` para representar su configuración. La configuración se almacena en la clase `Poco::Util::ServerApplication`, de la que hereda la clase `DaemonBase`, y esta, a su vez, es heredada por la clase `DB::Server`, que implementa `clickhouse-server`. Por tanto, se puede acceder a la configuración mediante el método `ServerApplication::config()`.

La configuración se lee desde varios archivos (en formato XML o YAML) y la clase `ConfigProcessor` la combina en un único `AbstractConfiguration`. La configuración se carga al iniciar el servidor y puede volver a cargarse más adelante si alguno de los archivos de configuración se actualiza, se elimina o se añade. La clase `ConfigReloader` también se encarga de supervisar periódicamente estos cambios y del procedimiento de recarga. La consulta `SYSTEM RELOAD CONFIG` también desencadena la recarga de la configuración.

En el caso de las consultas y de los subsistemas distintos de `Server`, se puede acceder a la configuración mediante el método `Context::getConfigRef()`. Todo subsistema capaz de recargar su configuración sin reiniciar el servidor debe registrarse en el callback de recarga en el método `Server::main()`. Tenga en cuenta que, si la configuración más reciente contiene algún error, la mayoría de los subsistemas ignorarán la nueva configuración, registrarán mensajes de advertencia y seguirán funcionando con la configuración cargada anteriormente. Debido a la naturaleza de `AbstractConfiguration`, no es posible pasar una referencia a una sección concreta, por lo que normalmente se utiliza `String config_prefix`.

<div id="context">
  ### Contexto
</div>

ClickHouse gestiona los ajustes mediante la jerarquía de contextos:

* **Contexto global** - ajustes de todo el servidor definidos mediante archivos de configuración
* **Contexto de sesión** - ajustes de la sesión del usuario procedentes de perfiles, la configuración del usuario y los comandos SET
* **Contexto de consulta** - ajustes a nivel de consulta procedentes de la cláusula SETTINGS
* **Contexto en segundo plano** - ajustes de todo el servidor para operaciones en segundo plano (Mutate, Merge) definidos mediante el perfil &#39;background&#39;

Al planificar una operación (consultas, mutaciones, etc.), el servidor construye el contexto específico combinando los ajustes en el siguiente orden (las secciones posteriores sobrescriben a las anteriores):

1. Valores globales predeterminados
2. Configuración global
3. Ajustes del perfil (de la sección `<profiles>`)
4. Ajustes del usuario (de la sección `<users>`)
5. Ajustes de sesión (del comando SET)
6. Ajustes de consulta (de la cláusula SETTINGS)

:::note
Las operaciones en segundo plano pueden configurarse mediante ajustes globales y ajustes del perfil &#39;background&#39;; los ajustes de sesión y de consulta no tienen efecto en este caso. Si no se proporciona ninguna configuración explícita, la configuración se heredará del contexto global. El nombre de perfil predeterminado para estas operaciones es &#39;background&#39; y puede sobrescribirse mediante el ajuste del servidor `background_profile`.
:::

<div id="threads-and-jobs">
  ## Hilos y trabajos
</div>

Para ejecutar consultas y realizar actividades secundarias, ClickHouse asigna hilos desde uno de los pool de hilos para evitar la creación y destrucción frecuente de hilos. Hay varios pool de hilos, que se seleccionan según el propósito y la estructura de un trabajo:

* Pool del servidor para las sesiones de cliente entrantes.
* Pool global de hilos para trabajos de propósito general, actividades en segundo plano e hilos independientes.
* Pool de hilos de IO para trabajos que están mayormente bloqueados por alguna operación de IO y no son intensivos en CPU.
* Pools en segundo plano para tareas periódicas.
* Pools para tareas expropiables que pueden dividirse en pasos.

El pool del servidor es una instancia de la clase `Poco::ThreadPool` definida en el método `Server::main()`. Puede tener como máximo `max_connection` hilos. Cada hilo está dedicado a una única connection activa.

El pool global de hilos es la clase singleton `GlobalThreadPool`. Para asignar un hilo desde él se usa `ThreadFromGlobalPool`. Tiene una interfaz similar a `std::thread`, pero toma un hilo del pool global y realiza toda la inicialización necesaria. Se configura con los siguientes ajustes:

* `max_thread_pool_size` - límite del número de hilos en el pool.
* `max_thread_pool_free_size` - límite del número de hilos inactivos en espera de nuevos trabajos.
* `thread_pool_queue_size` - límite del número de trabajos planificados.

El pool global es universal y todos los pools descritos a continuación se implementan sobre él. Esto puede entenderse como una jerarquía de pools. Cualquier pool especializado toma sus hilos del pool global usando la clase `ThreadPool`. Por lo tanto, el propósito principal de cualquier pool especializado es aplicar un límite al número de trabajos simultáneos y realizar la planificación de trabajos. Si hay más trabajos planificados que hilos en un pool, `ThreadPool` acumula los trabajos en una queue con prioridades. Cada trabajo tiene una prioridad entera. La prioridad predeterminada es cero. Todos los trabajos con valores de prioridad más altos se inician antes que cualquier trabajo con un valor de prioridad más bajo. Pero no hay diferencia entre los trabajos que ya se están ejecutando, por lo que la prioridad solo importa cuando el pool está sobrecargado.

El pool de hilos de IO está implementado como un `ThreadPool` simple accesible mediante el método `IOThreadPool::get()`. Se configura de la misma forma que el pool global con los ajustes `max_io_thread_pool_size`, `max_io_thread_pool_free_size` e `io_thread_pool_queue_size`. El propósito principal del pool de hilos de IO es evitar que los trabajos de IO agoten el pool global, lo que podría impedir que las consultas aprovechen completamente la CPU. Backup a S3 realiza una cantidad significativa de operaciones de IO y, para evitar el impacto en las consultas interactivas, existe un `BackupsIOThreadPool` independiente configurado con los ajustes `max_backups_io_thread_pool_size`, `max_backups_io_thread_pool_free_size` y `backups_io_thread_pool_queue_size`.

Para la ejecución de tareas periódicas existe la clase `BackgroundSchedulePool`. Puede registrar tareas usando objetos `BackgroundSchedulePool::TaskHolder` y el pool garantiza que ninguna tarea ejecute dos trabajos al mismo tiempo. También permite posponer la ejecución de una tarea hasta un instante específico en el futuro o desactivarla temporalmente. El `Context` global proporciona algunas instancias de esta clase para distintos propósitos. Para tareas de propósito general se usa `Context::getSchedulePool()`.

También hay pool de hilos especializados para tareas expropiables. Una tarea `IExecutableTask` de este tipo puede dividirse en una secuencia ordenada de trabajos, llamados pasos. Para planificar estas tareas de un modo que permita priorizar las tareas cortas sobre las largas, se usa `MergeTreeBackgroundExecutor`. Como su nombre sugiere, se utiliza para operaciones en segundo plano relacionadas con MergeTree, como merges, mutations, fetches y movimientos. Las instancias del pool están disponibles mediante `Context::getCommonExecutor()` y otros métodos similares.

Independientemente del pool que se use para un trabajo, al inicio se crea una instancia de `ThreadStatus` para ese trabajo. Encapsula toda la información por hilo: id del hilo, id de consulta, contadores de rendimiento, consumo de recursos y muchos otros datos útiles. El trabajo puede acceder a ella mediante un puntero local del hilo con la llamada `CurrentThread::get()`, por lo que no es necesario pasarla a cada función.

Si el hilo está relacionado con la ejecución de una consulta, entonces lo más importante asociado a `ThreadStatus` es el contexto de consulta `ContextPtr`. Cada consulta tiene su hilo maestro en el pool del servidor. El hilo maestro realiza esta asociación manteniendo un objeto `ThreadStatus::QueryScope query_scope(query_context)`. El hilo maestro también crea un grupo de hilos representado por el objeto `ThreadGroupStatus`. Cada hilo adicional que se asigna durante la ejecución de esta consulta se asocia a su grupo de hilos mediante la llamada `CurrentThread::attachTo(thread_group)`. Los grupos de hilos se utilizan para la agregación de contadores de eventos de perfil y para rastrear el consumo de memoria de todos los hilos dedicados a una sola tarea (consulte las clases `MemoryTracker` y `ProfileEvents::Counters` para más información).

<div id="concurrency-control">
  ## Control de concurrencia
</div>

Una consulta que puede paralelizarse usa la configuración `max_threads` para autolimitarse. El valor predeterminado de esta configuración se elige de forma que una sola consulta pueda aprovechar todos los núcleos de CPU de la mejor manera posible. Pero ¿qué ocurre si hay varias consultas concurrentes y cada una usa el valor predeterminado de la configuración `max_threads`? En ese caso, las consultas compartirán los recursos de CPU. El sistema operativo garantizará la equidad alternando constantemente entre hilos, lo que introduce cierta penalización en el rendimiento. `ConcurrencyControl` ayuda a reducir esta penalización y a evitar la asignación de demasiados hilos. La configuración `concurrent_threads_soft_limit_num` se usa para limitar cuántos hilos concurrentes pueden asignarse antes de aplicar algún tipo de presión sobre la CPU.

Se introduce la noción de `slot` de CPU. Un slot es una unidad de concurrencia: para ejecutar un hilo, una consulta debe adquirir un slot por adelantado y liberarlo cuando el hilo se detiene. El número de slots está limitado globalmente en un servidor. Varias consultas concurrentes compiten por slots de CPU si la demanda total supera el número total de slots. `ConcurrencyControl` se encarga de resolver esta competencia realizando la planificación de slots de CPU de forma justa.

Cada slot puede verse como una máquina de estados independiente con los siguientes estados:

* `free`: el slot está disponible para ser asignado a cualquier consulta.
* `granted`: el slot está `asignado` a una consulta específica, pero todavía no ha sido adquirido por ningún hilo.
* `acquired`: el slot está `asignado` a una consulta específica y ha sido adquirido por un hilo.

Tenga en cuenta que un slot `asignado` puede estar en dos estados diferentes: `granted` y `acquired`. El primero es un estado transitorio, que en realidad debería ser breve (desde el instante en que un slot se asigna a una consulta hasta el momento en que el procedimiento de escalado vertical es ejecutado por cualquier hilo de esa consulta).

```mermaid
stateDiagram-v2
    direction LR
    [*] --> free
    free --> allocated: allocate
    state allocated {
        direction LR
        [*] --> granted
        granted --> acquired: acquire
        acquired --> [*]
    }
    allocated --> free: release
```

La API de `ConcurrencyControl` incluye las siguientes funciones:

1. Crear una asignación de recursos para una consulta: `auto slots = ConcurrencyControl::instance().allocate(1, max_threads);`. Asignará como mínimo 1 y como máximo `max_threads` slots. Tenga en cuenta que el primer slot se otorga de inmediato, pero los slots restantes pueden otorgarse más adelante. Por tanto, el límite no es estricto, porque cada consulta obtendrá al menos un hilo.
2. Para cada hilo, se debe adquirir un slot de una asignación: `while (auto slot = slots->tryAcquire()) spawnThread([slot = std::move(slot)] { ... });`.
3. Actualizar la cantidad total de slots: `ConcurrencyControl::setMaxConcurrency(concurrent_threads_soft_limit_num)`. Puede hacerse en tiempo de ejecución, sin reiniciar el servidor.

Esta API permite que las consultas se inicien con al menos un hilo (cuando hay presión sobre la CPU) y luego aumenten hasta `max_threads`.

<div id="distributed-query-execution">
  ## Ejecución distribuida de consultas
</div>

Los servidores de una configuración en clúster son, en su mayor parte, independientes. Puede crear una tabla `Distributed` en uno o en todos los servidores de un clúster. La tabla `Distributed` no almacena datos por sí misma; solo proporciona una &quot;vista&quot; de todas las tablas locales en varios nodos del clúster. Cuando hace SELECT desde una tabla `Distributed`, reescribe esa consulta, elige los nodos remotos según la configuración de balanceo de carga y les envía la consulta. La tabla `Distributed` solicita a los servidores remotos que procesen una consulta solo hasta la etapa en la que los resultados intermedios de distintos servidores puedan fusionarse. Después, recibe esos resultados intermedios y los fusiona. La tabla distribuida intenta delegar la mayor cantidad de trabajo posible a los servidores remotos y evita enviar demasiados datos intermedios por la red.

La situación se complica cuando hay subconsultas en cláusulas IN o JOIN y cada una de ellas usa una tabla `Distributed`. Tenemos distintas estrategias para ejecutar estas consultas.

No existe un plan de consulta global para la ejecución distribuida de consultas. Cada nodo tiene su plan de consulta local para su parte del trabajo. Solo contamos con una ejecución distribuida de consultas simple, de una sola pasada: enviamos consultas a nodos remotos y luego fusionamos los resultados. Pero esto no es viable para consultas complejas con `GROUP BY` de alta cardinalidad o con una gran cantidad de datos temporales para JOIN. En esos casos, necesitamos &quot;redistribuir&quot; los datos entre servidores, lo que requiere coordinación adicional. ClickHouse no admite ese tipo de ejecución de consultas, y tenemos que seguir trabajando en ello.

<div id="merge-tree">
  ## MergeTree
</div>

`MergeTree` es una familia de motores de almacenamiento que admite indexación por clave primaria. La clave primaria puede ser una tupla arbitraria de columnas o expresiones. Los datos de una tabla `MergeTree` se almacenan en &quot;partes&quot;. Cada parte almacena los datos en el orden de la clave primaria, por lo que los datos quedan ordenados lexicográficamente por la tupla de la clave primaria. Todas las columnas de la tabla se almacenan en archivos `column.bin` independientes dentro de estas partes. Los archivos constan de bloques comprimidos. Cada bloque suele contener entre 64 KB y 1 MB de datos sin comprimir, según el tamaño medio de los valores. Los bloques constan de valores de columna colocados de forma contigua, uno detrás de otro. Los valores de columna están en el mismo orden en cada columna (la clave primaria define el orden), por lo que, cuando se recorre un gran número de columnas, se obtienen los valores de las filas correspondientes.

La propia clave primaria es &quot;dispersa&quot;. No apunta a cada fila individual, sino solo a algunos rangos de datos. Un archivo `primary.idx` independiente contiene el valor de la clave primaria para cada N-ésima fila, donde N se denomina `index_granularity` (normalmente, N = 8192). Además, para cada columna, hay archivos `column.mrk` con &quot;marcas&quot;, que son desplazamientos a cada N-ésima fila en el archivo de datos. Cada marca es un par: el desplazamiento en el archivo hasta el inicio del bloque comprimido y el desplazamiento en el bloque descomprimido hasta el inicio de los datos. Normalmente, los bloques comprimidos están alineados por marcas y el desplazamiento en el bloque descomprimido es cero. Los datos de `primary.idx` siempre residen en memoria, y los datos de los archivos `column.mrk` se almacenan en caché.

Cuando vamos a leer algo de una parte en `MergeTree`, consultamos los datos de `primary.idx` y localizamos los rangos que podrían contener los datos solicitados; después, consultamos los datos de `column.mrk` y calculamos los desplazamientos para determinar dónde empezar a leer esos rangos. Debido a esa naturaleza dispersa, es posible que se lean datos de más. ClickHouse no es adecuado para una carga elevada de consultas puntuales simples, porque debe leerse todo el rango con `index_granularity` filas para cada clave, y debe descomprimirse el bloque comprimido completo para cada columna. Hicimos el índice disperso porque debemos poder mantener billones de filas por servidor sin un consumo apreciable de memoria para el índice. Además, como la clave primaria es dispersa, no es única: no puede comprobar la existencia de la clave en la tabla en el momento del INSERT. Una tabla puede tener muchas filas con la misma clave.

Cuando haces `INSERT` de un conjunto de datos en `MergeTree`, ese conjunto se ordena según el orden de la clave primaria y forma una nueva parte. Hay hilos en segundo plano que seleccionan periódicamente algunas partes y las fusionan en una sola parte ordenada para mantener el número de partes relativamente bajo. Por eso se llama `MergeTree`. Por supuesto, la fusión provoca &quot;amplificación de escritura&quot;. Todas las partes son inmutables: solo se crean y se eliminan, pero no se modifican. Cuando se ejecuta SELECT, mantiene una instantánea de la tabla (un conjunto de partes). Después de la fusión, también conservamos las partes antiguas durante algún tiempo para facilitar la recuperación tras un fallo, de modo que, si vemos que alguna parte fusionada probablemente está dañada, podemos reemplazarla por sus partes de origen.

`MergeTree` no es un árbol LSM porque no contiene MEMTABLE ni LOG: los datos insertados se escriben directamente en el sistema de archivos. Este comportamiento hace que MergeTree sea mucho más adecuado para insertar datos en lotes. Por lo tanto, insertar con frecuencia pequeñas cantidades de filas no es ideal para MergeTree. Por ejemplo, un par de filas por segundo está bien, pero hacerlo mil veces por segundo no es óptimo para MergeTree. Sin embargo, existe un modo de inserción asíncrona para inserciones pequeñas que permite superar esta limitación. Lo hicimos así por simplicidad y porque ya insertamos datos en lotes en nuestras aplicaciones

Hay motores MergeTree que realizan trabajo adicional durante las fusiones en segundo plano. Algunos ejemplos son `CollapsingMergeTree` y `AggregatingMergeTree`. Esto puede considerarse una forma especial de soporte para actualizaciones. Ten en cuenta que no se trata de actualizaciones reales porque, por lo general, los usuarios no tienen control sobre cuándo se ejecutan las fusiones en segundo plano, y los datos de una tabla `MergeTree` casi siempre se almacenan en más de una parte, no en una forma completamente fusionada.

<div id="replication">
  ## Replicación
</div>

La replicación en ClickHouse puede configurarse por tabla. Puede tener algunas tablas replicadas y otras no replicadas en el mismo servidor. También puede tener tablas replicadas de distintas formas, por ejemplo, una tabla con replicación de dos factores y otra con tres.

La replicación se implementa en el motor de almacenamiento `ReplicatedMergeTree`. La ruta en `ZooKeeper` se especifica como parámetro del motor de almacenamiento. Todas las tablas con la misma ruta en `ZooKeeper` se convierten en réplicas entre sí: sincronizan sus datos y mantienen la consistencia. Las réplicas pueden añadirse y eliminarse dinámicamente simplemente creando o eliminando una tabla.

La replicación utiliza un esquema asíncrono multimáster. Puede insertar datos en cualquier réplica que tenga una sesión con `ZooKeeper`, y los datos se replican de forma asíncrona en todas las demás réplicas. Como ClickHouse no admite UPDATEs, la replicación no genera conflictos. Como, de forma predeterminada, no hay confirmación por quórum de las inserciones, los datos recién insertados podrían perderse si falla un nodo. El quórum de inserción puede habilitarse mediante la configuración `insert_quorum`.

Los metadatos de replicación se almacenan en ZooKeeper. Hay un registro de replicación que enumera las acciones que deben realizarse. Las acciones son: obtener una parte, fusionar partes, eliminar una partición, etc. Cada réplica copia el registro de replicación en su cola y luego ejecuta las acciones de esa cola. Por ejemplo, al insertar, se crea en el registro la acción &quot;obtener la parte&quot;, y cada réplica descarga esa parte. Las fusiones se coordinan entre réplicas para obtener resultados idénticos byte a byte. Todas las partes se fusionan de la misma manera en todas las réplicas. Uno de los líderes inicia primero una nueva fusión y escribe en el registro las acciones de &quot;fusionar partes&quot;. Varias réplicas (o incluso todas) pueden ser líderes al mismo tiempo. Puede impedirse que una réplica se convierta en líder mediante la configuración `merge_tree` `replicated_can_become_leader`. Los líderes son responsables de planificar las fusiones en segundo plano.

La replicación es física: entre nodos solo se transfieren partes comprimidas, no consultas. En la mayoría de los casos, las fusiones se procesan de forma independiente en cada réplica para reducir los costes de red y evitar la amplificación de tráfico. Las partes fusionadas de gran tamaño se envían por la red solo en casos de retraso de replicación significativo.

Además, cada réplica almacena su estado en ZooKeeper como el conjunto de partes y sus sumas de comprobación. Cuando el estado en el sistema de archivos local difiere del estado de referencia en ZooKeeper, la réplica restaura la consistencia descargando de otras réplicas las partes faltantes o dañadas. Cuando hay datos inesperados o dañados en el sistema de archivos local, ClickHouse no los elimina, sino que los mueve a un directorio independiente y deja de tenerlos en cuenta.

:::note
El clúster de ClickHouse consta de segmentos independientes, y cada segmento consta de réplicas. El clúster **no es elástico**, por lo que, después de añadir un nuevo segmento, los datos no se reequilibran automáticamente entre segmentos. En su lugar, se asume que la carga del clúster debe distribuirse de forma no uniforme. Esta implementación le da más control y es adecuada para clústeres relativamente pequeños, como los de decenas de nodos. Pero para clústeres con cientos de nodos que usamos en producción, este enfoque se convierte en un inconveniente importante. Deberíamos implementar un motor de tabla que abarque todo el clúster, con regiones replicadas dinámicamente que pudieran dividirse y equilibrarse automáticamente entre clústeres.
:::