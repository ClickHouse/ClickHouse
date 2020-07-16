---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Descripci\xF3n general de la arquitectura ClickHouse"
---

# Descripción general de la arquitectura ClickHouse {#overview-of-clickhouse-architecture}

ClickHouse es un verdadero DBMS orientado a columnas. Los datos se almacenan por columnas y durante la ejecución de matrices (vectores o fragmentos de columnas). Siempre que sea posible, las operaciones se envían en matrices, en lugar de en valores individuales. Se llama “vectorized query execution,” y ayuda a reducir el costo del procesamiento de datos real.

> Esta idea no es nada nuevo. Se remonta a la `APL` lenguaje de programación y sus descendientes: `A +`, `J`, `K`, y `Q`. La programación de matrices se utiliza en el procesamiento de datos científicos. Tampoco es esta idea algo nuevo en las bases de datos relacionales: por ejemplo, se usa en el `Vectorwise` sistema.

Existen dos enfoques diferentes para acelerar el procesamiento de consultas: la ejecución de consultas vectorizadas y la generación de código en tiempo de ejecución. Este último elimina toda la indirección y el despacho dinámico. Ninguno de estos enfoques es estrictamente mejor que el otro. La generación de código de tiempo de ejecución puede ser mejor cuando fusiona muchas operaciones, utilizando así las unidades de ejecución de la CPU y la canalización. La ejecución de consultas vectorizadas puede ser menos práctica porque implica vectores temporales que deben escribirse en la memoria caché y leerse. Si los datos temporales no caben en la memoria caché L2, esto se convierte en un problema. Pero la ejecución de consultas vectorizadas utiliza más fácilmente las capacidades SIMD de la CPU. Un [documento de investigación](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) escrito por nuestros amigos muestra que es mejor combinar ambos enfoques. ClickHouse utiliza la ejecución de consultas vectorizadas y tiene un soporte inicial limitado para la generación de código en tiempo de ejecución.

## Columna {#columns}

`IColumn` interfaz se utiliza para representar columnas en la memoria (en realidad, fragmentos de columnas). Esta interfaz proporciona métodos auxiliares para la implementación de varios operadores relacionales. Casi todas las operaciones son inmutables: no modifican la columna original, sino que crean una nueva modificada. Por ejemplo, el `IColumn :: filter` método acepta una máscara de bytes de filtro. Se utiliza para el `WHERE` y `HAVING` operadores relacionales. Ejemplos adicionales: el `IColumn :: permute` para apoyar `ORDER BY`, el `IColumn :: cut` para apoyar `LIMIT`.

Diversos `IColumn` aplicación (`ColumnUInt8`, `ColumnString`, y así sucesivamente) son responsables del diseño de memoria de las columnas. El diseño de memoria suele ser una matriz contigua. Para el tipo entero de columnas, es solo una matriz contigua, como `std :: vector`. Para `String` y `Array` columnas, son dos vectores: uno para todos los elementos de la matriz, colocados contiguamente, y un segundo para los desplazamientos al comienzo de cada matriz. También hay `ColumnConst` que almacena solo un valor en la memoria, pero parece una columna.

## Campo {#field}

Sin embargo, también es posible trabajar con valores individuales. Para representar un valor individual, el `Field` se utiliza. `Field` es sólo una unión discriminada de `UInt64`, `Int64`, `Float64`, `String` y `Array`. `IColumn` tiene el `operator[]` para obtener el valor n-ésimo como un `Field` y el `insert` método para agregar un `Field` al final de una columna. Estos métodos no son muy eficientes, ya que requieren tratar con temporal `Field` objetos que representan un valor individual. Hay métodos más eficientes, tales como `insertFrom`, `insertRangeFrom` y así sucesivamente.

`Field` no tiene suficiente información sobre un tipo de datos específico para una tabla. Por ejemplo, `UInt8`, `UInt16`, `UInt32`, y `UInt64` todos están representados como `UInt64` en una `Field`.

## Abstracciones con fugas {#leaky-abstractions}

`IColumn` tiene métodos para transformaciones relacionales comunes de datos, pero no satisfacen todas las necesidades. Por ejemplo, `ColumnUInt64` no tiene un método para calcular la suma de dos columnas, y `ColumnString` no tiene un método para ejecutar una búsqueda de subcadena. Estas innumerables rutinas se implementan fuera de `IColumn`.

Varias funciones en columnas se pueden implementar de una manera genérica, no eficiente utilizando `IColumn` para extraer `Field` valores, o de una manera especializada utilizando el conocimiento del diseño de la memoria interna de los datos en un `IColumn` aplicación. Se implementa mediante la conversión de funciones a un `IColumn` escriba y trate con la representación interna directamente. Por ejemplo, `ColumnUInt64` tiene el `getData` método que devuelve una referencia a una matriz interna, luego una rutina separada lee o llena esa matriz directamente. Tenemos “leaky abstractions” para permitir especializaciones eficientes de varias rutinas.

## Tipos de datos {#data_types}

`IDataType` es responsable de la serialización y deserialización: para leer y escribir fragmentos de columnas o valores individuales en formato binario o de texto. `IDataType` corresponde directamente a los tipos de datos en las tablas. Por ejemplo, hay `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` y así sucesivamente.

`IDataType` y `IColumn` están vagamente relacionados entre sí. Diferentes tipos de datos se pueden representar en la memoria por el mismo `IColumn` aplicación. Por ejemplo, `DataTypeUInt32` y `DataTypeDateTime` están representados por `ColumnUInt32` o `ColumnConstUInt32`. Además, el mismo tipo de datos se puede representar mediante `IColumn` aplicación. Por ejemplo, `DataTypeUInt8` puede ser representado por `ColumnUInt8` o `ColumnConstUInt8`.

`IDataType` sólo almacena metadatos. Por ejemplo, `DataTypeUInt8` no almacena nada en absoluto (excepto vptr) y `DataTypeFixedString` tiendas solo `N` (el tamaño de las cadenas de tamaño fijo).

`IDataType` tiene métodos auxiliares para varios formatos de datos. Los ejemplos son métodos para serializar un valor con posibles citas, para serializar un valor para JSON y para serializar un valor como parte del formato XML. No hay correspondencia directa con los formatos de datos. Por ejemplo, los diferentes formatos de datos `Pretty` y `TabSeparated` puede utilizar el mismo `serializeTextEscaped` método de ayuda de la `IDataType` interfaz.

## Bloque {#block}

A `Block` es un contenedor que representa un subconjunto (porción) de una tabla en la memoria. Es sólo un conjunto de triples: `(IColumn, IDataType, column name)`. Durante la ejecución de la consulta, los datos son procesados por `Block`s. Si tenemos un `Block`, tenemos datos (en el `IColumn` objeto), tenemos información sobre su tipo (en `IDataType`) que nos dice cómo lidiar con esa columna, y tenemos el nombre de la columna. Podría ser el nombre de columna original de la tabla o algún nombre artificial asignado para obtener resultados temporales de los cálculos.

Cuando calculamos alguna función sobre columnas en un bloque, agregamos otra columna con su resultado al bloque, y no tocamos columnas para argumentos de la función porque las operaciones son inmutables. Más tarde, las columnas innecesarias se pueden eliminar del bloque, pero no se pueden modificar. Es conveniente para la eliminación de subexpresiones comunes.

Se crean bloques para cada fragmento de datos procesado. Tenga en cuenta que para el mismo tipo de cálculo, los nombres y tipos de columna siguen siendo los mismos para diferentes bloques y solo cambian los datos de columna. Es mejor dividir los datos del bloque desde el encabezado del bloque porque los tamaños de bloque pequeños tienen una gran sobrecarga de cadenas temporales para copiar shared\_ptrs y nombres de columna.

## Bloquear flujos {#block-streams}

Los flujos de bloques son para procesar datos. Usamos flujos de bloques para leer datos de algún lugar, realizar transformaciones de datos o escribir datos en algún lugar. `IBlockInputStream` tiene el `read` método para buscar el siguiente bloque mientras esté disponible. `IBlockOutputStream` tiene el `write` método para empujar el bloque en alguna parte.

Los flujos son responsables de:

1.  Leer o escribir en una mesa. La tabla solo devuelve una secuencia para leer o escribir bloques.
2.  Implementación de formatos de datos. Por ejemplo, si desea enviar datos a un terminal en `Pretty` formato, crea un flujo de salida de bloque donde presiona bloques y los formatea.
3.  Realización de transformaciones de datos. Digamos que tienes `IBlockInputStream` y desea crear una secuencia filtrada. Usted crea `FilterBlockInputStream` e inicializarlo con su transmisión. Luego, cuando tiras de un bloque de `FilterBlockInputStream`, extrae un bloque de su flujo, lo filtra y le devuelve el bloque filtrado. Las canalizaciones de ejecución de consultas se representan de esta manera.

Hay transformaciones más sofisticadas. Por ejemplo, cuando tiras de `AggregatingBlockInputStream`, lee todos los datos de su origen, los agrega y, a continuación, devuelve un flujo de datos agregados para usted. Otro ejemplo: `UnionBlockInputStream` acepta muchas fuentes de entrada en el constructor y también una serie de subprocesos. Lanza múltiples hilos y lee de múltiples fuentes en paralelo.

> Las secuencias de bloques usan el “pull” enfoque para controlar el flujo: cuando extrae un bloque de la primera secuencia, en consecuencia extrae los bloques requeridos de las secuencias anidadas, y toda la tubería de ejecución funcionará. Ni “pull” ni “push” es la mejor solución, porque el flujo de control está implícito y eso limita la implementación de varias características, como la ejecución simultánea de múltiples consultas (fusionando muchas tuberías). Esta limitación podría superarse con coroutines o simplemente ejecutando hilos adicionales que se esperan el uno al otro. Podemos tener más posibilidades si hacemos explícito el flujo de control: si localizamos la lógica para pasar datos de una unidad de cálculo a otra fuera de esas unidades de cálculo. Lea esto [artículo](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/) para más pensamientos.

Debemos tener en cuenta que la canalización de ejecución de consultas crea datos temporales en cada paso. Tratamos de mantener el tamaño del bloque lo suficientemente pequeño para que los datos temporales se ajusten a la memoria caché de la CPU. Con esa suposición, escribir y leer datos temporales es casi gratis en comparación con otros cálculos. Podríamos considerar una alternativa, que es fusionar muchas operaciones en la tubería. Podría hacer que la tubería sea lo más corta posible y eliminar gran parte de los datos temporales, lo que podría ser una ventaja, pero también tiene inconvenientes. Por ejemplo, una canalización dividida facilita la implementación de almacenamiento en caché de datos intermedios, el robo de datos intermedios de consultas similares que se ejecutan al mismo tiempo y la fusión de canalizaciones para consultas similares.

## Formato {#formats}

Los formatos de datos se implementan con flujos de bloques. Hay “presentational” sólo es adecuado para la salida de datos al cliente, tales como `Pretty` formato, que proporciona sólo `IBlockOutputStream`. Y hay formatos de entrada / salida, como `TabSeparated` o `JSONEachRow`.

También hay secuencias de filas: `IRowInputStream` y `IRowOutputStream`. Permiten pull/push datos por filas individuales, no por bloques. Y solo son necesarios para simplificar la implementación de formatos orientados a filas. Envoltura `BlockInputStreamFromRowInputStream` y `BlockOutputStreamFromRowOutputStream` le permite convertir flujos orientados a filas en flujos regulares orientados a bloques.

## I/O {#io}

Para la entrada / salida orientada a bytes, hay `ReadBuffer` y `WriteBuffer` clases abstractas. Se usan en lugar de C ++ `iostream`s. No se preocupe: cada proyecto maduro de C ++ está usando algo más que `iostream`s por buenas razones.

`ReadBuffer` y `WriteBuffer` son solo un búfer contiguo y un cursor apuntando a la posición en ese búfer. Las implementaciones pueden poseer o no la memoria del búfer. Hay un método virtual para llenar el búfer con los siguientes datos (para `ReadBuffer`) o para vaciar el búfer en algún lugar (para `WriteBuffer`). Los métodos virtuales rara vez se llaman.

Implementaciones de `ReadBuffer`/`WriteBuffer` se utilizan para trabajar con archivos y descriptores de archivos y sockets de red, para implementar la compresión (`CompressedWriteBuffer` is initialized with another WriteBuffer and performs compression before writing data to it), and for other purposes – the names `ConcatReadBuffer`, `LimitReadBuffer`, y `HashingWriteBuffer` hablar por sí mismos.

Read / WriteBuffers solo se ocupan de bytes. Hay funciones de `ReadHelpers` y `WriteHelpers` archivos de encabezado para ayudar con el formato de entrada / salida. Por ejemplo, hay ayudantes para escribir un número en formato decimal.

Veamos qué sucede cuando quieres escribir un conjunto de resultados en `JSON` formato a stdout. Tiene un conjunto de resultados listo para ser recuperado de `IBlockInputStream`. Usted crea `WriteBufferFromFileDescriptor(STDOUT_FILENO)` para escribir bytes en stdout. Usted crea `JSONRowOutputStream`, inicializado con eso `WriteBuffer` para escribir filas en `JSON` a stdout. Usted crea `BlockOutputStreamFromRowOutputStream` encima de él, para representarlo como `IBlockOutputStream`. Entonces usted llama `copyData` para transferir datos desde `IBlockInputStream` a `IBlockOutputStream` y todo funciona. Internamente, `JSONRowOutputStream` escribirá varios delimitadores JSON y llamará al `IDataType::serializeTextJSON` con una referencia a `IColumn` y el número de fila como argumentos. Consecuentemente, `IDataType::serializeTextJSON` llamará a un método de `WriteHelpers.h`: por ejemplo, `writeText` para tipos numéricos y `writeJSONString` para `DataTypeString`.

## Tabla {#tables}

El `IStorage` interfaz representa tablas. Las diferentes implementaciones de esa interfaz son diferentes motores de tabla. Los ejemplos son `StorageMergeTree`, `StorageMemory` y así sucesivamente. Las instancias de estas clases son solo tablas.

Clave `IStorage` son `read` y `write`. También hay `alter`, `rename`, `drop` y así sucesivamente. El `read` método acepta los siguientes argumentos: el conjunto de columnas para leer de una tabla, el `AST` consulta a considerar, y el número deseado de flujos para devolver. Devuelve uno o varios `IBlockInputStream` objetos e información sobre la etapa de procesamiento de datos que se completó dentro de un motor de tablas durante la ejecución de la consulta.

En la mayoría de los casos, el método de lectura solo es responsable de leer las columnas especificadas de una tabla, no de ningún procesamiento de datos adicional. Todo el procesamiento de datos adicional es realizado por el intérprete de consultas y está fuera de la responsabilidad de `IStorage`.

Pero hay excepciones notables:

-   La consulta AST se pasa al `read` método, y el motor de tablas puede usarlo para derivar el uso del índice y leer menos datos de una tabla.
-   A veces, el motor de tablas puede procesar los datos a una etapa específica. Por ejemplo, `StorageDistributed` puede enviar una consulta a servidores remotos, pedirles que procesen datos a una etapa donde se puedan fusionar datos de diferentes servidores remotos y devolver esos datos preprocesados. El intérprete de consultas termina de procesar los datos.

Tabla `read` método puede devolver múltiples `IBlockInputStream` objetos para permitir el procesamiento de datos en paralelo. Estos flujos de entrada de bloques múltiples pueden leer de una tabla en paralelo. A continuación, puede ajustar estas secuencias con varias transformaciones (como la evaluación de expresiones o el filtrado) que se pueden calcular de forma independiente y crear un `UnionBlockInputStream` encima de ellos, para leer desde múltiples flujos en paralelo.

También hay `TableFunction`s. Estas son funciones que devuelven un `IStorage` objeto a utilizar en el `FROM` cláusula de una consulta.

Para tener una idea rápida de cómo implementar su motor de tabla, vea algo simple, como `StorageMemory` o `StorageTinyLog`.

> Como resultado de la `read` método, `IStorage` devoluciones `QueryProcessingStage` – information about what parts of the query were already calculated inside storage.

## Analizador {#parsers}

Un analizador de descenso recursivo escrito a mano analiza una consulta. Por ejemplo, `ParserSelectQuery` simplemente llama recursivamente a los analizadores subyacentes para varias partes de la consulta. Los analizadores crean un `AST`. El `AST` está representado por nodos, que son instancias de `IAST`.

> Los generadores de analizadores no se utilizan por razones históricas.

## Interprete {#interpreters}

Los intérpretes son responsables de crear la canalización de ejecución de consultas `AST`. Hay intérpretes simples, como `InterpreterExistsQuery` y `InterpreterDropQuery` o el más sofisticado `InterpreterSelectQuery`. La canalización de ejecución de consultas es una combinación de flujos de entrada o salida de bloques. Por ejemplo, el resultado de interpretar el `SELECT` la consulta es la `IBlockInputStream` para leer el conjunto de resultados; el resultado de la consulta INSERT es el `IBlockOutputStream` para escribir datos para su inserción, y el resultado de interpretar el `INSERT SELECT` la consulta es la `IBlockInputStream` que devuelve un conjunto de resultados vacío en la primera lectura, pero que copia datos de `SELECT` a `INSERT` al mismo tiempo.

`InterpreterSelectQuery` utilizar `ExpressionAnalyzer` y `ExpressionActions` maquinaria para el análisis de consultas y transformaciones. Aquí es donde se realizan la mayoría de las optimizaciones de consultas basadas en reglas. `ExpressionAnalyzer` es bastante complicado y debe reescribirse: se deben extraer varias transformaciones de consultas y optimizaciones para separar clases para permitir transformaciones modulares o consultas.

## Función {#functions}

Hay funciones ordinarias y funciones agregadas. Para las funciones agregadas, consulte la siguiente sección.

Ordinary functions don't change the number of rows – they work as if they are processing each row independently. In fact, functions are not called for individual rows, but for `Block`de datos para implementar la ejecución de consultas vectorizadas.

Hay algunas funciones diversas, como [BlockSize](../sql-reference/functions/other-functions.md#function-blocksize), [rowNumberInBlock](../sql-reference/functions/other-functions.md#function-rownumberinblock), y [runningAccumulate](../sql-reference/functions/other-functions.md#function-runningaccumulate), que explotan el procesamiento de bloques y violan la independencia de las filas.

ClickHouse tiene una tipificación fuerte, por lo que no hay conversión de tipo implícita. Si una función no admite una combinación específica de tipos, produce una excepción. Pero las funciones pueden funcionar (estar sobrecargadas) para muchas combinaciones diferentes de tipos. Por ejemplo, el `plus` función (para implementar el `+` operador) funciona para cualquier combinación de tipos numéricos: `UInt8` + `Float32`, `UInt16` + `Int8` y así sucesivamente. Además, algunas funciones variadas pueden aceptar cualquier número de argumentos, como el `concat` función.

Implementar una función puede ser un poco inconveniente porque una función distribuye explícitamente tipos de datos compatibles y `IColumns`. Por ejemplo, el `plus` La función tiene código generado por la creación de instancias de una plantilla de C ++ para cada combinación de tipos numéricos y argumentos izquierdo y derecho constantes o no constantes.

Es un excelente lugar para implementar la generación de código en tiempo de ejecución para evitar la hinchazón del código de plantilla. Además, permite agregar funciones fusionadas como multiplicar-agregar fusionado o hacer comparaciones múltiples en una iteración de bucle.

Debido a la ejecución de consultas vectorizadas, las funciones no se cortocircuitan. Por ejemplo, si escribe `WHERE f(x) AND g(y)`, ambos lados se calculan, incluso para las filas, cuando `f(x)` es cero (excepto cuando `f(x)` es una expresión constante cero). Pero si la selectividad del `f(x)` la condición es alta, y el cálculo de `f(x)` es mucho más barato que `g(y)`, es mejor implementar el cálculo de paso múltiple. Primero calcularía `f(x)`, a continuación, filtrar columnas por el resultado, y luego calcular `g(y)` solo para trozos de datos más pequeños y filtrados.

## Funciones agregadas {#aggregate-functions}

Las funciones agregadas son funciones con estado. Acumulan valores pasados en algún estado y le permiten obtener resultados de ese estado. Se gestionan con el `IAggregateFunction` interfaz. Los estados pueden ser bastante simples (el estado para `AggregateFunctionCount` es sólo una sola `UInt64` valor) o bastante complejo (el estado de `AggregateFunctionUniqCombined` es una combinación de una matriz lineal, una tabla hash, y un `HyperLogLog` estructura de datos probabilística).

Los Estados están asignados en `Arena` (un grupo de memoria) para tratar con múltiples estados mientras se ejecuta una alta cardinalidad `GROUP BY` consulta. Los estados pueden tener un constructor y destructor no trivial: por ejemplo, los estados de agregación complicados pueden asignar memoria adicional ellos mismos. Requiere cierta atención a la creación y destrucción de estados y a la adecuada aprobación de su orden de propiedad y destrucción.

Los estados de agregación se pueden serializar y deserializar para pasar a través de la red durante la ejecución de consultas distribuidas o para escribirlos en el disco donde no hay suficiente RAM. Incluso se pueden almacenar en una tabla con el `DataTypeAggregateFunction` para permitir la agregación incremental de datos.

> El formato de datos serializados para los estados de función agregados no tiene versiones en este momento. Está bien si los estados agregados solo se almacenan temporalmente. Pero tenemos el `AggregatingMergeTree` motor de tabla para la agregación incremental, y la gente ya lo está utilizando en producción. Es la razón por la que se requiere compatibilidad con versiones anteriores al cambiar el formato serializado para cualquier función agregada en el futuro.

## Servidor {#server}

El servidor implementa varias interfaces diferentes:

-   Una interfaz HTTP para cualquier cliente extranjero.
-   Una interfaz TCP para el cliente nativo de ClickHouse y para la comunicación entre servidores durante la ejecución de consultas distribuidas.
-   Una interfaz para transferir datos para la replicación.

Internamente, es solo un servidor multiproceso primitivo sin corutinas ni fibras. Dado que el servidor no está diseñado para procesar una alta tasa de consultas simples, sino para procesar una tasa relativamente baja de consultas complejas, cada uno de ellos puede procesar una gran cantidad de datos para análisis.

El servidor inicializa el `Context` clase con el entorno necesario para la ejecución de consultas: la lista de bases de datos disponibles, usuarios y derechos de acceso, configuración, clústeres, la lista de procesos, el registro de consultas, etc. Los intérpretes utilizan este entorno.

Mantenemos una compatibilidad total con versiones anteriores y posteriores para el protocolo TCP del servidor: los clientes antiguos pueden hablar con servidores nuevos y los nuevos clientes pueden hablar con servidores antiguos. Pero no queremos mantenerlo eternamente, y estamos eliminando el soporte para versiones antiguas después de aproximadamente un año.

!!! note "Nota"
    Para la mayoría de las aplicaciones externas, recomendamos usar la interfaz HTTP porque es simple y fácil de usar. El protocolo TCP está más estrechamente vinculado a las estructuras de datos internas: utiliza un formato interno para pasar bloques de datos y utiliza marcos personalizados para datos comprimidos. No hemos lanzado una biblioteca C para ese protocolo porque requiere vincular la mayor parte de la base de código ClickHouse, lo cual no es práctico.

## Ejecución de consultas distribuidas {#distributed-query-execution}

Los servidores de una configuración de clúster son en su mayoría independientes. Puede crear un `Distributed` en uno o todos los servidores de un clúster. El `Distributed` table does not store data itself – it only provides a “view” a todas las tablas locales en varios nodos de un clúster. Cuando se SELECCIONA desde un `Distributed` tabla, reescribe esa consulta, elige nodos remotos de acuerdo con la configuración de equilibrio de carga y les envía la consulta. El `Distributed` table solicita a los servidores remotos que procesen una consulta hasta una etapa en la que se pueden fusionar resultados intermedios de diferentes servidores. Luego recibe los resultados intermedios y los fusiona. La tabla distribuida intenta distribuir tanto trabajo como sea posible a servidores remotos y no envía muchos datos intermedios a través de la red.

Las cosas se vuelven más complicadas cuando tiene subconsultas en cláusulas IN o JOIN, y cada una de ellas usa un `Distributed` tabla. Tenemos diferentes estrategias para la ejecución de estas consultas.

No existe un plan de consulta global para la ejecución de consultas distribuidas. Cada nodo tiene su plan de consulta local para su parte del trabajo. Solo tenemos una ejecución simple de consultas distribuidas de un solo paso: enviamos consultas para nodos remotos y luego fusionamos los resultados. Pero esto no es factible para consultas complicadas con alta cardinalidad GROUP BY o con una gran cantidad de datos temporales para JOIN. En tales casos, necesitamos “reshuffle” datos entre servidores, lo que requiere una coordinación adicional. ClickHouse no admite ese tipo de ejecución de consultas, y tenemos que trabajar en ello.

## Árbol de fusión {#merge-tree}

`MergeTree` es una familia de motores de almacenamiento que admite la indexación por clave principal. La clave principal puede ser una tupla arbitraria de columnas o expresiones. Datos en un `MergeTree` se almacena en “parts”. Cada parte almacena los datos en el orden de la clave principal, por lo que la tupla de la clave principal ordena los datos lexicográficamente. Todas las columnas de la tabla se almacenan en `column.bin` archivos en estas partes. Los archivos consisten en bloques comprimidos. Cada bloque suele ser de 64 KB a 1 MB de datos sin comprimir, dependiendo del tamaño del valor promedio. Los bloques constan de valores de columna colocados contiguamente uno tras otro. Los valores de columna están en el mismo orden para cada columna (la clave principal define el orden), por lo que cuando itera por muchas columnas, obtiene valores para las filas correspondientes.

La clave principal en sí es “sparse”. No aborda cada fila, sino solo algunos rangos de datos. Separado `primary.idx` file tiene el valor de la clave principal para cada fila N-ésima, donde se llama N `index_granularity` (generalmente, N = 8192). Además, para cada columna, tenemos `column.mrk` archivos con “marks,” que son desplazamientos a cada fila N-ésima en el archivo de datos. Cada marca es un par: el desplazamiento en el archivo al comienzo del bloque comprimido y el desplazamiento en el bloque descomprimido al comienzo de los datos. Por lo general, los bloques comprimidos están alineados por marcas, y el desplazamiento en el bloque descomprimido es cero. Datos para `primary.idx` siempre reside en la memoria, y los datos para `column.mrk` archivos se almacena en caché.

Cuando vamos a leer algo de una parte en `MergeTree` miramos `primary.idx` datos y localice rangos que podrían contener datos solicitados, luego mire `column.mrk` datos y calcular compensaciones para dónde comenzar a leer esos rangos. Debido a la escasez, el exceso de datos puede ser leído. ClickHouse no es adecuado para una gran carga de consultas de puntos simples, porque todo el rango con `index_granularity` se deben leer filas para cada clave, y todo el bloque comprimido debe descomprimirse para cada columna. Hicimos que el índice sea disperso porque debemos poder mantener billones de filas por único servidor sin un consumo de memoria notable para el índice. Además, debido a que la clave principal es escasa, no es única: no puede verificar la existencia de la clave en la tabla en el momento de INSERTAR. Podría tener muchas filas con la misma clave en una tabla.

Cuando `INSERT` un montón de datos en `MergeTree`, ese grupo está ordenado por orden de clave primaria y forma una nueva parte. Hay subprocesos de fondo que seleccionan periódicamente algunas partes y las fusionan en una sola parte ordenada para mantener el número de partes relativamente bajo. Es por eso que se llama `MergeTree`. Por supuesto, la fusión conduce a “write amplification”. Todas las partes son inmutables: solo se crean y eliminan, pero no se modifican. Cuando se ejecuta SELECT, contiene una instantánea de la tabla (un conjunto de partes). Después de la fusión, también mantenemos las piezas viejas durante algún tiempo para facilitar la recuperación después de la falla, por lo que si vemos que alguna parte fusionada probablemente esté rota, podemos reemplazarla con sus partes de origen.

`MergeTree` no es un árbol de LSM porque no contiene “memtable” y “log”: inserted data is written directly to the filesystem. This makes it suitable only to INSERT data in batches, not by individual row and not very frequently – about once per second is ok, but a thousand times a second is not. We did it this way for simplicity's sake, and because we are already inserting data in batches in our applications.

> Las tablas MergeTree solo pueden tener un índice (primario): no hay índices secundarios. Sería bueno permitir múltiples representaciones físicas bajo una tabla lógica, por ejemplo, para almacenar datos en más de un orden físico o incluso para permitir representaciones con datos preagregados junto con datos originales.

Hay motores MergeTree que están haciendo un trabajo adicional durante las fusiones en segundo plano. Los ejemplos son `CollapsingMergeTree` y `AggregatingMergeTree`. Esto podría tratarse como soporte especial para actualizaciones. Tenga en cuenta que estas no son actualizaciones reales porque los usuarios generalmente no tienen control sobre el tiempo en que se ejecutan las fusiones en segundo plano y los datos en un `MergeTree` casi siempre se almacena en más de una parte, no en forma completamente fusionada.

## Replicación {#replication}

La replicación en ClickHouse se puede configurar por tabla. Podría tener algunas tablas replicadas y otras no replicadas en el mismo servidor. También puede tener tablas replicadas de diferentes maneras, como una tabla con replicación de dos factores y otra con replicación de tres factores.

La replicación se implementa en el `ReplicatedMergeTree` motor de almacenamiento. El camino en `ZooKeeper` se especifica como un parámetro para el motor de almacenamiento. Todas las tablas con la misma ruta en `ZooKeeper` se convierten en réplicas entre sí: sincronizan sus datos y mantienen la coherencia. Las réplicas se pueden agregar y eliminar dinámicamente simplemente creando o soltando una tabla.

La replicación utiliza un esquema multi-maestro asíncrono. Puede insertar datos en cualquier réplica que tenga una sesión con `ZooKeeper`, y los datos se replican en todas las demás réplicas de forma asíncrona. Como ClickHouse no admite UPDATE, la replicación está libre de conflictos. Como no hay reconocimiento de quórum de inserciones, los datos recién insertados pueden perderse si un nodo falla.

Los metadatos para la replicación se almacenan en ZooKeeper. Hay un registro de replicación que enumera las acciones que se deben realizar. Las acciones son: obtener parte; fusionar partes; soltar una partición, etc. Cada réplica copia el registro de replicación en su cola y, a continuación, ejecuta las acciones desde la cola. Por ejemplo, en la inserción, el “get the part” la acción se crea en el registro y cada réplica descarga esa parte. Las fusiones se coordinan entre réplicas para obtener resultados idénticos en bytes. Todas las piezas se combinan de la misma manera en todas las réplicas. Se logra eligiendo una réplica como líder, y esa réplica inicia fusiones y escrituras “merge parts” acciones al registro.

La replicación es física: solo las partes comprimidas se transfieren entre nodos, no consultas. Las fusiones se procesan en cada réplica de forma independiente en la mayoría de los casos para reducir los costos de red al evitar la amplificación de la red. Las piezas combinadas grandes se envían a través de la red solo en casos de retraso de replicación significativo.

Además, cada réplica almacena su estado en ZooKeeper como el conjunto de piezas y sus sumas de comprobación. Cuando el estado en el sistema de archivos local difiere del estado de referencia en ZooKeeper, la réplica restaura su coherencia descargando partes faltantes y rotas de otras réplicas. Cuando hay algunos datos inesperados o rotos en el sistema de archivos local, ClickHouse no los elimina, sino que los mueve a un directorio separado y los olvida.

!!! note "Nota"
    El clúster ClickHouse consta de fragmentos independientes y cada fragmento consta de réplicas. El clúster es **no elástico**, por lo tanto, después de agregar un nuevo fragmento, los datos no se reequilibran automáticamente entre fragmentos. En su lugar, se supone que la carga del clúster debe ajustarse para que sea desigual. Esta implementación le da más control, y está bien para clústeres relativamente pequeños, como decenas de nodos. Pero para los clústeres con cientos de nodos que estamos utilizando en producción, este enfoque se convierte en un inconveniente significativo. Debemos implementar un motor de tablas que abarque todo el clúster con regiones replicadas dinámicamente que puedan dividirse y equilibrarse entre clústeres automáticamente.

{## [Artículo Original](https://clickhouse.tech/docs/en/development/architecture/) ##}
