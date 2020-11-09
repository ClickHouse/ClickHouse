---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 69
toc_title: "C\xF3mo ejecutar pruebas de ClickHouse"
---

# Pruebas de ClickHouse {#clickhouse-testing}

## Pruebas funcionales {#functional-tests}

Las pruebas funcionales son las más simples y cómodas de usar. La mayoría de las características de ClickHouse se pueden probar con pruebas funcionales y son obligatorias para cada cambio en el código de ClickHouse que se puede probar de esa manera.

Cada prueba funcional envía una o varias consultas al servidor ClickHouse en ejecución y compara el resultado con la referencia.

Las pruebas se encuentran en `queries` directorio. Hay dos subdirectorios: `stateless` y `stateful`. Las pruebas sin estado ejecutan consultas sin datos de prueba precargados: a menudo crean pequeños conjuntos de datos sintéticos sobre la marcha, dentro de la prueba misma. Las pruebas estatales requieren datos de prueba precargados de Yandex.Métrica y no está disponible para el público en general. Tendemos a usar sólo `stateless` pruebas y evitar la adición de nuevos `stateful` prueba.

Cada prueba puede ser de dos tipos: `.sql` y `.sh`. `.sql` test es el script SQL simple que se canaliza a `clickhouse-client --multiquery --testmode`. `.sh` test es un script que se ejecuta por sí mismo.

Para ejecutar todas las pruebas, use `clickhouse-test` herramienta. Mira `--help` para la lista de posibles opciones. Simplemente puede ejecutar todas las pruebas o ejecutar un subconjunto de pruebas filtradas por subcadena en el nombre de la prueba: `./clickhouse-test substring`.

La forma más sencilla de invocar pruebas funcionales es copiar `clickhouse-client` a `/usr/bin/`, ejecutar `clickhouse-server` y luego ejecutar `./clickhouse-test` de su propio directorio.

Para agregar una nueva prueba, cree un `.sql` o `.sh` archivo en `queries/0_stateless` directorio, compruébelo manualmente y luego genere `.reference` archivo de la siguiente manera: `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` o `./00000_test.sh > ./00000_test.reference`.

Las pruebas deben usar (crear, soltar, etc.) solo tablas en `test` base de datos que se supone que se crea de antemano; también las pruebas pueden usar tablas temporales.

Si desea utilizar consultas distribuidas en pruebas funcionales, puede aprovechar `remote` función de la tabla con `127.0.0.{1..2}` direcciones para que el servidor se consulte; o puede usar clústeres de prueba predefinidos en el archivo de configuración del servidor como `test_shard_localhost`.

Algunas pruebas están marcadas con `zookeeper`, `shard` o `long` en sus nombres.
`zookeeper` es para pruebas que están usando ZooKeeper. `shard` es para pruebas que
requiere servidor para escuchar `127.0.0.*`; `distributed` o `global` tienen el mismo
significado. `long` es para pruebas que duran un poco más de un segundo. Usted puede
deshabilitar estos grupos de pruebas utilizando `--no-zookeeper`, `--no-shard` y
`--no-long` opciones, respectivamente.

## Bugs Conocidos {#known-bugs}

Si conocemos algunos errores que se pueden reproducir fácilmente mediante pruebas funcionales, colocamos pruebas funcionales preparadas en `tests/queries/bugs` directorio. Estas pruebas se moverán a `tests/queries/0_stateless` cuando se corrigen errores.

## Pruebas de integración {#integration-tests}

Las pruebas de integración permiten probar ClickHouse en la configuración agrupada y la interacción de ClickHouse con otros servidores como MySQL, Postgres, MongoDB. Son útiles para emular divisiones de red, caídas de paquetes, etc. Estas pruebas se ejecutan bajo Docker y crean múltiples contenedores con varios software.

Ver `tests/integration/README.md` sobre cómo ejecutar estas pruebas.

Tenga en cuenta que la integración de ClickHouse con controladores de terceros no se ha probado. Además, actualmente no tenemos pruebas de integración con nuestros controladores JDBC y ODBC.

## Pruebas unitarias {#unit-tests}

Las pruebas unitarias son útiles cuando desea probar no ClickHouse como un todo, sino una sola biblioteca o clase aislada. Puede habilitar o deshabilitar la compilación de pruebas con `ENABLE_TESTS` Opción CMake. Las pruebas unitarias (y otros programas de prueba) se encuentran en `tests` subdirectorios en todo el código. Para ejecutar pruebas unitarias, escriba `ninja test`. Algunas pruebas usan `gtest`, pero algunos son solo programas que devuelven un código de salida distinto de cero en caso de fallo de prueba.

No es necesariamente tener pruebas unitarias si el código ya está cubierto por pruebas funcionales (y las pruebas funcionales suelen ser mucho más simples de usar).

## Pruebas de rendimiento {#performance-tests}

Las pruebas de rendimiento permiten medir y comparar el rendimiento de alguna parte aislada de ClickHouse en consultas sintéticas. Las pruebas se encuentran en `tests/performance`. Cada prueba está representada por `.xml` archivo con la descripción del caso de prueba. Las pruebas se ejecutan con `clickhouse performance-test` herramienta (que está incrustada en `clickhouse` binario). Ver `--help` para la invocación.

Cada prueba ejecuta una o varias consultas (posiblemente con combinaciones de parámetros) en un bucle con algunas condiciones para detener (como “maximum execution speed is not changing in three seconds”) y medir algunas métricas sobre el rendimiento de las consultas (como “maximum execution speed”). Algunas pruebas pueden contener condiciones previas en el conjunto de datos de pruebas precargado.

Si desea mejorar el rendimiento de ClickHouse en algún escenario, y si se pueden observar mejoras en consultas simples, se recomienda encarecidamente escribir una prueba de rendimiento. Siempre tiene sentido usar `perf top` u otras herramientas de perf durante sus pruebas.

## Herramientas de prueba y secuencias de comandos {#test-tools-and-scripts}

Algunos programas en `tests` directorio no son pruebas preparadas, pero son herramientas de prueba. Por ejemplo, para `Lexer` hay una herramienta `src/Parsers/tests/lexer` que solo hacen la tokenización de stdin y escriben el resultado coloreado en stdout. Puede usar este tipo de herramientas como ejemplos de código y para exploración y pruebas manuales.

También puede colocar un par de archivos `.sh` y `.reference` junto con la herramienta para ejecutarlo en alguna entrada predefinida, entonces el resultado del script se puede comparar con `.reference` file. Este tipo de pruebas no están automatizadas.

## Pruebas diversas {#miscellaneous-tests}

Hay pruebas para diccionarios externos ubicados en `tests/external_dictionaries` y para modelos aprendidos a máquina en `tests/external_models`. Estas pruebas no se actualizan y deben transferirse a pruebas de integración.

Hay una prueba separada para inserciones de quórum. Esta prueba ejecuta el clúster ClickHouse en servidores separados y emula varios casos de fallas: división de red, caída de paquetes (entre nodos ClickHouse, entre ClickHouse y ZooKeeper, entre el servidor ClickHouse y el cliente, etc.), `kill -9`, `kill -STOP` y `kill -CONT` , como [Jepsen](https://aphyr.com/tags/Jepsen). A continuación, la prueba comprueba que todas las inserciones reconocidas se escribieron y todas las inserciones rechazadas no.

La prueba de quórum fue escrita por un equipo separado antes de que ClickHouse fuera de código abierto. Este equipo ya no trabaja con ClickHouse. La prueba fue escrita accidentalmente en Java. Por estas razones, la prueba de quórum debe reescribirse y trasladarse a pruebas de integración.

## Pruebas manuales {#manual-testing}

Cuando desarrolla una nueva característica, es razonable probarla también manualmente. Puede hacerlo con los siguientes pasos:

Construir ClickHouse. Ejecute ClickHouse desde el terminal: cambie el directorio a `programs/clickhouse-server` y ejecutarlo con `./clickhouse-server`. Se utilizará la configuración (`config.xml`, `users.xml` y archivos dentro de `config.d` y `users.d` directorios) desde el directorio actual de forma predeterminada. Para conectarse al servidor ClickHouse, ejecute `programs/clickhouse-client/clickhouse-client`.

Tenga en cuenta que todas las herramientas de clickhouse (servidor, cliente, etc.) son solo enlaces simbólicos a un único binario llamado `clickhouse`. Puede encontrar este binario en `programs/clickhouse`. Todas las herramientas también se pueden invocar como `clickhouse tool` en lugar de `clickhouse-tool`.

Alternativamente, puede instalar el paquete ClickHouse: ya sea una versión estable del repositorio de Yandex o puede crear un paquete para usted con `./release` en la raíz de fuentes de ClickHouse. Luego inicie el servidor con `sudo service clickhouse-server start` (o detener para detener el servidor). Busque registros en `/etc/clickhouse-server/clickhouse-server.log`.

Cuando ClickHouse ya está instalado en su sistema, puede crear un nuevo `clickhouse` binario y reemplazar el binario existente:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

También puede detener el servidor de clickhouse del sistema y ejecutar el suyo propio con la misma configuración pero con el registro en la terminal:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Ejemplo con gdb:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Si el servidor de clickhouse del sistema ya se está ejecutando y no desea detenerlo, puede cambiar los números de `config.xml` (o anularlos en un archivo en `config.d` directorio), proporcione la ruta de datos adecuada y ejecútela.

`clickhouse` binary casi no tiene dependencias y funciona en una amplia gama de distribuciones de Linux. Para probar rápidamente y sucio sus cambios en un servidor, simplemente puede `scp` su fresco construido `clickhouse` binario a su servidor y luego ejecútelo como en los ejemplos anteriores.

## Entorno de prueba {#testing-environment}

Antes de publicar la versión como estable, la implementamos en el entorno de prueba. El entorno de prueba es un clúster que procesa 1/39 parte de [El Yandex.Métrica](https://metrica.yandex.com/) datos. Compartimos nuestro entorno de pruebas con Yandex.Equipo de Metrica. ClickHouse se actualiza sin tiempo de inactividad sobre los datos existentes. Nos fijamos en un primer momento que los datos se procesan con éxito sin retraso de tiempo real, la replicación continúan trabajando y no hay problemas visibles para Yandex.Equipo de Metrica. La primera comprobación se puede hacer de la siguiente manera:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

En algunos casos también implementamos en el entorno de prueba de nuestros equipos de amigos en Yandex: Market, Cloud, etc. También tenemos algunos servidores de hardware que se utilizan con fines de desarrollo.

## Pruebas de carga {#load-testing}

Después de implementar en el entorno de prueba, ejecutamos pruebas de carga con consultas del clúster de producción. Esto se hace manualmente.

Asegúrese de que ha habilitado `query_log` en su clúster de producción.

Recopilar el registro de consultas para un día o más:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

Este es un ejemplo complicado. `type = 2` filtrará las consultas que se ejecutan correctamente. `query LIKE '%ym:%'` es seleccionar consultas relevantes de Yandex.Métrica. `is_initial_query` es seleccionar solo las consultas iniciadas por el cliente, no por ClickHouse (como partes del procesamiento de consultas distribuidas).

`scp` este registro en su clúster de prueba y ejecútelo de la siguiente manera:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

(probablemente también desee especificar un `--user`)

Luego déjalo por una noche o un fin de semana e ir a tomar un descanso.

Usted debe comprobar que `clickhouse-server` no se bloquea, la huella de memoria está limitada y el rendimiento no se degrada con el tiempo.

Los tiempos de ejecución de consultas precisos no se registran y no se comparan debido a la alta variabilidad de las consultas y el entorno.

## Pruebas de construcción {#build-tests}

Las pruebas de compilación permiten verificar que la compilación no esté rota en varias configuraciones alternativas y en algunos sistemas extranjeros. Las pruebas se encuentran en `ci` directorio. Ejecutan compilación desde la fuente dentro de Docker, Vagrant y, a veces, con `qemu-user-static` dentro de Docker. Estas pruebas están en desarrollo y las ejecuciones de pruebas no están automatizadas.

Motivación:

Normalmente lanzamos y ejecutamos todas las pruebas en una sola variante de compilación ClickHouse. Pero hay variantes de construcción alternativas que no se prueban a fondo. Ejemplos:

-   construir en FreeBSD;
-   construir en Debian con bibliotecas de paquetes del sistema;
-   construir con enlaces compartidos de bibliotecas;
-   construir en la plataforma AArch64;
-   construir en la plataforma PowerPc.

Por ejemplo, construir con paquetes del sistema es una mala práctica, porque no podemos garantizar qué versión exacta de paquetes tendrá un sistema. Pero esto es realmente necesario para los mantenedores de Debian. Por esta razón, al menos tenemos que admitir esta variante de construcción. Otro ejemplo: la vinculación compartida es una fuente común de problemas, pero es necesaria para algunos entusiastas.

Aunque no podemos ejecutar todas las pruebas en todas las variantes de compilaciones, queremos verificar al menos que varias variantes de compilación no estén rotas. Para este propósito utilizamos pruebas de construcción.

## Pruebas de Compatibilidad de protocolos {#testing-for-protocol-compatibility}

Cuando ampliamos el protocolo de red ClickHouse, probamos manualmente que el antiguo clickhouse-client funciona con el nuevo clickhouse-server y el nuevo clickhouse-client funciona con el antiguo clickhouse-server (simplemente ejecutando binarios de los paquetes correspondientes).

## Ayuda del compilador {#help-from-the-compiler}

Código principal de ClickHouse (que se encuentra en `dbms` directorio) se construye con `-Wall -Wextra -Werror` y con algunas advertencias habilitadas adicionales. Aunque estas opciones no están habilitadas para bibliotecas de terceros.

Clang tiene advertencias aún más útiles: puedes buscarlas con `-Weverything` y elige algo para la compilación predeterminada.

Para las compilaciones de producción, se usa gcc (todavía genera un código ligeramente más eficiente que clang). Para el desarrollo, el clang suele ser más conveniente de usar. Puede construir en su propia máquina con el modo de depuración (para ahorrar batería de su computadora portátil), pero tenga en cuenta que el compilador puede generar más advertencias con `-O3` debido a un mejor flujo de control y análisis entre procedimientos. Al construir con clang, `libc++` se utiliza en lugar de `libstdc++` y al construir con el modo de depuración, la versión de depuración de `libc++` se utiliza que permite detectar más errores en tiempo de ejecución.

## Desinfectantes {#sanitizers}

**Dirección desinfectante**.
Ejecutamos pruebas funcionales y de integración bajo ASan por compromiso.

**Valgrind (Memcheck)**.
Realizamos pruebas funcionales bajo Valgrind durante la noche. Se tarda varias horas. Actualmente hay un falso positivo conocido en `re2` biblioteca, ver [este artículo](https://research.swtch.com/sparse).

**Desinfectante de comportamiento indefinido.**
Ejecutamos pruebas funcionales y de integración bajo ASan por compromiso.

**Desinfectante de hilo**.
Ejecutamos pruebas funcionales bajo TSan por compromiso. Todavía no ejecutamos pruebas de integración bajo TSan por compromiso.

**Desinfectante de memoria**.
Actualmente todavía no usamos MSan.

**Asignador de depuración.**
Versión de depuración de `jemalloc` se utiliza para la compilación de depuración.

## Fuzzing {#fuzzing}

ClickHouse fuzzing se implementa tanto usando [LibFuzzer](https://llvm.org/docs/LibFuzzer.html) y consultas SQL aleatorias.
Todas las pruebas de fuzz deben realizarse con desinfectantes (Dirección y Undefined).

LibFuzzer se usa para pruebas de fuzz aisladas del código de la biblioteca. Fuzzers se implementan como parte del código de prueba y tienen “\_fuzzer” nombre postfixes.
El ejemplo de Fuzzer se puede encontrar en `src/Parsers/tests/lexer_fuzzer.cpp`. Las configuraciones, diccionarios y corpus específicos de LibFuzzer se almacenan en `tests/fuzz`.
Le recomendamos que escriba pruebas fuzz para cada funcionalidad que maneje la entrada del usuario.

Fuzzers no se construyen de forma predeterminada. Para construir fuzzers ambos `-DENABLE_FUZZING=1` y `-DENABLE_TESTS=1` se deben establecer opciones.
Recomendamos deshabilitar Jemalloc mientras se construyen fuzzers. Configuración utilizada para integrar
Google OSS-Fuzz se puede encontrar en `docker/fuzz`.

También usamos una prueba de fuzz simple para generar consultas SQL aleatorias y verificar que el servidor no muera al ejecutarlas.
Lo puedes encontrar en `00746_sql_fuzzy.pl`. Esta prueba debe ejecutarse de forma continua (de la noche a la mañana y más).

## Auditoría de seguridad {#security-audit}

La gente de Yandex Security Team hace una visión general básica de las capacidades de ClickHouse desde el punto de vista de la seguridad.

## Analizadores estáticos {#static-analyzers}

Corremos `PVS-Studio` por compromiso. Hemos evaluado `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`. Encontrará instrucciones de uso en `tests/instructions/` directorio. También puedes leer [el artículo en ruso](https://habr.com/company/yandex/blog/342018/).

Si usted usa `CLion` como IDE, puede aprovechar algunos `clang-tidy` comprueba fuera de la caja.

## Endurecer {#hardening}

`FORTIFY_SOURCE` se utiliza de forma predeterminada. Es casi inútil, pero todavía tiene sentido en casos raros y no lo desactivamos.

## Estilo de código {#code-style}

Se describen las reglas de estilo de código [aqui](https://clickhouse.tech/docs/en/development/style/).

Para comprobar si hay algunas violaciones de estilo comunes, puede usar `utils/check-style` script.

Para forzar el estilo adecuado de su código, puede usar `clang-format`. File `.clang-format` se encuentra en la raíz de las fuentes. Se corresponde principalmente con nuestro estilo de código real. Pero no se recomienda aplicar `clang-format` a los archivos existentes porque empeora el formato. Usted puede utilizar `clang-format-diff` herramienta que puede encontrar en el repositorio de origen clang.

Alternativamente, puede intentar `uncrustify` herramienta para reformatear su código. La configuración está en `uncrustify.cfg` en la raíz de las fuentes. Es menos probado que `clang-format`.

`CLion` tiene su propio formateador de código que debe ajustarse para nuestro estilo de código.

## Pruebas Metrica B2B {#metrica-b2b-tests}

Cada lanzamiento de ClickHouse se prueba con los motores Yandex Metrica y AppMetrica. Las pruebas y las versiones estables de ClickHouse se implementan en máquinas virtuales y se ejecutan con una copia pequeña del motor Metrica que procesa una muestra fija de datos de entrada. A continuación, los resultados de dos instancias del motor Metrica se comparan juntos.

Estas pruebas son automatizadas por un equipo separado. Debido a la gran cantidad de piezas móviles, las pruebas fallan la mayor parte del tiempo por razones completamente no relacionadas, que son muy difíciles de descubrir. Lo más probable es que estas pruebas tengan un valor negativo para nosotros. Sin embargo, se demostró que estas pruebas son útiles en aproximadamente una o dos veces de cada cientos.

## Cobertura de prueba {#test-coverage}

A partir de julio de 2018, no realizamos un seguimiento de la cobertura de las pruebas.

## Automatización de pruebas {#test-automation}

Realizamos pruebas con el CI interno de Yandex y el sistema de automatización de trabajos llamado “Sandbox”.

Los trabajos de compilación y las pruebas se ejecutan en Sandbox por confirmación. Los paquetes resultantes y los resultados de las pruebas se publican en GitHub y se pueden descargar mediante enlaces directos. Los artefactos se almacenan eternamente. Cuando envías una solicitud de extracción en GitHub, la etiquetamos como “can be tested” y nuestro sistema CI construirá paquetes ClickHouse (liberación, depuración, con desinfectante de direcciones, etc.) para usted.

No usamos Travis CI debido al límite de tiempo y potencia computacional.
No usamos Jenkins. Se usó antes y ahora estamos felices de no estar usando Jenkins.

[Artículo Original](https://clickhouse.tech/docs/en/development/tests/) <!--hide-->
