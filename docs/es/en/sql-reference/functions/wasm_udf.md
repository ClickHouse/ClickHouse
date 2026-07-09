---
description: 'Documentación sobre funciones definidas por el usuario de WebAssembly'
sidebar_label: 'UDFs de WebAssembly'
slug: /sql-reference/functions/wasm_udf
title: 'Funciones definidas por el usuario de WebAssembly'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # Funciones definidas por el usuario en WebAssembly
</div>

ClickHouse admite la creación de funciones definidas por el usuario (UDFs) escritas en WebAssembly. Esto permite ejecutar lógica personalizada escrita en lenguajes como Rust, C, C++ u otros, al compilarlos en módulos de WebAssembly.

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## Descripción general
</div>

Un módulo de WebAssembly es un archivo binario compilado que contiene una o más funciones a las que se puede llamar desde ClickHouse.
Piense en un módulo como una biblioteca o un objeto compartido que se carga una vez y se reutiliza muchas veces.

Un módulo de WebAssembly que contiene UDFs puede escribirse en cualquier lenguaje que pueda compilarse a WebAssembly, como Rust, C o C++.

El código compilado a WebAssembly (código &quot;guest&quot;) y ejecutado por ClickHouse (&quot;host&quot;) se ejecuta en un entorno aislado con acceso únicamente a un espacio de memoria dedicado.

El código guest exporta funciones que ClickHouse puede invocar; entre ellas se incluyen las funciones que implementan su lógica personalizada (utilizadas para definir UDFs), así como las funciones de soporte necesarias para la gestión de memoria y el intercambio de datos entre ClickHouse y el código WebAssembly.

Su código debe compilarse a WebAssembly &quot;freestanding&quot; (también conocido como `wasm32-unknown-unknown`) sin dependencias de un sistema operativo ni de una biblioteca estándar. Además, solo se admite el destino predeterminado de WebAssembly de 32 bits (sin la extensión `wasm64`).
El módulo debe seguir uno de los protocolos de comunicación (ABI) compatibles para interactuar con ClickHouse.

Una vez compilado, el código binario del módulo se carga en ClickHouse insertándolo en la tabla `system.webassembly_modules`.
Después, puede crear UDFs que hagan referencia a funciones exportadas por el módulo mediante la sentencia `CREATE FUNCTION ... LANGUAGE WASM`.

<div id="prerequisites">
  ## Requisitos previos
</div>

Habilite la compatibilidad con WebAssembly en la configuración de ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

Implementaciones de motores disponibles:

* `wasmtime` (por defecto, recomendado) — usa [WasmTime](https://github.com/bytecodealliance/wasmtime)
* `wasmedge` — usa [WasmEdge](https://github.com/WasmEdge/WasmEdge)

<div id="quick-start">
  ## Inicio rápido
</div>

Este ejemplo muestra el flujo de trabajo completo para crear una WebAssembly UDF implementando una calculadora de la [conjetura de Collatz](https://en.wikipedia.org/wiki/Collatz_conjecture).

Escribiremos el código en el formato de texto de WebAssembly (WAT), que es una representación legible para humanos de WebAssembly, por lo que en esta etapa no se necesita ningún lenguaje de programación.
ClickHouse requiere que el módulo esté en formato binario, así que usaremos un transpilador para convertir WAT en WASM.
Para realizar esta conversión, puede usar `wat2wasm` de [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) o el comando `parse` de [wasm-tools](https://github.com/bytecodealliance/wasm-tools).

```bash
cat << 'EOF' | wasm-tools parse | clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
(module
  (func $next (param $n i32) (result i32)
    local.get $n i32.const 1 i32.and
    (if (result i32)
      (then local.get $n i32.const 3 i32.mul i32.const 1 i32.add)
      (else local.get $n i32.const 2 i32.div_u)))
  (func $steps (export "steps") (param $n i32) (result i32)
    (local $count i32)
    local.get $n i32.const 1 i32.lt_u
    (if (then i32.const 0 return))
    (block $done (loop $loop
      local.get $n i32.const 1 i32.eq br_if $done
      local.get $n call $next local.set $n
      local.get $count i32.const 1 i32.add local.set $count
      br $loop))
    local.get $count)
)
EOF
```

En el fragmento anterior, enviamos el código binario WASM directamente al client de ClickHouse mediante una tubería usando `FORMAT RawBlob` para insertarlo en la tabla `system.webassembly_modules`.

Luego definimos la UDF que hace referencia a la función `steps` exportada por el módulo:

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

Ten en cuenta que especificamos el nombre de la función del módulo después de `::`, ya que difiere del nombre de la UDF.

Ahora podemos usar la función `collatz_steps` en nuestras consultas:

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

La columna `number` se convierte explícitamente a `UInt32`, porque las funciones de WebAssembly esperan una correspondencia exacta de tipos con la firma especificada en la sentencia `CREATE FUNCTION`.

En el resultado obtuvimos la secuencia de pasos de Collatz para los números del 1 al 100, correspondiente a la secuencia [A006577 de la OEIS](https://oeis.org/A006577).

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## Administrar módulos WASM mediante una tabla del sistema
</div>

Los módulos de WebAssembly se almacenan en la tabla `system.webassembly_modules`, que tiene la siguiente estructura:

* **Columnas**
  * `name` String — Nombre del módulo. No puede estar vacío; solo admite caracteres de palabra.
  * `code` String — Código binario WASM sin procesar. Solo escritura; las lecturas devuelven una cadena vacía.
  * `hash` UInt256 — SHA256 del binario del módulo (cero si está presente en disco, pero todavía no se ha cargado).

La administración de módulos se realiza mediante operaciones SQL estándar sobre esta tabla:

<div id="insert-a-module">
  ### Insertar un módulo
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Opcionalmente, proporcione el hash de integridad:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

Si el hash proporcionado no coincide con el SHA256 calculado del código del módulo, la inserción falla. Puede resultar útil al cargar módulos desde orígenes externos, como S3 o HTTP.

<div id="distribute-a-module-across-a-cluster">
  ### Distribuir un módulo en un clúster
</div>

`system.webassembly_modules` es una tabla por instancia: un `INSERT` solo llega a la réplica que gestiona la conexión. No existe una forma `ON CLUSTER` de la sentencia `INSERT`, por lo que un `CREATE FUNCTION ... ON CLUSTER` posterior fallará en las réplicas que no tengan el módulo:

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

Para propagar una operación de `insert` a todos los nodos, escriba en la función de tabla `cluster` en lugar de en la tabla local `system.webassembly_modules`:

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
Este patrón depende de que la ruta subyacente de escritura distribuida pase por cada réplica de cada segmento, algo que solo ocurre cuando el clúster está configurado con `internal_replication=false`. Con `internal_replication=true` (el valor predeterminado en los clústeres que usan `ReplicatedMergeTree` para encargarse ellos mismos de la replicación), el insert se envía a una sola réplica en buen estado por segmento, y `system.webassembly_modules` no se replica por esa ruta, por lo que algunas réplicas seguirán sin tener el módulo. En esa configuración, debe hacer el insert en cada réplica por separado, por ejemplo, iterando sobre `system.clusters` y escribiendo mediante `remote(...)` para cada host, o copiando el binario en `user_scripts/wasm/` en cada host.

Puede consultar `internal_replication` de un clúster con `SELECT cluster, shard_num, internal_replication FROM system.clusters`.
:::

Después del insert distribuido, el módulo está presente en cada réplica y `CREATE FUNCTION ... ON CLUSTER` se ejecuta correctamente:

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

Puede comprobar que el módulo está cargado en todos los nodos con `clusterAllReplicas`:

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

Las inserciones en `system.webassembly_modules` son idempotentes para el mismo par `(name, hash)`, por lo que volver a ejecutar la inserción distribuida es seguro y es una forma razonable de reparar el estado después de reemplazar una réplica. Tenga en cuenta que los servidores recién añadidos no reciben de forma retroactiva los módulos existentes: debe volver a ejecutar la inserción en el clúster actualizado o colocar el binario en el directorio `user_scripts/wasm/` del nuevo servidor.

<div id="list-modules">
  ### Consultar los módulos
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### Eliminar un módulo
</div>

La eliminación se realiza mediante la sentencia `DELETE FROM system.webassembly_modules WHERE name = '...'`.
El predicado debe ser `name = 'literal'` para una coincidencia exacta o `name LIKE 'pattern'` para eliminar todos los módulos cuyo nombre coincida con el patrón; no se admite ninguna otra forma.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

Si alguna UDF existente hace referencia a alguno de los módulos coincidentes, la eliminación fallará, por lo que primero debes eliminar esas UDF.

<div id="create-a-webassembly-udf">
  ## Crear una UDF de WebAssembly
</div>

**Sintaxis**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1 | ABI ASSEMBLYSCRIPT]
[DETERMINISTIC]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**Parámetros**:

* `function_name`: Nombre de la función en ClickHouse. Puede ser distinto del nombre de la función exportada en el módulo.
* `FROM 'module_name' :: 'source_function_name'`: Nombre del módulo WASM cargado y nombre de la función del módulo WASM que se utilizará (por defecto, function&#95;name)
* `ARGUMENTS`: Lista de nombres y tipos de argumentos (los nombres son opcionales y se usan en formatos de serialización que admiten campos con nombre)
* `ABI`: Versión de la interfaz binaria de aplicación
  * `ROW_DIRECT`: correspondencia directa de tipos, procesamiento fila por fila
  * `BUFFERED_V1`: Procesamiento basado en bloques con serialización
  * `ASSEMBLYSCRIPT`: Procesamiento fila por fila para módulos generados por el compilador [AssemblyScript](https://www.assemblyscript.org). Los tipos numéricos se asignan a los primitivos de AssemblyScript; `String` de ClickHouse se asigna a `string` de AssemblyScript.
* `DETERMINISTIC`: Declara la función como determinista: siempre devuelve el mismo resultado para la misma entrada. Cuando se especifica, ClickHouse puede precomputar las llamadas en las que todos los argumentos son constantes: la función se evalúa una vez durante el análisis de la consulta y el resultado se reutiliza para cada fila.
* `SHA256_HASH`: Hash esperado del módulo para su verificación (se completa automáticamente si se omite); puede usarse para garantizar que se cargue el módulo WASM correcto en distintas réplicas.
* `SETTINGS`: Configuración por función
  * `serialization_format` String — Formato de serialización si el ABI lo requiere. Valores admitidos: `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary` y `Buffers`. Valor predeterminado: `MsgPack`. Los formatos basados en bloques, como `Buffers`, deben devolver una única columna cuyo tipo coincida con la firma declarada de la función.
  * `webassembly_udf_enable_fuel` Bool — Habilita un presupuesto finito de fuel para la función. Valor predeterminado: `true`. Cuando es `false`, la configuración a nivel de consulta `webassembly_udf_max_fuel` se ignora para esta función. Deshabilitar los límites de fuel puede mejorar el rendimiento al usar el motor `wasmtime`. Sin embargo, en el caso de código del módulo no confiable o con errores, puede aumentar el riesgo de ejecución descontrolada.

<div id="abis-versions">
  ## Versiones de las ABI
</div>

Para interactuar con ClickHouse, los módulos de WebAssembly deben ajustarse a una de las ABI compatibles (interfaces binarias de aplicación).

* `ROW_DIRECT`: correspondencia directa de tipos (solo tipos primitivos `Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, `Float64`)
* `BUFFERED_V1`: tipos complejos con serialización
* `ASSEMBLYSCRIPT`: interoperabilidad fila por fila con módulos de [AssemblyScript](https://www.assemblyscript.org); admite tipos numéricos y `String`.

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

Llama directamente a una función WASM exportada para cada fila.

* Argumentos y tipos de retorno de tipo numérico `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`.
* Las cadenas no son compatibles con esta ABI.
* Las firmas deben coincidir con la exportación de WASM (`i32/i64/f32/f64/v128`).
* El módulo no necesita exportar funciones de soporte.

Por ejemplo, una función con la firma:

```
(func (param i32 i64 f32) (result f64) ...)
```

Se puede crear así:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly no distingue entre argumentos con signo y sin signo, sino que utiliza instrucciones distintas para interpretar los valores. Por tanto, el tamaño del argumento debe coincidir exactamente, mientras que el signo lo determinan las operaciones dentro de la función.

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
Esta ABI es experimental y puede cambiar en futuras versiones.
:::

Procesa bloques completos de una sola vez mediante (des)serialización a través de la memoria de WASM. Admite cualquier tipo de argumento y de retorno.

Los datos serializados se copian en la memoria de WASM, que se pasa a la función UDF como un puntero a un búfer (que consta de un puntero a los datos y el tamaño de estos), junto con el número de filas de entrada. Por lo tanto, la función definida por el usuario del lado de WASM siempre acepta dos argumentos `i32` y devuelve un único valor `i32`.
El código guest procesa los datos y devuelve un puntero al búfer de resultados con los datos del resultado serializados.

El código guest debe proporcionar dos funciones para crear y destruir estos búferes.

```
(module
  ;; Allocate a new buffer of specified size
  ;; Returns: handle to Buffer structure (not direct data pointer!) with pointer to data and size
  (func (export "clickhouse_create_buffer")
    (param $size i32)    ;; Size of data to allocate
    (result i32))        ;; Returns buffer handle with enough space

  ;; Free a buffer by its handle
  (func (export "clickhouse_destroy_buffer")
    (param $handle i32)  ;; Buffer handle to free
    (result))            ;; No return value

    ;; User-defined function
    (func (export "user_defined_function1")
      (param $input_buffer_handle i32)  ;; Input buffer handle
      (param $n i32)                    ;; Number of rows in input
      (result i32))                     ;; Returns output buffer handle
)
```

Definiciones de ejemplo en C:

```c
typedef struct {
    uint8_t * data;
    uint32_t size;
} ClickhouseBuffer;

ClickhouseBuffer * clickhouse_create_buffer(uint32_t size) { /* ... */ }

void clickhouse_destroy_buffer(ClickhouseBuffer * data) { /* ... */ }

/// Example user-defined functions
ClickhouseBuffer * user_defined_function1(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
ClickhouseBuffer * user_defined_function2(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
```

<div id="abi-assemblyscript">
  ### ABI ASSEMBLYSCRIPT
</div>

Se aplica a los módulos generados por el compilador [AssemblyScript](https://www.assemblyscript.org). Cada fila provoca una llamada a la función exportada, asignando los valores de ClickHouse a primitivos y objetos de cadena de AssemblyScript.

**Tipos compatibles**:

* Numéricos: `Int8`/`UInt8`, `Int16`/`UInt16` (convertidos a `i32` al cruzar el límite), `Int32`/`UInt32`, `Int64`/`UInt64`, `Float32`, `Float64`

* `String` — se asigna a `string` de AssemblyScript (UTF-16 en la memoria WASM). ClickHouse gestiona automáticamente la conversión UTF-8 ↔ UTF-16.

* Las clases personalizadas de AssemblyScript no son compatibles como tipos de argumento o de retorno; sus identificadores de clase en tiempo de ejecución no son estables entre compilaciones (consulta [AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982)).

**Requisitos del módulo**:

El módulo debe compilarse con el runtime administrado de AssemblyScript para que se exporten `__new`, `__pin` y `__unpin`. El manejo estándar de cadenas de entrada y salida depende de ello. La invocación recomendada:

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript también importa `env.abort` para interrupciones del runtime (falta de memoria, comprobaciones de límites, etc.). ClickHouse proporciona esta importación automáticamente: cuando se desencadena un `abort`, la consulta activa falla con una excepción `WASM_ERROR` que incluye el mensaje decodificado de AssemblyScript y la ubicación en el código fuente.

**Ejemplo**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

Después de compilar con `asc` y cargar el archivo `.wasm` resultante en `system.webassembly_modules`, declare los UDFs de la siguiente manera:

```sql
CREATE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;
```

<div id="note-for-developing-udfs-in-rust">
  ### Nota para desarrollar UDFs en Rust
</div>

Para los programas en Rust, proporcionamos un crate auxiliar [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) para simplificar el desarrollo de UDFs de WebAssembly para ClickHouse. El crate proporciona funciones para la gestión de memoria, por lo que no es necesario implementar manualmente las funciones `clickhouse_create_buffer` y `clickhouse_destroy_buffer`; basta con añadir el crate como dependencia. También incluye macros `#[clickhouse_wasm_udf]` para ajustar las funciones habituales de Rust al formato ABI requerido.

Con este crate puedes escribir UDFs así:

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

Las macros generarán funciones envoltorio que aceptarán y devolverán estructuras de búfer, y gestionarán automáticamente la serialización/deserialización mediante `serde`.

<div id="host-api-available-to-modules">
  ## API del host disponible para los módulos
</div>

Las siguientes funciones del host pueden importarse y utilizarse en los módulos:

* `clickhouse_server_version() -> i64` — devuelve la versión de ClickHouse server como un entero (p. ej., 25011001 para v25.11.1.1).
* `clickhouse_throw(ptr: i32, size: i32)` — genera un error con el mensaje proporcionado. Acepta un puntero a la ubicación de memoria que contiene la cadena del mensaje de error y el tamaño de la cadena.
* `clickhouse_log(ptr: i32, size: i32)` — registra un mensaje en el log de texto de ClickHouse server.
* `clickhouse_random(ptr: i32, size: i32)` — rellena la memoria con bytes aleatorios.
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — se proporciona para módulos compatibles con AssemblyScript. Llamarla (o activar un trap del entorno de ejecución de AssemblyScript que la llame) finaliza la UDF con una excepción `WASM_ERROR` que contiene el mensaje decodificado y la ubicación del código fuente. Los módulos que no importan `env.abort` no se ven afectados.

<div id="settings">
  ## Configuración
</div>

Las siguientes opciones de configuración a nivel de consulta controlan la ejecución de UDF de WebAssembly:

* `webassembly_udf_max_fuel` — Límite de fuel por ejecución de una instancia de UDF de WebAssembly. Cada instrucción de WebAssembly consume cierta cantidad de fuel. El valor se escala por 1024 antes de pasarse al runtime, por lo que `webassembly_udf_max_fuel = 1` corresponde a aproximadamente 1024 unidades de fuel. Establézcalo en 0 para no tener ningún límite finito. Se aplica solo a las funciones cuya configuración por función `webassembly_udf_enable_fuel` sea true, que es el valor predeterminado.

* `webassembly_udf_max_memory` — Límite de memoria en bytes por instancia de UDF de WebAssembly.

* `webassembly_udf_max_input_block_size` — Número máximo de filas que se pasan a una UDF de WebAssembly en un solo bloque. Establézcalo en 0 para procesar todas las filas de una sola vez.

* `webassembly_udf_max_instances` — Número máximo de instancias de UDF de WebAssembly que pueden ejecutarse en paralelo por función.

Ejemplo de uso:

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## Véase también
</div>

* [Descripción general de las UDF de ClickHouse](/es/sql-reference/functions/udf)