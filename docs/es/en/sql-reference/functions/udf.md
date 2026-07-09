---
description: 'Documentación de las funciones definidas por el usuario (UDFs)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'Funciones definidas por el usuario (UDFs)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # UDFs Funciones definidas por el usuario
</div>

ClickHouse admite varios tipos de funciones definidas por el usuario (UDFs):

* [UDFs ejecutables](#executable-user-defined-functions) inician un programa o script externo (Python, Bash, etc.) y transmiten bloques de datos a través de STDIN / STDOUT. Úselas para integrar código o herramientas existentes sin recompilar ClickHouse. Tienen una mayor sobrecarga por llamada que las opciones en el proceso y son más adecuadas para lógica más pesada o cuando se requiere un entorno de ejecución distinto.
* [UDFs SQL](#sql-user-defined-functions) se definen con `CREATE FUNCTION` exclusivamente en SQL. Se insertan o expanden en el plan de consulta (sin cambiar de proceso), lo que las hace ligeras e ideales para reutilizar lógica de expresiones o simplificar columnas calculadas complejas.
* [UDFs experimentales de WebAssembly](#webassembly-user-defined-functions) ejecutan código compilado a WebAssembly dentro de un entorno aislado en el proceso del servidor. Ofrecen menor sobrecarga por llamada que los ejecutables externos y mejor aislamiento que las extensiones nativas, por lo que son adecuadas para algoritmos personalizados escritos en lenguajes que pueden compilarse a WASM (p. ej., C/C++/Rust).
* [UDFs ejecutables experimentales basadas en driver](#driver-based-executable-user-defined-functions) permiten que un &quot;driver&quot; proporcionado por el operador convierta un fragmento de código suministrado en `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'` en una UDF ejecutable en el momento de crear la función (por ejemplo, compilándolo). Se basan en las UDFs ejecutables y requieren configuración del driver del lado del servidor.

<div id="executable-user-defined-functions">
  ## UDF ejecutables
</div>

<BetaBadge />

:::note
En ClickHouse Cloud, las UDF ejecutables están en beta pública y se crean desde la UI de la consola de Cloud. Consulta [Funciones definidas por el usuario en Cloud](/es/cloud/features/user-defined-functions) para ver el flujo de trabajo específico de Cloud.
:::

ClickHouse puede llamar a cualquier programa ejecutable externo o script para procesar datos.

La configuración de las funciones ejecutables definidas por el usuario puede estar en uno o varios archivos XML.
La ruta de acceso a la configuración se especifica en el parámetro [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config).

La configuración de una función contiene los siguientes ajustes:

| Parámetro                     | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Requerido | Valor predeterminado      |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------------------------- |
| `name`                        | Nombre de la función                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Sí        | -                         |
| `command`                     | Nombre del script que se va a ejecutar o comando si `execute_direct` es false                                                                                                                                                                                                                                                                                                                                                                                                             | Sí        | -                         |
| `argument`                    | Descripción del argumento con su `type` y, opcionalmente, su `name`. Cada argumento se describe en una configuración independiente. Es necesario especificar el nombre si los nombres de los argumentos forman parte de la serialización en formatos de funciones definidas por el usuario, como [Native](/es/interfaces/formats/Native) o [JSONEachRow](/es/interfaces/formats/JSONEachRow)                                                                                                    | Sí        | `c` + argument&#95;number |
| `format`                      | [Formato](../../interfaces/formats.md) en el que se pasan los argumentos al comando. Se espera que la salida del comando también use el mismo formato                                                                                                                                                                                                                                                                                                                                     | Sí        | -                         |
| `return_type`                 | Tipo del valor devuelto                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Sí        | -                         |
| `return_name`                 | Nombre del valor devuelto. Es necesario especificarlo si forma parte de la serialización en formatos de funciones definidas por el usuario, como [Native](/es/interfaces/formats/Native) o [JSONEachRow](/es/interfaces/formats/JSONEachRow)                                                                                                                                                                                                                                                    | Opcional  | `result`                  |
| `type`                        | Tipo de ejecutable. Si `type` se establece en `executable`, se inicia un único comando. Si se establece en `executable_pool`, se crea un grupo de comandos                                                                                                                                                                                                                                                                                                                                | Sí        | -                         |
| `max_command_execution_time`  | Tiempo máximo de ejecución, en segundos, para procesar un bloque de datos. Esta configuración solo es válida para comandos `executable_pool`                                                                                                                                                                                                                                                                                                                                              | Opcional  | `10`                      |
| `command_termination_timeout` | Tiempo, en segundos, durante el cual un comando debe finalizar después de que se cierre su tubería. Transcurrido ese tiempo, se envía `SIGTERM` al proceso que ejecuta el comando                                                                                                                                                                                                                                                                                                         | Opcional  | `10`                      |
| `command_read_timeout`        | Tiempo de espera para leer datos de stdout del comando, en milisegundos                                                                                                                                                                                                                                                                                                                                                                                                                   | Opcional  | `10000`                   |
| `command_write_timeout`       | Tiempo de espera para escribir datos en stdin del comando, en milisegundos                                                                                                                                                                                                                                                                                                                                                                                                                | Opcional  | `10000`                   |
| `pool_size`                   | Tamaño del grupo de comandos                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Opcional  | `16`                      |
| `send_chunk_header`           | Controla si se debe enviar el recuento de filas antes de enviar un fragmento de datos al proceso                                                                                                                                                                                                                                                                                                                                                                                          | Opcional  | `false`                   |
| `execute_direct`              | Si `execute_direct` = `1`, `command` se buscará dentro de la carpeta user&#95;scripts especificada por [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Se pueden especificar argumentos adicionales del script separados por espacios en blanco. Ejemplo: `script_name arg1 arg2`. Si `execute_direct` = `0`, `command` se pasa como argumento a `bin/sh -c`                                                                 | Opcional  | `1`                       |
| `lifetime`                    | Intervalo de recarga de una función, en segundos. Si se establece en `0`, la función no se recarga                                                                                                                                                                                                                                                                                                                                                                                        | Opcional  | `0`                       |
| `deterministic`               | Si la función es determinista (devuelve el mismo resultado para la misma entrada)                                                                                                                                                                                                                                                                                                                                                                                                         | Opcional  | `false`                   |
| `stderr_reaction`             | Cómo manejar la salida de stderr del comando. Valores: `none` (ignorar), `log` (registrar inmediatamente todo stderr), `log_first` (registrar los primeros 4 KiB después de la finalización), `log_last` (registrar los últimos 4 KiB después de la finalización), `throw` (lanzar una excepción inmediatamente ante cualquier salida en stderr). Al usar `log_first` o `log_last` con un código de salida distinto de cero, el contenido de stderr se incluye en el mensaje de excepción | Opcional  | `log_last`                |
| `check_exit_code`             | Si es `true`, ClickHouse comprobará el código de salida del comando. Un código de salida distinto de cero provoca una excepción                                                                                                                                                                                                                                                                                                                                                           | Opcional  | `true`                    |

El comando debe leer los argumentos desde `STDIN` y enviar el resultado a `STDOUT`. Debe procesar los argumentos de forma iterativa. Es decir, después de procesar un fragmento de argumentos, debe esperar al siguiente fragmento.

<div id="executable-user-defined-functions">
  ## UDF ejecutables
</div>

<div id="examples">
  ## Ejemplos
</div>

<div id="udf-inline">
  ### UDF a partir de un script inline
</div>

Cree `test_function_sum` manualmente y establezca `execute_direct` en `0` mediante una configuración XML o YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Archivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` con la ruta predeterminada).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum</name>
            <return_type>UInt64</return_type>
            <argument>
                <type>UInt64</type>
                <name>lhs</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>rhs</name>
            </argument>
            <format>TabSeparated</format>
            <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
            <execute_direct>0</execute_direct>
            <deterministic>true</deterministic>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Archivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` con la ruta predeterminada).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum
      return_type: UInt64
      argument:
        - type: UInt64
          name: lhs
        - type: UInt64
          name: rhs
      format: TabSeparated
      command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
      execute_direct: 0
      deterministic: true
    ```
  </TabItem>
</Tabs>

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### UDF a partir de un script de Python
</div>

En este ejemplo, creamos una UDF que lee un valor desde `STDIN` y lo devuelve como una cadena.

Cree `test_function` usando configuración XML o YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Archivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` con la ruta predeterminada).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_function.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Archivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` con la ruta predeterminada).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_python
      return_type: String
      argument:
        - type: UInt64
          name: value
      format: TabSeparated
      command: test_function.py
    ```
  </TabItem>
</Tabs>

<br />

Cree el archivo de script `test_function.py` dentro de la carpeta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function.py` con la ruta predeterminada).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

<div id="udf-stdin">
  ### Leer dos valores de `STDIN` y devolver su suma como un objeto JSON
</div>

Cree `test_function_sum_json` con argumentos nombrados y formato [JSONEachRow](/es/interfaces/formats/JSONEachRow) mediante una configuración XML o YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Archivo `test_function.xml` (`/etc/clickhouse-server/test_function.xml` con la configuración predeterminada de rutas).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum_json</name>
            <return_type>UInt64</return_type>
            <return_name>result_name</return_name>
            <argument>
                <type>UInt64</type>
                <name>argument_1</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>argument_2</name>
            </argument>
            <format>JSONEachRow</format>
            <command>test_function_sum_json.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Archivo `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` con la configuración predeterminada de rutas).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum_json
      return_type: UInt64
      return_name: result_name
      argument:
        - type: UInt64
          name: argument_1
        - type: UInt64
          name: argument_2
      format: JSONEachRow
      command: test_function_sum_json.py
    ```
  </TabItem>
</Tabs>

<br />

Cree el archivo del script `test_function_sum_json.py` dentro de la carpeta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` con la configuración predeterminada de rutas).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

<div id="udf-parameters-in-command">
  ### Usar parámetros en la opción `command`
</div>

Las funciones definidas por el usuario ejecutables pueden aceptar parámetros constantes configurados en la opción `command` (esto solo funciona para funciones definidas por el usuario de tipo `executable`).
También requiere la opción `execute_direct` para garantizar que no exista ninguna vulnerabilidad por expansión de argumentos del shell.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Archivo `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` con la ruta predeterminada).

    ```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
    <functions>
        <function>
            <type>executable</type>
            <execute_direct>true</execute_direct>
            <name>test_function_parameter_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
            </argument>
            <format>TabSeparated</format>
            <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Archivo `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` con la ruta predeterminada).

    ```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
    functions:
      type: executable
      execute_direct: true
      name: test_function_parameter_python
      return_type: String
      argument:
        - type: UInt64
      format: TabSeparated
      command: test_function_parameter_python.py {test_parameter:UInt64}
    ```
  </TabItem>
</Tabs>

<br />

Cree el archivo de script `test_function_parameter_python.py` dentro de la carpeta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` con la ruta predeterminada).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

<div id="udf-shell-script">
  ### UDF desde un script de shell
</div>

En este ejemplo, creamos un script de shell que multiplica cada valor por 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Archivo `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` si se usa la configuración de rutas predeterminada).

    ```xml title="/etc/clickhouse-server/test_function_shell.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_shell</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt8</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_shell.sh</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Archivo `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` si se usa la configuración de rutas predeterminada).

    ```yml title="/etc/clickhouse-server/test_function_shell.yaml"
    functions:
      type: executable
      name: test_shell
      return_type: String
      argument:
        - type: UInt8
          name: value
      format: TabSeparated
      command: test_shell.sh
    ```
  </TabItem>
</Tabs>

<br />

Cree el archivo de script `test_shell.sh` dentro de la carpeta `user_scripts` (`/var/lib/clickhouse/user_scripts/test_shell.sh` si se usa la configuración de rutas predeterminada).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```

<div id="error-handling">
  ## Manejo de errores
</div>

Algunas funciones pueden lanzar una excepción si los datos no son válidos.
En este caso, la consulta se cancela y se devuelve un mensaje de error al client.
En el procesamiento distribuido, cuando se produce una excepción en uno de los servidores, los demás servidores también intentan abortar la consulta.

<div id="evaluation-of-argument-expressions">
  ## Evaluación de las expresiones de los argumentos
</div>

En casi todos los lenguajes de programación, puede que uno de los argumentos no se evalúe con determinados operadores.
Normalmente, se trata de los operadores `&&`, `||` y `?:`.
En ClickHouse, los argumentos de las funciones (operadores) siempre se evalúan.
Esto se debe a que se evalúan partes completas de columnas de una sola vez, en lugar de calcular cada fila por separado.

<div id="performing-functions-for-distributed-query-processing">
  ## Ejecución de funciones para el procesamiento distribuido de consultas
</div>

En el procesamiento distribuido de consultas, tantas etapas del procesamiento de la consulta como sea posible se ejecutan en servidores remotos, y el resto de las etapas (la combinación de resultados intermedios y todo lo que viene después) se ejecutan en el servidor solicitante.

Esto significa que las funciones pueden ejecutarse en distintos servidores.
Por ejemplo, en la consulta `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

* si `distributed_table` tiene al menos dos segmentos, las funciones &#39;g&#39; y &#39;h&#39; se ejecutan en servidores remotos, y la función &#39;f&#39; se ejecuta en el servidor solicitante.
* si `distributed_table` tiene solo un segmento, todas las funciones &#39;f&#39;, &#39;g&#39; y &#39;h&#39; se ejecutan en el servidor de ese segmento.

El resultado de una función normalmente no depende del servidor en el que se ejecute. Sin embargo, a veces esto es importante.
Por ejemplo, las funciones que trabajan con diccionarios usan el diccionario que existe en el servidor en el que se ejecutan.
Otro ejemplo es la función `hostName`, que devuelve el nombre del servidor en el que se está ejecutando para poder hacer `GROUP BY` por servidores en una consulta `SELECT`.

Si una función de una consulta se ejecuta en el servidor solicitante, pero necesitas ejecutarla en servidores remotos, puedes envolverla en una función de agregación &#39;any&#39; o añadirla a una clave de `GROUP BY`.

<div id="sql-user-defined-functions">
  ## Funciones definidas por el usuario en SQL
</div>

Se pueden crear funciones personalizadas a partir de expresiones lambda mediante la sentencia [CREATE FUNCTION](../statements/create/function.md). Para eliminar estas funciones, utilice la sentencia [DROP FUNCTION](../statements/drop.md#drop-function).

<div id="webassembly-user-defined-functions">
  ## Funciones definidas por el usuario de WebAssembly
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

Las funciones definidas por el usuario de WebAssembly (WASM UDFs) le permiten ejecutar código personalizado compilado para WebAssembly dentro del proceso del servidor de ClickHouse.

<div id="quick-start">
  ### Inicio rápido
</div>

Habilite el soporte experimental de WebAssembly en la configuración de ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

Inserte el módulo WASM compilado en la tabla del sistema:

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Cree una función con su módulo WASM:

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

Usa la función en tus consultas:

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### Más información
</div>

Consulta la documentación sobre [las funciones definidas por el usuario de WebAssembly](wasm_udf.md) para obtener más información.

<div id="driver-based-executable-user-defined-functions">
  ## Funciones ejecutables definidas por el usuario basadas en drivers
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
Esta es una funcionalidad experimental que puede cambiar de maneras incompatibles con versiones anteriores en versiones futuras. Habilítela con la configuración del servidor [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers).
:::

Un *driver* es un adaptador proporcionado por el operador que convierte un fragmento de código del usuario en una [UDF ejecutable](#executable-user-defined-functions). Cuando se crea una función con `ENGINE = DriverName(...)`, ClickHouse ejecuta el `create_command` del driver, pasándole la firma de la función y el cuerpo del código; el driver compila el cuerpo o lo procesa de otro modo, y genera una configuración de UDF ejecutable que ClickHouse luego almacena y carga.

Esto permite a los administradores ofrecer a los usuarios una forma segura y limitada de definir funciones en un lenguaje arbitrario (por ejemplo, C compilado dentro de un contenedor aislado) sin darles acceso a los archivos de configuración ni al sistema de archivos del servidor. El conjunto de drivers disponibles está totalmente controlado por el operador.

<div id="enabling-drivers">
  ### Habilitar drivers
</div>

Las funciones ejecutables definidas por el usuario basadas en drivers están deshabilitados de forma predeterminada. Para habilitarlos:

1. Active la opción experimental en la configuración del servidor:

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. Haga que [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) apunte a uno o más archivos de configuración de drivers (se admite un glob) y, opcionalmente, establezca [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), el directorio donde se almacenan las configuraciones generadas de UDFs ejecutables:

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

El registro de drivers se carga al iniciar el servidor y se actualiza con `SYSTEM RELOAD CONFIG`, por lo que se pueden agregar, modificar o eliminar drivers sin reiniciar el servidor.

<div id="driver-configuration">
  ### Configuración del driver
</div>

Un driver se describe mediante un archivo XML (o YAML) con un elemento `<driver>` en el nivel superior. Se admiten los siguientes campos:

| Campo              | Descripción                                                                                                                                                                          | Obligatorio |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| `name`             | El nombre del driver, tal como se usa en `CREATE FUNCTION ... ENGINE = <name>(...)`.                                                                                                 | Sí          |
| `create_command`   | Ruta al programa que se invoca para crear una UDF a partir de un fragmento de código. Las rutas relativas se resuelven con respecto al archivo de configuración del driver.          | Sí          |
| `drop_command`     | Ruta al programa que se invoca cuando se elimina una función basada en este driver.                                                                                                  | No          |
| `engine_arguments` | Declara los argumentos permitidos dentro de `ENGINE = DriverName(...)`. Cada elemento hijo es un nombre de argumento; un hijo `<required>true</required>` indica que es obligatorio. | No          |
| `env`              | Variables de entorno que se exportan al invocar los comandos del driver.                                                                                                             | No          |

Ejemplo de configuración del driver:

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### Contrato de invocación del driver
</div>

Cuando se ejecuta `CREATE FUNCTION`, se invoca `create_command` con las variables `env` configuradas y los siguientes argumentos:

* `--name <function_name>`
* `--return <return_type>` (si hay una cláusula `RETURNS`)
* `--args <signature>` (si hay una cláusula `ARGUMENTS`), donde la firma es la lista de argumentos declarados; por ejemplo, `x UInt8, y DateTime`
* `--<key> <value>` para cada argumento del engine declarado que se proporcione en `ENGINE = DriverName(key = value)`

El cuerpo del código de usuario (el texto que aparece después de `AS`) se envía a la entrada estándar del comando. El comando debe imprimir la configuración de una UDF ejecutable en su salida estándar. El format se detecta automáticamente: la salida que comienza con `<` se trata como XML; en caso contrario, como YAML. El nombre de la función definida en la configuración generada debe coincidir con el nombre que se está creando. Si `create_command` finaliza con un status distinto de cero, la sentencia falla con una Exception que incluye el código de salida y el error estándar del driver.

Cuando está presente, `drop_command` se invoca de la misma manera (sin un cuerpo de código en stdin) al eliminar la función.

<div id="creating-a-function-with-a-driver">
  ### Crear una función
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

ClickHouse ejecuta el `create_command` del driver, escribe la configuración generada en [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) y el cargador existente de UDF ejecutables la recoge. Luego, la función puede llamarse como cualquier otra función.

<div id="dropping-a-function-with-a-driver">
  ### Eliminar una función
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

`DROP FUNCTION` invoca el `drop_command` del driver (si está presente), elimina la configuración dinámica generada y el directorio de trabajo de cada función, recarga el cargador de UDF ejecutables y elimina la consulta persistida.

<div id="driver-persistence-and-restart">
  ### Persistencia y reinicio
</div>

La consulta original se guarda como una sentencia `ATTACH FUNCTION ...` en el directorio de objetos SQL definidos por el usuario, por lo que la función sobrevive al reinicio del servidor. Al iniciarse, las configuraciones generadas en [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) se cargan directamente sin volver a ejecutar el driver. Si una sentencia `ATTACH FUNCTION` guardada no tiene una configuración generada correspondiente (por ejemplo, si se perdió el directorio dinámico), el driver se vuelve a ejecutar para recrearla.

<div id="driver-limitations">
  ### Limitaciones
</div>

* La funcionalidad es experimental y requiere habilitar `allow_experimental_executable_udf_drivers`.
* Las funciones basadas en drivers no son compatibles con el almacenamiento replicado de funciones definidas por el usuario (`ON CLUSTER` y `<user_defined_zookeeper_path>`), porque solo se replica la consulta original, no los artefactos generados.
* La operación `RESTORE` de una función basada en drivers incluida en una copia de seguridad conserva la consulta, pero no vuelve a ejecutar el driver; la configuración generada se materializa más adelante durante la recuperación tras el reinicio.

<div id="example-c-drivers">
  ### Ejemplo de drivers en C
</div>

El árbol de fuentes incluye drivers de prueba de concepto en `programs/server/user_defined_executable_function_drivers_config.d/` que compilan y ejecutan el cuerpo de una función en C. Son ejemplos y **no se instalan mediante paquetes**:

* `DockerC` - compila y ejecuta el código dentro de contenedores Docker aislados en un sandbox (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`, además de límites de memoria/CPU/PID), generando una UDF `executable_pool`.
* `GVisorC` - una variante que ejecuta el binario compilado con el runtime `runsc` de [gVisor](https://gvisor.dev/).
* `UnsafeC` - compila y ejecuta el código directamente en el host sin sandbox. Como indica el nombre, no proporciona ningún aislamiento y está pensado únicamente para entornos de confianza y pruebas.

Estos drivers de ejemplo están pensados como punto de partida; revisa y refuerza el aislamiento del sandbox para tu entorno antes de exponerlos a usuarios no confiables.

<div id="related-content">
  ## Contenido relacionado
</div>

* [Funciones definidas por el usuario en ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)