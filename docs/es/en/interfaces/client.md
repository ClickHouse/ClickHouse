---
description: 'Documentación de la interfaz de Client de línea de comandos de ClickHouse'
sidebar_label: 'ClickHouse Client'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse Client'
doc_type: 'referencia'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse proporciona un Client de línea de comandos nativo para ejecutar consultas SQL directamente en un servidor ClickHouse.
Admite tanto el modo interactivo (para ejecutar consultas en tiempo real) como el modo por lotes (para scripts y automatización).
Los resultados de las consultas pueden mostrarse en la terminal o exportarse a un archivo, con compatibilidad con todos los [formatos](formats.md) de salida de ClickHouse, como Pretty, CSV, JSON, entre otros.

El Client ofrece información en tiempo real sobre la ejecución de las consultas, con una barra de progreso y el número de filas leídas, bytes procesados y el tiempo de ejecución de la consulta.
Admite tanto [opciones de línea de comandos](#command-line-options) como [archivos de configuración](#configuration_files).

<div id="install">
  ## Instalación
</div>

Para descargar ClickHouse, ejecute:

```bash
curl https://clickhouse.com/ | sh
```

Para instalarlo también, ejecuta:

```bash
sudo ./clickhouse install
```

Consulta [Instalar ClickHouse](../getting-started/install/install.mdx) para ver más opciones de instalación.

Las distintas versiones de Client y servidor son compatibles entre sí, pero es posible que algunas funciones no estén disponibles en Clients más antiguos. Recomendamos usar la misma versión para Client y servidor.

<div id="run">
  ## Ejecutar
</div>

:::note
Si solo descargó ClickHouse, pero no lo instaló, use `./clickhouse client` en lugar de `clickhouse-client`.
:::

Para conectarse a un servidor de ClickHouse, ejecute:

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

Especifique los detalles de conexión adicionales según sea necesario:

| Opción                           | Descripción                                                                                                                                                                                           |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | El puerto en el que ClickHouse server acepta conexiones. Los puertos predeterminados son 9440 (TLS) y 9000 (sin TLS). Tenga en cuenta que ClickHouse Client utiliza el protocolo nativo y no HTTP(S). |
| `-s [ --secure ]`                | Indica si se debe usar TLS (normalmente se detecta automáticamente).                                                                                                                                  |
| `-u [ --user ] <username>`       | El usuario de base de datos con el que se conectará. De forma predeterminada, se conecta como el usuario `default`.                                                                                   |
| `--password <password>`          | La contraseña del usuario de base de datos. También puede especificar la contraseña de una conexión en el archivo de configuración. Si no especifica la contraseña, el Client la solicitará.          |
| `-c [ --config ] <path-to-file>` | La ubicación del archivo de configuración de ClickHouse Client, si no se encuentra en una de las ubicaciones predeterminadas. Consulte [Archivos de configuración](#configuration_files).             |
| `--connection <name>`            | El nombre de los detalles de conexión preconfigurados en el [archivo de configuración](#connection-credentials).                                                                                      |

Para ver la lista completa de opciones de la línea de comandos, consulte [Opciones de línea de comandos](#command-line-options).

<div id="connecting-cloud">
  ### Conexión a ClickHouse Cloud
</div>

Los datos de su servicio de ClickHouse Cloud están disponibles en la consola de ClickHouse Cloud. Seleccione el servicio al que desea conectarse y haga clic en **Conectar**:

<Image img={cloud_connect_button} size="md" alt="Botón Conectar del servicio de ClickHouse Cloud" />

<br />

<br />

Elija **Native**; se mostrarán los datos junto con un comando `clickhouse-client` de ejemplo:

<Image img={connection_details_native} size="md" alt="Detalles de la conexión TCP nativa de ClickHouse Cloud" />

<div id="connection-credentials">
  ### Guardar conexiones en un archivo de configuración
</div>

Puede guardar los datos de conexión de uno o más servidores de ClickHouse en un [archivo de configuración](#configuration_files).

El formato es el siguiente:

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

Consulte la [sección sobre los archivos de configuración](#configuration_files) para obtener más información.

:::note
Para centrarse en la sintaxis de la consulta, en el resto de los ejemplos se omiten los detalles de conexión (`--host`, `--port`, etc.). Recuerde añadirlos cuando utilice los comandos.
:::

<div id="interactive-mode">
  ## Modo interactivo
</div>

<div id="using-interactive-mode">
  ### Uso del modo interactivo
</div>

Para ejecutar ClickHouse en modo interactivo, solo ejecute:

```bash
clickhouse-client
```

Esto abre el bucle de lectura, evaluación e impresión (REPL), donde puedes empezar a escribir consultas SQL de forma interactiva.
Una vez conectado, verás un indicador donde puedes introducir consultas:

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

En modo interactivo, el formato de salida predeterminado es `PrettyCompact`.
Puede cambiar el formato en la cláusula `FORMAT` de la consulta o especificando la opción de línea de comandos `--format`.
Para usar Vertical format, puede usar `--vertical` o especificar `\G` al final de la consulta.
En este formato, cada valor se imprime en una línea independiente, lo que resulta práctico para tablas anchas.

En modo interactivo, de forma predeterminada, todo lo que introduzca se ejecuta al pulsar `Enter`.
No es necesario añadir un punto y coma al final de la consulta.

Puede iniciar el Client con el parámetro `-m, --multiline`.
Para introducir una consulta multilínea, escriba una barra invertida `\` antes del salto de línea.
Después de pulsar `Enter`, se le pedirá que introduzca la siguiente línea de la consulta.
Para ejecutar la consulta, termínela con un punto y coma y pulse `Enter`.

ClickHouse Client se basa en `replxx` (similar a `readline`), por lo que usa atajos de teclado habituales y conserva un historial.
El historial se escribe en `~/.clickhouse-client-history` de forma predeterminada.

Para salir del Client, pulse `Ctrl+D` o introduzca una de las siguientes opciones en lugar de una consulta:

* `exit` o `exit;`
* `quit` o `quit;`
* `q`, `Q` o `:q`
* `logout` o `logout;`

<div id="getting-help">
  ### Obtener ayuda
</div>

Puedes consultar la documentación de cualquier función, motor de tabla, tipo de dato, formato, ajuste y otros componentes del sistema sin salir del Client. Escribe `help` seguido de un nombre (las formas equivalentes `/help`, `man` y `/man` también funcionan):

```text
help domainWithoutWWW
```

La búsqueda no distingue entre mayúsculas y minúsculas y consulta la tabla [`system.documentation`](../operations/system-tables/documentation.md). La documentación correspondiente se muestra en la terminal a partir de Markdown, con texto en negrita/cursiva, tablas y bloques de código con resaltado de sintaxis. Cuando varios componentes comparten un mismo nombre (por ejemplo, `file`, que es tanto una función como un motor de tabla), se muestran todos.

Cuando no hay ninguna coincidencia exacta, el Client enumera nombres similares (admitiendo posibles errores tipográficos) y los componentes cuya documentación menciona la palabra:

```text
help maxx_threads
```

Si se escribe `help` solo, se muestra un breve resumen de uso.

<div id="processing-info">
  ### Información sobre el procesamiento de consultas
</div>

Al procesar una consulta, el Client muestra:

1. Progress, que se actualiza de forma predeterminada como máximo 10 veces por segundo.
   En las consultas rápidas, es posible que no dé tiempo a mostrar el progreso.
2. La consulta formateada tras el análisis sintáctico, para depuración.
3. El resultado en el formato especificado.
4. El número de líneas del resultado, el tiempo transcurrido y la velocidad media de procesamiento de la consulta.
   Todas las cantidades de datos se refieren a datos sin comprimir.

Puede cancelar una consulta larga pulsando `Ctrl+C`.
Sin embargo, tendrá que esperar un poco a que el servidor aborte la solicitud.
No es posible cancelar una consulta en ciertas etapas.
Si no espera y pulsa `Ctrl+C` una segunda vez, el Client se cerrará.

ClickHouse Client permite pasar datos externos (tablas temporales externas) para realizar consultas.
Para obtener más información, consulte la sección [Datos externos para el procesamiento de consultas](../engines/table-engines/special/external-data.md).

<div id="cli_aliases">
  ### Alias
</div>

Puede usar los siguientes alias en el REPL:

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - repite la última consulta

<div id="keyboard_shortcuts">
  ### Atajos de teclado
</div>

* `Alt (Option) + Shift + e` - abre el editor con la consulta actual. Se puede especificar qué editor usar con la variable de entorno `EDITOR`. De forma predeterminada, se usa `vim`.
* `Alt (Option) + #` - comentar la línea.
* `Ctrl + r` - búsqueda difusa en el historial.

La lista completa de todos los atajos de teclado disponibles está en [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
Para configurar correctamente la tecla Meta (Option) en MacOS:

iTerm2: Vaya a Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key y haga clic en Esc+
:::

<div id="batch-mode">
  ## Modo por lotes
</div>

<div id="using-batch-mode">
  ### Uso del modo por lotes
</div>

En lugar de usar ClickHouse Client de forma interactiva, puedes ejecutarlo en modo por lotes.
En modo por lotes, ClickHouse ejecuta una sola consulta y se cierra de inmediato; no hay prompt interactivo ni bucle.

Puedes especificar una sola consulta así:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

También puede utilizar la opción de línea de comandos `--query`:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

Puedes proporcionar una consulta a través de `stdin`:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

Asumiendo que existe una tabla `messages`, también puede insertar datos desde la línea de comandos:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

Cuando se especifica `--query`, cualquier entrada se agrega a la petición tras un salto de línea.

<div id="cloud-example">
  ### Inserción de un archivo CSV en un servicio remoto de ClickHouse
</div>

En este ejemplo, se inserta el archivo CSV de ejemplo `cell_towers.csv` en la tabla existente `cell_towers` de la base de datos `default`:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### Ejemplos de inserción de datos desde la línea de comandos
</div>

Hay varias formas de insertar datos desde la línea de comandos.
El siguiente ejemplo inserta dos filas de datos CSV en una tabla de ClickHouse en modo por lotes:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

En el ejemplo siguiente, `cat <<_EOF` inicia un heredoc que leerá todo hasta volver a encontrar `_EOF` y, a continuación, lo mostrará:

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

En el ejemplo siguiente, el contenido de file.csv se envía a stdout mediante `cat` y se redirige a `clickhouse-client` como entrada:

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

En el modo por lotes, el [formato](formats.md) de datos predeterminado es `TabSeparated`.
Puede definir el formato en la cláusula `FORMAT` de la consulta, como se muestra en el ejemplo anterior.

<div id="cli-queries-with-parameters">
  ## Consultas con parámetros
</div>

Puede especificar parámetros en una consulta y pasarles valores mediante opciones de línea de comandos.
Esto evita tener que dar formato a una consulta con valores dinámicos específicos en el lado del cliente.
Por ejemplo:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

También es posible definir parámetros desde una [sesión interactiva](#interactive-mode):

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### Sintaxis de la consulta
</div>

En la consulta, coloque entre llaves los valores que quiera completar mediante parámetros de línea de comandos con el siguiente formato:

```sql
{<name>:<data type>}
```

| Parámetro   | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | Identificador del marcador de posición. La opción correspondiente de la línea de comandos es `--param_<name> = value`.                                                                                                                                                                                                                                                                                                                                                                                                      |
| `data type` | [Tipo de dato](../sql-reference/data-types/index.md) del parámetro. <br /><br />Por ejemplo, una estructura de datos como `(integer, ('string', integer))` puede tener el tipo de dato `Tuple(UInt8, Tuple(String, UInt8))` (también se pueden usar otros tipos [integer](../sql-reference/data-types/int-uint.md)). <br /><br />También es posible pasar como parámetros el nombre de la tabla, el nombre de la base de datos y los nombres de las columnas; en ese caso, tendría que usar `Identifier` como tipo de dato. |

<div id="cli-queries-with-parameters-examples">
  ### Ejemplos
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## Generación de SQL con IA
</div>

ClickHouse Client incluye asistencia de IA integrada para generar consultas SQL a partir de descripciones en lenguaje natural. Esta función ayuda a los usuarios a escribir consultas complejas sin necesidad de tener conocimientos avanzados de SQL.

La asistencia de IA funciona de forma inmediata si tienes definida la variable de entorno `OPENAI_API_KEY` o `ANTHROPIC_API_KEY`. Para una configuración más avanzada, consulta la sección [Configuración](#ai-sql-generation-configuration).

<div id="ai-sql-generation-usage">
  ### Uso
</div>

Para usar la generación de SQL con IA, añada `??` al principio de su consulta en lenguaje natural:

```bash
:) ?? show all users who made purchases in the last 30 days
```

La IA:

1. Explorará automáticamente el esquema de tu base de datos
2. Generará el SQL adecuado a partir de las tablas y columnas detectadas
3. Ejecutará la consulta generada de inmediato

<div id="ai-sql-generation-example">
  ### Ejemplo
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### Configuración
</div>

La generación de SQL con IA requiere configurar un proveedor de IA en el archivo de configuración de ClickHouse Client. Puede usar OpenAI, Anthropic o cualquier servicio de API compatible con OpenAI.

<div id="ai-sql-generation-fallback">
  #### Uso de variables de entorno como alternativa
</div>

Si no se especifica ninguna configuración de IA en el archivo de configuración, ClickHouse Client intentará usar automáticamente variables de entorno:

1. Primero, comprueba la variable de entorno `OPENAI_API_KEY`
2. Si no la encuentra, comprueba la variable de entorno `ANTHROPIC_API_KEY`
3. Si no encuentra ninguna de las dos, las funciones de IA se desactivarán

Esto permite una configuración rápida sin necesidad de archivos de configuración:

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### Archivo de configuración
</div>

Para tener más control sobre la configuración de IA, configúrala en el archivo de configuración de tu ClickHouse Client ubicado en:

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (o `~/.config/clickhouse/config.xml` si `XDG_CONFIG_HOME` no está definido) (formato XML)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (o `~/.config/clickhouse/config.yaml` si `XDG_CONFIG_HOME` no está definido) (formato YAML)
* `~/.clickhouse-client/config.xml` (formato XML, ubicación heredada)
* `~/.clickhouse-client/config.yaml` (formato YAML, ubicación heredada)
* O especifica una ubicación personalizada con `--config-file`

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- Obligatorio: Tu clave de API (o configúrala mediante una variable de entorno) -->
            <api_key>your-api-key-here</api_key>

            <!-- Obligatorio: Tipo de proveedor (openai, anthropic) -->
            <provider>openai</provider>

            <!-- Modelo que se usará (los valores predeterminados varían según el proveedor) -->
            <model>gpt-4o</model>

            <!-- Opcional: endpoint de API personalizado para servicios compatibles con OpenAI -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- Configuración de exploración del esquema -->
            <enable_schema_access>true</enable_schema_access>

            <!-- Parámetros de generación -->
            <!-- Opcional: temperature solo se envía al modelo cuando se establece aquí.
                 Se omite de forma predeterminada porque algunos modelos rechazan este parámetro. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- Opcional: prompt del sistema personalizado -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # Obligatorio: Tu clave de API (o configúrala mediante una variable de entorno)
      api_key: your-api-key-here

      # Obligatorio: Tipo de proveedor (openai, anthropic)
      provider: openai

      # Modelo que se usará
      model: gpt-4o

      # Opcional: endpoint de API personalizado para servicios compatibles con OpenAI
      # base_url: https://openrouter.ai/api

      # Habilita el acceso al esquema: permite que la IA consulte información de la base de datos/tabla
      enable_schema_access: true

      # Parámetros de generación
      # temperature solo se envía al modelo cuando se establece aquí; se omite de forma predeterminada
      # porque algunos modelos rechazan este parámetro.
      # temperature: 0.0    # Controla la aleatoriedad (0.0 = determinista)
      max_tokens: 1000      # Longitud máxima de la respuesta
      timeout_seconds: 30   # Tiempo de espera de la solicitud
      max_steps: 10         # Número máximo de pasos de exploración del esquema

      # Opcional: prompt del sistema personalizado
      # system_prompt: |
      #   You are an expert ClickHouse SQL assistant. Convert natural language to SQL.
      #   Focus on performance and use ClickHouse-specific optimizations.
      #   Always return executable SQL without explanations.
    ```
  </TabItem>
</Tabs>

<br />

**Uso de API compatibles con OpenAI (p. ej., OpenRouter):**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**Ejemplos mínimos de configuración:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### Parámetros
</div>

<details>
  <summary>Parámetros obligatorios</summary>

  * `api_key` - Tu clave de API para el servicio de IA. Puede omitirse si se establece mediante una variable de entorno:
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * Nota: la clave de API del archivo de configuración tiene prioridad sobre la variable de entorno
  * `provider` - El proveedor de IA: `openai` o `anthropic`
    * Si se omite, usa automáticamente una alternativa según las variables de entorno disponibles
</details>

<details>
  <summary>Configuración del modelo</summary>

  * `model` - El modelo que se va a usar (predeterminado: específico del proveedor)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo`, etc.
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`, etc.
    * OpenRouter: usa su nomenclatura de modelos, como `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>Configuración de la conexión</summary>

  * `base_url` - Endpoint de API personalizado para servicios compatibles con OpenAI (opcional)
  * `timeout_seconds` - Tiempo de espera de la solicitud en segundos (predeterminado: `30`)
</details>

<details>
  <summary>Exploración de esquemas</summary>

  * `enable_schema_access` - Permite que la IA explore los esquemas de la base de datos (predeterminado: `true`)
  * `max_steps` - Número máximo de pasos de llamada a herramientas para la exploración de esquemas (predeterminado: `10`)
</details>

<details>
  <summary>Parámetros de generación</summary>

  * `temperature` - Controla la aleatoriedad; 0.0 = determinista, 1.0 = creativo. De forma predeterminada se omite y solo se envía al modelo cuando se establece explícitamente, porque algunos modelos rechazan este parámetro.
  * `max_tokens` - Longitud máxima de la respuesta en tokens (predeterminado: `1000`)
  * `system_prompt` - Instrucciones personalizadas para la IA (opcional)
</details>

<div id="ai-sql-generation-how-it-works">
  ### Cómo funciona
</div>

El generador de SQL con IA utiliza un proceso de varios pasos:

<VerticalStepper headerLevel="list">
  1. **Descubrimiento del esquema**

  La IA utiliza herramientas integradas para explorar su base de datos

  * Enumera las bases de datos disponibles
  * Descubre tablas en las bases de datos relevantes
  * Examina las estructuras de las tablas mediante sentencias `CREATE TABLE`

  2. **Generación de consultas**

  Según el esquema detectado, la IA genera SQL que:

  * Se ajusta a su intención expresada en lenguaje natural
  * Utiliza los nombres correctos de tablas y columnas
  * Aplica JOIN y agregaciones adecuados

  3. **Ejecución**

  El SQL generado se ejecuta automáticamente y se muestran los resultados
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### Limitaciones
</div>

* Requiere una conexión a internet activa
* El uso de la API está sujeto a límites de uso y costos del proveedor de IA
* Las consultas complejas pueden requerir varios ajustes
* La IA tiene acceso de solo lectura a la información del esquema, no a los datos reales

<div id="ai-sql-generation-security">
  ### Seguridad
</div>

* Las claves de API nunca se envían a los servidores de ClickHouse
* La IA solo ve información del esquema (nombres de tablas y columnas, y tipos), no los datos reales
* Todas las consultas generadas respetan los permisos existentes de su base de datos

<div id="connection_string">
  ## Cadena de conexión
</div>

<div id="ai-sql-generation-usage">
  ### Uso
</div>

ClickHouse Client también admite conectarse a un servidor de ClickHouse mediante una cadena de conexión similar a las de [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) y [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). La sintaxis es la siguiente:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| Componente (todos opcionales) | Descripción                                                                                                                                                                                              | Predeterminado   |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `user`                        | Nombre de usuario de la base de datos.                                                                                                                                                                   | `default`        |
| `password`                    | Contraseña del usuario de base de datos. Si se especifica `:` y la contraseña se deja en blanco, el client solicitará la contraseña del usuario.                                                         | -                |
| `hosts_and_ports`             | Lista de hosts y puertos opcionales `host[:port] [, host:[port]], ...`.                                                                                                                                  | `localhost:9000` |
| `database`                    | Nombre de la base de datos.                                                                                                                                                                              | `default`        |
| `query_parameters`            | Lista de pares clave-valor `param1=value1[,&param2=value2], ...`. Para algunos parámetros, no se requiere ningún valor. Los nombres y valores de los parámetros son sensibles a mayúsculas y minúsculas. | -                |

<div id="connection-string-notes">
  ### Notas
</div>

Si el nombre de usuario, la contraseña o la base de datos se especificaron en la cadena de conexión, no se pueden especificar con `--user`, `--password` o `--database` (y viceversa).

El componente host puede ser un hostname o una dirección IPv4 o IPv6.
Las direcciones IPv6 deben ir entre `[]`:

```text
clickhouse://[2001:db8::1234]
```

Las cadenas de conexión pueden contener varios hosts.
ClickHouse Client intentará conectarse a estos hosts en orden (de izquierda a derecha).
Una vez establecida la conexión, no se intentará conectar a los hosts restantes.

La cadena de conexión debe especificarse como el primer argumento de `clickHouse-client`.
La cadena de conexión puede combinarse con cualquier número de otras [opciones de línea de comandos](#command-line-options), excepto `--host` y `--port`.

Se permiten las siguientes claves para `query_parameters`:

| Clave            | Descripción                                                                                                                                                                 |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `secure` (o `s`) | Si se especifica, el client se conectará al servidor mediante una conexión segura (TLS). Consulte `--secure` en las [opciones de línea de comandos](#command-line-options). |

**Codificación porcentual**

Los caracteres que no pertenezcan a US-ASCII, los espacios y los caracteres especiales de los siguientes parámetros deben codificarse mediante [codificación porcentual](https://en.wikipedia.org/wiki/URL_encoding):

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### Ejemplos
</div>

Conéctese a `localhost` por el puerto 9000 y ejecute la consulta `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Conéctese a `localhost` como el usuario `john`, con la contraseña `secret`, el host `127.0.0.1` y el puerto `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Conéctese a `localhost` como el usuario `default`, con el host `[::1]` (dirección IPv6) y el puerto `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

Conéctese a `localhost` por el puerto 9000 en modo multilínea.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Conéctese a `localhost` por el puerto 9000 con el usuario `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Conéctese a `localhost` en el puerto 9000 y use `my_database` como base de datos predeterminada.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Conéctese a `localhost` en el puerto 9000, use de forma predeterminada la base de datos `my_database` especificada en la cadena de conexión y establezca una conexión segura mediante el parámetro abreviado `s`.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Conéctese al host predeterminado con el puerto predeterminado, el usuario `default` y la base de datos predeterminada.

```bash
clickhouse-client clickhouse:
```

Conéctese al host predeterminado usando el puerto predeterminado, con el usuario `my_user` y sin contraseña.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Conéctese a `localhost` usando la dirección de correo electrónico como nombre de usuario. El símbolo `@` se codifica como `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Conéctese a uno de los dos hosts: `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## Formato del Query ID
</div>

En modo interactivo, ClickHouse Client muestra el Query ID de cada consulta. De forma predeterminada, el ID tiene este formato:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

Se puede especificar un formato personalizado en un archivo de configuración dentro de una etiqueta `query_id_formats`. El marcador `{query_id}` de la cadena de formato se reemplaza por el ID de la consulta. Se permiten varias cadenas de formato dentro de la etiqueta.
Esta funcionalidad puede usarse para generar URL y facilitar el perfilado de consultas.

**Ejemplo**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

Con la configuración anterior, el ID de una consulta se muestra en el siguiente formato:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## Archivos de configuración
</div>

ClickHouse Client usa el primer archivo existente de la siguiente lista:

* Un archivo definido con el parámetro `-c [ -C, --config, --config-file ]`.
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (o `~/.config/clickhouse/config.[xml|yaml|yml]` si `XDG_CONFIG_HOME` no está definida)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

Consulta el archivo de configuración de ejemplo en el repositorio de ClickHouse: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## Opciones de variables de entorno
</div>

El nombre de usuario, la contraseña y el host se pueden establecer mediante las variables de entorno `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` y `CLICKHOUSE_HOST`.
Los argumentos de la línea de comandos `--user`, `--password` o `--host`, o una [cadena de conexión](#connection_string) (si se especifica), tienen prioridad sobre las variables de entorno.

<div id="command-line-options">
  ## Opciones de línea de comandos
</div>

Todas las opciones de línea de comandos pueden especificarse directamente en la línea de comandos o establecerse como valores predeterminados en el [archivo de configuración](#configuration_files).

<div id="command-line-options-general">
  ### Opciones generales
</div>

| Option                                              | Description                                                                                                                                                                         | Default                      |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | La ubicación del archivo de configuración del client, si no se encuentra en ninguna de las ubicaciones predeterminadas. Consulte [Archivos de configuración](#configuration_files). | -                            |
| `--help`                                            | Muestra el resumen de uso y sale. Combínelo con `--verbose` para mostrar todas las opciones posibles, incluido el ajuste de consulta.                                              | -                            |
| `--history_file <path-to-file>`                     | Ruta a un archivo que contiene el historial de comandos.                                                                                                                            | -                            |
| `--history_max_entries`                             | Número máximo de entradas en el archivo de historial.                                                                                                                               | `1000000` (1 millón)         |
| `--prompt <prompt>`                                 | Especifica un prompt personalizado.                                                                                                                                                 | El `display_name` del server |
| `--verbose`                                         | Aumenta el nivel de detalle de la salida.                                                                                                                                           | -                            |
| `-V [ --version ]`                                  | Muestra la versión y sale.                                                                                                                                                          | -                            |

<div id="command-line-options-connection">
  ### Opciones de conexión
</div>

| Option                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Default                                                                                                                                                 |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--connection <name>`                | El nombre de los detalles de conexión preconfigurados en el archivo de configuración. Consulta [Credenciales de conexión](#connection-credentials).                                                                                                                                                                                                                                                                                                                     | -                                                                                                                                                       |
| `-d [ --database ] <database>`       | Selecciona la base de datos predeterminada para esta conexión.                                                                                                                                                                                                                                                                                                                                                                                                          | La base de datos actual de la configuración del servidor (`default` de forma predeterminada)                                                            |
| `-h [ --host ] <host>`               | El hostname del servidor de ClickHouse al que conectarse. Puede ser un hostname o una dirección IPv4 o IPv6. Se pueden pasar varios hosts mediante varios argumentos.                                                                                                                                                                                                                                                                                                   | `localhost`                                                                                                                                             |
| `--jwt <value>`                      | Usa un JSON Web Token (JWT) para la autenticación. <br /><br />La autorización JWT del servidor solo está disponible en ClickHouse Cloud.                                                                                                                                                                                                                                                                                                                               | -                                                                                                                                                       |
| `login`                              | Invoca el flujo de OAuth de concesión de dispositivo para autenticarse mediante un IdP. <br /><br />En los hosts de ClickHouse Cloud, las variables de OAuth se infieren automáticamente; en caso contrario, deben proporcionarse con `--oauth-url`, `--oauth-client-id` y `--oauth-audience`.                                                                                                                                                                          | -                                                                                                                                                       |
| `--no-warnings`                      | Desactiva la visualización de advertencias de `system.warnings` cuando el client se conecta al servidor.                                                                                                                                                                                                                                                                                                                                                                | -                                                                                                                                                       |
| `--no-server-client-version-message` | Suprime el mensaje de incompatibilidad de versiones entre el servidor y el client cuando el client se conecta al servidor.                                                                                                                                                                                                                                                                                                                                              | -                                                                                                                                                       |
| `--password <password>`              | La contraseña del usuario de base de datos. También puedes especificar la contraseña de una conexión en el archivo de configuración. Si no especificas la contraseña, el client la solicitará.                                                                                                                                                                                                                                                                          | -                                                                                                                                                       |
| `--port <port>`                      | El puerto en el que el servidor acepta conexiones. Los puertos predeterminados son 9440 (TLS) y 9000 (sin TLS). <br /><br />Nota: El client usa el protocolo nativo, no HTTP(S).                                                                                                                                                                                                                                                                                        | `9440` si se especifica `--secure`; `9000` en caso contrario. Siempre usa `9440` de forma predeterminada si el hostname termina en `.clickhouse.cloud`. |
| `-s [ --secure ]`                    | Indica si se debe usar TLS. <br /><br />Se habilita automáticamente al conectarse al puerto 9440 (el puerto seguro predeterminado) o a ClickHouse Cloud. <br /><br />Puede que necesites configurar tus certificados de CA en el [archivo de configuración](#configuration_files). Los ajustes de configuración disponibles son los mismos que para la [configuración de TLS del lado del servidor](../operations/server-configuration-parameters/settings.md#openssl). | Se habilita automáticamente al conectarse al puerto 9440 o a ClickHouse Cloud                                                                           |
| `--ssh-key-file <path-to-file>`      | Archivo que contiene la clave privada SSH para autenticarse con el servidor.                                                                                                                                                                                                                                                                                                                                                                                            | -                                                                                                                                                       |
| `--ssh-key-passphrase <value>`       | Frase de contraseña para la clave privada SSH especificada en `--ssh-key-file`.                                                                                                                                                                                                                                                                                                                                                                                         | -                                                                                                                                                       |
| `--tls-sni-override <server name>`   | Si se usa TLS, el nombre del servidor (SNI) que se envía en el handshake.                                                                                                                                                                                                                                                                                                                                                                                               | El host proporcionado mediante `-h` o `--host`.                                                                                                         |
| `-u [ --user ] <username>`           | El usuario de base de datos con el que conectarse.                                                                                                                                                                                                                                                                                                                                                                                                                      | `default`                                                                                                                                               |

:::note
En lugar de las opciones `--host`, `--port`, `--user` y `--password`, el client también admite [cadenas de conexión](#connection_string).
:::

<div id="command-line-options-query">
  ### Opciones de consulta
</div>

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--param_<name>=<value>`        | Valor de sustitución para un parámetro de una [consulta con parámetros](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `-q [ --query ] <query>`        | La consulta que se ejecutará en modo por lotes. Puede especificarse varias veces (`--query "SELECT 1" --query "SELECT 2"`) o una sola vez con varias consultas separadas por punto y coma (`--query "SELECT 1; SELECT 2;"`). En este último caso, las consultas `INSERT` con formatos distintos de `VALUES` deben separarse con líneas vacías. <br /><br />También puede especificarse una sola consulta sin ningún parámetro: `clickhouse-client "SELECT 1"` <br /><br />No puede usarse junto con `--queries-file`.                                                                                                                                                                                                          |
| `--queries-file <path-to-file>` | Ruta a un archivo que contiene consultas. `--queries-file` puede especificarse varias veces; por ejemplo, `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />No puede usarse junto con `--query`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `-m [ --multiline ]`            | Si se especifica, permite consultas de varias líneas (no envía la consulta al pulsar Enter). Las consultas solo se enviarán cuando terminen con un punto y coma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `--inline-insert-data`          | Envía `INSERT ... VALUES` (y otros formatos en línea) tal cual en el texto de la consulta, en lugar de convertir los datos en bloques en el formato nativo. El server analiza los datos en línea por sí mismo, lo que evita el viaje de ida y vuelta necesario para enviar la estructura de la tabla y los valores predeterminados de las columnas de vuelta al client. Esto puede mejorar el rendimiento de muchas inserciones pequeñas a través del protocolo nativo. Establece automáticamente [`send_table_structure_on_insert_with_inline_data`](/es/operations/settings/settings#send_table_structure_on_insert_with_inline_data) en `0`. No puede combinarse con datos en línea y datos externos (desde stdin o `INFILE`). |

<div id="command-line-options-query-settings">
  ### Ajustes de consulta
</div>

Los ajustes de la consulta se pueden especificar como opciones de línea de comandos en el client; por ejemplo:

```bash
$ clickhouse-client --max_threads 1
```

Consulte [Settings](../operations/settings/settings.md) para obtener una lista de opciones de configuración.

<div id="command-line-options-formatting">
  ### Opciones de formato
</div>

| Opción                            | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Predeterminado                                                     |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `-f [ --format ] <format>`        | Use el formato especificado para mostrar el resultado. <br /><br />Consulte [Formats for Input and Output Data](formats.md) para ver una lista de formatos compatibles.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `TabSeparated`                                                     |
| `--pager <command>`               | Envíe toda la salida a este comando. Normalmente `less` (p. ej., `less -S` para mostrar result sets amplios) o uno similar.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | -                                                                  |
| `-E [ --vertical ]`               | Use el [formato vertical](/es/interfaces/formats/Vertical) para mostrar el resultado. Equivale a `–-format Vertical`. En este formato, cada valor se imprime en una línea independiente, lo que resulta útil al mostrar tablas anchas.                                                                                                                                                                                                                                                                                                                                                                                                            | -                                                                  |
| `--echo [ <bool> ]`               | Imprima cada consulta antes de ejecutarla. Acepta un valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `true` en modo interactivo, `false` en modo no interactivo (batch) |
| `--echo-formatted [ <bool> ]`     | Dé formato a las consultas mostradas por eco. Acepta un valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `true` en modo interactivo, `false` en modo no interactivo (batch) |
| `--echo-query-id [ <bool> ]`      | Imprima el Query id antes de ejecutarla. Acepta un valor booleano opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `true` en modo interactivo, `false` en modo no interactivo (batch) |
| `--echo-query-separator <string>` | Imprima este separador antes de la consulta mostrada por eco con formato (requiere `--echo-formatted`), lo que facilita distinguir la consulta escrita de su versión reformateada.                                                                                                                                                                                                                                                                                                                                                                                                                                                             | Vacío (deshabilitado)                                              |
| `--highlight [ --hilite ] <bool>` | Active o desactive el resaltado de sintaxis del prompt de comandos y de las consultas mostradas por eco.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `true`                                                             |
| `--hints <bool>`                  | Muestre sugerencias de autocompletado mientras escribe (texto &quot;fantasma&quot; inline) para la coincidencia más probable cuando el cursor esté al final de la entrada. Desplácese por las sugerencias con Arriba/Abajo (o Ctrl-Arriba/Ctrl-Abajo); acepte la sugerencia inline con Tab o Flecha derecha; `Enter` acepta una sugerencia solo después de haberla seleccionado explícitamente y, en caso contrario, ejecuta la consulta; `Tab` también abre la lista clásica de autocompletado. Requiere `--highlight` (las sugerencias necesitan color) y el motor de sugerencias (por lo que `--disable_suggestion` también las desactiva). | `true`                                                             |

<div id="command-line-options-execution-details">
  ### Detalles de ejecución
</div>

| Opción                           | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Predeterminado                                                  |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| `--chime [N]`                    | Escribe el carácter de control `BEL` en `stderr` cuando una consulta finaliza (tanto si termina correctamente como si produce un error) después de haberse ejecutado durante al menos `N` segundos. Solo se emite cuando `stderr` está conectado a un terminal (TTY); redirigir `stderr` (por ejemplo, `2>err.log`) lo suprime, mientras que redirigir `stdout` (por ejemplo, `> result.tsv`) no lo hace. Pasar `--chime` sin un valor usa el umbral predeterminado. Establece `--chime 0` para desactivarlo. | `5` segundos                                                    |
| `--enable-progress-table-toggle` | Habilita la alternancia de la tabla de progreso al pulsar la tecla de control (Espacio). Solo se aplica en modo interactivo cuando la impresión de la tabla de progreso está habilitada.                                                                                                                                                                                                                                                                                                                      | `enabled`                                                       |
| `--hardware-utilization`         | Imprime información de uso del hardware en la barra de progreso.                                                                                                                                                                                                                                                                                                                                                                                                                                              | -                                                               |
| `--memory-usage`                 | Si se especifica, imprime el uso de memoria en `stderr` en modo no interactivo. <br /><br />Valores posibles: <br />• `none` - no imprime el uso de memoria <br />• `default` - imprime el número de bytes <br />• `readable` - imprime el uso de memoria en un formato legible para humanos                                                                                                                                                                                                                  | -                                                               |
| `--print-profile-events`         | Imprime paquetes `ProfileEvents`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | -                                                               |
| `--progress`                     | Imprime el progreso de ejecución de la consulta. <br /><br />Valores posibles: <br />• `tty\|on\|1\|true\|yes` - muestra la salida en el terminal en modo interactivo <br />• `err` - muestra la salida en `stderr` en modo no interactivo <br />• `off\|0\|false\|no` - desactiva la impresión del progreso                                                                                                                                                                                                  | `tty` en modo interactivo, `off` en modo no interactivo (batch) |
| `--progress-table`               | Imprime una tabla de progreso con métricas que cambian durante la ejecución de la consulta. <br /><br />Valores posibles: <br />• `tty\|on\|1\|true\|yes` - muestra la salida en el terminal en modo interactivo <br />• `err` - muestra la salida en `stderr` en modo no interactivo <br />• `off\|0\|false\|no` - desactiva la tabla de progreso                                                                                                                                                            | `tty` en modo interactivo, `off` en modo no interactivo (batch) |
| `--stacktrace`                   | Imprime stack traces de excepciones.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | -                                                               |
| `-t [ --time ]`                  | Imprime el tiempo de ejecución de la consulta en `stderr` en modo no interactivo (para benchmarks).                                                                                                                                                                                                                                                                                                                                                                                                           | -                                                               |