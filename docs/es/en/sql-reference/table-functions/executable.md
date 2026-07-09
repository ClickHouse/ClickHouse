---
description: 'La función de tabla `executable` crea una tabla a partir de la salida
  de una función definida por el usuario (UDF) que defines en un script que escribe filas en
  **stdout**.'
keywords: ['udf', 'función definida por el usuario', 'clickhouse', 'executable', 'tabla', 'función']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'reference'
---

La función de tabla `executable` crea una tabla a partir de la salida de una función definida por el usuario (UDF) que defines en un script que escribe filas en **stdout**. El script ejecutable se almacena en el directorio `users_scripts` y puede leer datos de cualquier fuente. Asegúrate de que tu servidor de ClickHouse tenga todos los paquetes necesarios para ejecutar el script. Por ejemplo, si es un script de Python, asegúrate de que el servidor tenga instalados los paquetes de Python necesarios.

Opcionalmente, puedes incluir una o varias consultas de entrada que envíen sus resultados a **stdin** para que el script los lea.

:::note
Una ventaja clave de la función de tabla `executable` y del motor de tabla `Executable` frente a las funciones UDF convencionales es que estas últimas no pueden cambiar el número de filas. Por ejemplo, si la entrada tiene 100 filas, el resultado debe devolver 100 filas. Al usar la función de tabla `executable` o el motor de tabla `Executable`, tu script puede realizar cualquier transformación de datos que quieras, incluidas agregaciones complejas.
:::

<div id="syntax">
  ## Sintaxis
</div>

La función de tabla `executable` requiere tres parámetros y acepta una lista opcional de consultas de entrada:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: el nombre del archivo del script, guardado en la carpeta `user_scripts` (la carpeta predeterminada de la configuración `user_scripts_path`)
* `format`: el formato de la tabla generada
* `structure`: el esquema de la tabla generada
* `input_query`: una consulta opcional (o colección o consultas) cuyos resultados se pasan al script a través de **stdin**

:::note
Si va a invocar repetidamente el mismo script con las mismas consultas de entrada, considere usar el [motor de tabla `Executable`](../../engines/table-engines/special/executable.md).
:::

El siguiente script de Python se llama `generate_random.py` y se guarda en la carpeta `user_scripts`. Lee un número `i` e imprime `i` cadenas aleatorias, cada una precedida por un número separado por una tabulación:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Ejecutemos el script y hagamos que genere 10 cadenas aleatorias:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

La respuesta se verá así:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## Configuración
</div>

* `send_chunk_header` - controla si se envía el recuento de filas antes de enviar un fragmento de datos para su procesamiento. El valor predeterminado es `false`.
* `pool_size` — Tamaño del grupo. Si se especifica 0 como `pool_size`, no hay restricciones en el tamaño del grupo. El valor predeterminado es `16`.
* `max_command_execution_time` — Tiempo máximo de ejecución del comando del script ejecutable para procesar un bloque de datos. Se especifica en segundos. El valor predeterminado es 10.
* `command_termination_timeout` — el script ejecutable debe contener el bucle principal de lectura y escritura. Después de que se destruya la función de tabla, se cierre la tubería y el ejecutable tendrá `command_termination_timeout` segundos para finalizar antes de que ClickHouse envíe la señal SIGTERM al proceso hijo. Se especifica en segundos. El valor predeterminado es 10.
* `command_read_timeout` - tiempo de espera para leer datos del stdout del comando, en milisegundos. El valor predeterminado es 10000.
* `command_write_timeout` - tiempo de espera para escribir datos en el stdin del comando, en milisegundos. El valor predeterminado es 10000.

<div id="passing-query-results-to-a-script">
  ## Pasar los resultados de una consulta a un script
</div>

Consulta el ejemplo del motor de tabla `Executable` sobre [cómo pasar los resultados de una consulta a un script](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). A continuación se muestra cómo ejecutar el mismo script de ese ejemplo con la función de tabla `executable`:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```