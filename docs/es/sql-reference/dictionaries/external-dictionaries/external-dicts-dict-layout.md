---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: Almacenamiento de diccionarios en la memoria
---

# Almacenamiento de diccionarios en la memoria {#dicts-external-dicts-dict-layout}

Hay una variedad de formas de almacenar diccionarios en la memoria.

Recomendamos [plano](#flat), [Hashed](#dicts-external_dicts_dict_layout-hashed) y [Método de codificación de datos:](#complex-key-hashed). que proporcionan una velocidad de procesamiento óptima.

No se recomienda el almacenamiento en caché debido al rendimiento potencialmente bajo y las dificultades para seleccionar los parámetros óptimos. Lea más en la sección “[cache](#cache)”.

Hay varias formas de mejorar el rendimiento del diccionario:

-   Llame a la función para trabajar con el diccionario después `GROUP BY`.
-   Marque los atributos para extraer como inyectivos. Un atributo se llama injective si diferentes valores de atributo corresponden a claves diferentes. Entonces, cuando `GROUP BY` utiliza una función que obtiene un valor de atributo mediante la clave, esta función se elimina automáticamente de `GROUP BY`.

ClickHouse genera una excepción para errores con diccionarios. Ejemplos de errores:

-   No se pudo cargar el diccionario al que se accede.
-   Error al consultar un `cached` diccionario.

Puede ver la lista de diccionarios externos y sus estados en el `system.dictionaries` tabla.

La configuración se ve así:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

Correspondiente [Consulta DDL](../../statements/create.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## Maneras de almacenar diccionarios en la memoria {#ways-to-store-dictionaries-in-memory}

-   [plano](#flat)
-   [Hashed](#dicts-external_dicts_dict_layout-hashed)
-   [Sistema abierto.](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [cache](#cache)
-   [directo](#direct)
-   [range\_hashed](#range-hashed)
-   [Método de codificación de datos:](#complex-key-hashed)
-   [complejo\_key\_cache](#complex-key-cache)
-   [Método de codificación de datos:](#ip-trie)

### plano {#flat}

El diccionario está completamente almacenado en la memoria en forma de matrices planas. ¿Cuánta memoria usa el diccionario? La cantidad es proporcional al tamaño de la clave más grande (en el espacio utilizado).

La clave del diccionario tiene el `UInt64` tipo y el valor está limitado a 500.000. Si se descubre una clave más grande al crear el diccionario, ClickHouse produce una excepción y no crea el diccionario.

Se admiten todos los tipos de fuentes. Al actualizar, los datos (de un archivo o de una tabla) se leen en su totalidad.

Este método proporciona el mejor rendimiento entre todos los métodos disponibles para almacenar el diccionario.

Ejemplo de configuración:

``` xml
<layout>
  <flat />
</layout>
```

o

``` sql
LAYOUT(FLAT())
```

### Hashed {#dicts-external_dicts_dict_layout-hashed}

El diccionario está completamente almacenado en la memoria en forma de una tabla hash. El diccionario puede contener cualquier número de elementos con cualquier identificador En la práctica, el número de claves puede alcanzar decenas de millones de elementos.

Se admiten todos los tipos de fuentes. Al actualizar, los datos (de un archivo o de una tabla) se leen en su totalidad.

Ejemplo de configuración:

``` xml
<layout>
  <hashed />
</layout>
```

o

``` sql
LAYOUT(HASHED())
```

### Sistema abierto {#dicts-external_dicts_dict_layout-sparse_hashed}

Similar a `hashed`, pero usa menos memoria a favor más uso de CPU.

Ejemplo de configuración:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### Método de codificación de datos: {#complex-key-hashed}

Este tipo de almacenamiento es para su uso con material compuesto [claves](external-dicts-dict-structure.md). Similar a `hashed`.

Ejemplo de configuración:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range\_hashed {#range-hashed}

El diccionario se almacena en la memoria en forma de una tabla hash con una matriz ordenada de rangos y sus valores correspondientes.

Este método de almacenamiento funciona de la misma manera que hash y permite el uso de intervalos de fecha / hora (tipo numérico arbitrario) además de la clave.

Ejemplo: La tabla contiene descuentos para cada anunciante en el formato:

``` text
+---------|-------------|-------------|------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------|-------------|-------------|------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------|-------------|-------------|------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------|-------------|-------------|------+
```

Para utilizar un ejemplo para intervalos de fechas, defina el `range_min` y `range_max` elementos en el [estructura](external-dicts-dict-structure.md). Estos elementos deben contener elementos `name` y`type` (si `type` no se especifica, se utilizará el tipo predeterminado - Fecha). `type` puede ser de cualquier tipo numérico (Fecha / DateTime / UInt64 / Int32 / otros).

Ejemplo:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

o

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

Para trabajar con estos diccionarios, debe pasar un argumento adicional al `dictGetT` función, para la que se selecciona un rango:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

Esta función devuelve el valor para el `id`s y el intervalo de fechas que incluye la fecha pasada.

Detalles del algoritmo:

-   Si el `id` no se encuentra o no se encuentra un rango para el `id` devuelve el valor predeterminado para el diccionario.
-   Si hay rangos superpuestos, puede usar cualquiera.
-   Si el delimitador de rango es `NULL` o una fecha no válida (como 1900-01-01 o 2039-01-01), el rango se deja abierto. La gama puede estar abierta en ambos lados.

Ejemplo de configuración:

``` xml
<yandex>
        <dictionary>

                ...

                <layout>
                        <range_hashed />
                </layout>

                <structure>
                        <id>
                                <name>Abcdef</name>
                        </id>
                        <range_min>
                                <name>StartTimeStamp</name>
                                <type>UInt64</type>
                        </range_min>
                        <range_max>
                                <name>EndTimeStamp</name>
                                <type>UInt64</type>
                        </range_max>
                        <attribute>
                                <name>XXXType</name>
                                <type>String</type>
                                <null_value />
                        </attribute>
                </structure>

        </dictionary>
</yandex>
```

o

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

### cache {#cache}

El diccionario se almacena en una memoria caché que tiene un número fijo de celdas. Estas celdas contienen elementos de uso frecuente.

Al buscar un diccionario, primero se busca en la memoria caché. Para cada bloque de datos, todas las claves que no se encuentran en la memoria caché o están desactualizadas se solicitan desde el origen utilizando `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Los datos recibidos se escriben en la memoria caché.

Para los diccionarios de caché, la caducidad [vida](external-dicts-dict-lifetime.md) de datos en la memoria caché se puede establecer. Si más tiempo que `lifetime` ha pasado desde que se cargaron los datos en una celda, el valor de la celda no se usa y se vuelve a solicitar la próxima vez que se deba usar.
Esta es la menos efectiva de todas las formas de almacenar diccionarios. La velocidad de la memoria caché depende en gran medida de la configuración correcta y del escenario de uso. Un diccionario de tipo de caché funciona bien solo cuando las tasas de aciertos son lo suficientemente altas (recomendado 99% y superior). Puede ver la tasa de aciertos promedio en el `system.dictionaries` tabla.

Para mejorar el rendimiento de la caché, utilice una subconsulta con `LIMIT`, y llame a la función con el diccionario externamente.

Apoyar [fuente](external-dicts-dict-sources.md): MySQL, ClickHouse, ejecutable, HTTP.

Ejemplo de configuración:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

o

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

Establezca un tamaño de caché lo suficientemente grande. Necesitas experimentar para seleccionar el número de celdas:

1.  Establecer algún valor.
2.  Ejecute consultas hasta que la memoria caché esté completamente llena.
3.  Evalúe el consumo de memoria utilizando el `system.dictionaries` tabla.
4.  Aumente o disminuya el número de celdas hasta que se alcance el consumo de memoria requerido.

!!! warning "Advertencia"
    No use ClickHouse como fuente, ya que es lento procesar consultas con lecturas aleatorias.

### complejo\_key\_cache {#complex-key-cache}

Este tipo de almacenamiento es para su uso con material compuesto [claves](external-dicts-dict-structure.md). Similar a `cache`.

### directo {#direct}

El diccionario no se almacena en la memoria y va directamente a la fuente durante el procesamiento de una solicitud.

La clave del diccionario tiene el `UInt64` tipo.

Todos los tipos de [fuente](external-dicts-dict-sources.md), excepto los archivos locales, son compatibles.

Ejemplo de configuración:

``` xml
<layout>
  <direct />
</layout>
```

o

``` sql
LAYOUT(DIRECT())
```

### Método de codificación de datos: {#ip-trie}

Este tipo de almacenamiento sirve para asignar prefijos de red (direcciones IP) a metadatos como ASN.

Ejemplo: La tabla contiene prefijos de red y su correspondiente número AS y código de país:

``` text
  +-----------|-----|------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------|-----|------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------|-----|------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------|-----|------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------|-----|------+
```

Cuando se utiliza este tipo de diseño, la estructura debe tener una clave compuesta.

Ejemplo:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
```

o

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

La clave debe tener solo un atributo de tipo String que contenga un prefijo IP permitido. Todavía no se admiten otros tipos.

Para consultas, debe utilizar las mismas funciones (`dictGetT` con una tupla) como para diccionarios con claves compuestas:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

La función toma cualquiera `UInt32` para IPv4, o `FixedString(16)` para IPv6:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Todavía no se admiten otros tipos. La función devuelve el atributo para el prefijo que corresponde a esta dirección IP. Si hay prefijos superpuestos, se devuelve el más específico.

Los datos se almacenan en un `trie`. Debe encajar completamente en la RAM.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
