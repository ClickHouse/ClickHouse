# Configuración de un diccionario externo {#dicts-external-dicts-dict}

Si el diccionario se configura usando un archivo xml, la configuración del diccionario tiene la siguiente estructura:

``` xml
<dictionary>
    <name>dict_name</name>

    <structure>
      <!-- Complex key configuration -->
    </structure>

    <source>
      <!-- Source configuration -->
    </source>

    <layout>
      <!-- Memory layout configuration -->
    </layout>

    <lifetime>
      <!-- Lifetime of dictionary in memory -->
    </lifetime>
</dictionary>
```

Correspondiente [Consulta DDL](../create.md#create-dictionary-query) tiene la siguiente estructura:

``` sql
CREATE DICTIONARY dict_name
(
    ... -- attributes
)
PRIMARY KEY ... -- complex or single key configuration
SOURCE(...) -- Source configuration
LAYOUT(...) -- Memory layout configuration
LIFETIME(...) -- Lifetime of dictionary in memory
```

-   `name` – El identificador que se puede utilizar para acceder al diccionario. Usa los personajes `[a-zA-Z0-9_\-]`.
-   [fuente](external_dicts_dict_sources.md) — Fuente del diccionario.
-   [diseño](external_dicts_dict_layout.md) — Diseño del diccionario en la memoria.
-   [estructura](external_dicts_dict_structure.md) — Estructura del diccionario . Una clave y atributos que se pueden recuperar con esta clave.
-   [vida](external_dicts_dict_lifetime.md) — Frecuencia de actualizaciones del diccionario.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/dicts/external_dicts_dict/) <!--hide-->
