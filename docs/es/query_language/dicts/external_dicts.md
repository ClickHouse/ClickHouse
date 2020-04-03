---
machine_translated: true
---

# Diccionarios externos {#dicts-external-dicts}

Puede agregar sus propios diccionarios de varias fuentes de datos. El origen de datos de un diccionario puede ser un archivo ejecutable o de texto local, un recurso HTTP u otro DBMS. Para obtener más información, consulte “[Fuentes para diccionarios externos](external_dicts_dict_sources.md)”.

Haga clic en Casa:

-   Almacena total o parcialmente los diccionarios en RAM.
-   Actualiza periódicamente los diccionarios y carga dinámicamente los valores que faltan. En otras palabras, los diccionarios se pueden cargar dinámicamente.
-   Permite crear diccionarios externos con archivos xml o [Consultas DDL](../create.md#create-dictionary-query).

La configuración de diccionarios externos se puede ubicar en uno o más archivos xml. La ruta de acceso a la configuración se especifica en el [Diccionarios\_config](../../operations/server_settings/settings.md#server_settings-dictionaries_config) parámetro.

Los diccionarios se pueden cargar en el inicio del servidor o en el primer uso, dependiendo de la [Diccionarios\_lazy\_load](../../operations/server_settings/settings.md#server_settings-dictionaries_lazy_load) configuración.

El archivo de configuración del diccionario tiene el siguiente formato:

``` xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

Usted puede [configurar](external_dicts_dict.md) cualquier número de diccionarios en el mismo archivo.

[Consultas DDL para diccionarios](../create.md#create-dictionary-query) no requiere ningún registro adicional en la configuración del servidor. Permiten trabajar con diccionarios como entidades de primera clase, como tablas o vistas.

!!! attention "Atención"
    Puede convertir valores para un diccionario pequeño describiéndolo en un `SELECT` Consulta (ver el [Ciudad](../functions/other_functions.md) función). Esta funcionalidad no está relacionada con diccionarios externos.

## Ver también {#ext-dicts-see-also}

-   [Configuración de un diccionario externo](external_dicts_dict.md)
-   [Almacenamiento de diccionarios en la memoria](external_dicts_dict_layout.md)
-   [Actualizaciones del diccionario](external_dicts_dict_lifetime.md)
-   [Fuentes de diccionarios externos](external_dicts_dict_sources.md)
-   [Clave y campos del diccionario](external_dicts_dict_structure.md)
-   [Funciones para trabajar con diccionarios externos](../functions/ext_dict_functions.md)

[Artículo Original](https://clickhouse.tech/docs/es/query_language/dicts/external_dicts/) <!--hide-->
