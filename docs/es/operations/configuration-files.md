---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: "Archivos de configuraci\xF3n"
---

# Archivos de configuración {#configuration_files}

ClickHouse admite la administración de configuración de varios archivos. El archivo de configuración del servidor principal es `/etc/clickhouse-server/config.xml`. Otros archivos deben estar en el `/etc/clickhouse-server/config.d` directorio.

!!! note "Nota"
    Todos los archivos de configuración deben estar en formato XML. Además, deben tener el mismo elemento raíz, generalmente `<yandex>`.

Algunos valores especificados en el archivo de configuración principal se pueden anular en otros archivos de configuración. El `replace` o `remove` se pueden especificar atributos para los elementos de estos archivos de configuración.

Si no se especifica ninguno, combina el contenido de los elementos de forma recursiva, reemplazando los valores de los elementos secundarios duplicados.

Si `replace` se especifica, reemplaza todo el elemento por el especificado.

Si `remove` se especifica, elimina el elemento.

La configuración también puede definir “substitutions”. Si un elemento tiene el `incl` atributo, la sustitución correspondiente del archivo se utilizará como el valor. De forma predeterminada, la ruta al archivo con sustituciones es `/etc/metrika.xml`. Esto se puede cambiar en el [include_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) elemento en la configuración del servidor. Los valores de sustitución se especifican en `/yandex/substitution_name` elementos en este archivo. Si una sustitución especificada en `incl` no existe, se registra en el registro. Para evitar que ClickHouse registre las sustituciones que faltan, especifique `optional="true"` atributo (por ejemplo, ajustes para [macro](server-configuration-parameters/settings.md)).

Las sustituciones también se pueden realizar desde ZooKeeper. Para hacer esto, especifique el atributo `from_zk = "/path/to/node"`. El valor del elemento se sustituye por el contenido del nodo en `/path/to/node` en ZooKeeper. También puede colocar un subárbol XML completo en el nodo ZooKeeper y se insertará completamente en el elemento de origen.

El `config.xml` file puede especificar una configuración separada con configuraciones de usuario, perfiles y cuotas. La ruta relativa a esta configuración se establece en el `users_config` elemento. Por defecto, es `users.xml`. Si `users_config` se omite, la configuración de usuario, los perfiles y las cuotas se especifican directamente en `config.xml`.

La configuración de los usuarios se puede dividir en archivos separados similares a `config.xml` y `config.d/`.
El nombre del directorio se define como `users_config` sin `.xml` postfix concatenado con `.d`.
Directorio `users.d` se utiliza por defecto, como `users_config` por defecto `users.xml`.
Por ejemplo, puede tener un archivo de configuración separado para cada usuario como este:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<yandex>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</yandex>
```

Para cada archivo de configuración, el servidor también genera `file-preprocessed.xml` archivos al iniciar. Estos archivos contienen todas las sustituciones y anulaciones completadas, y están destinados para uso informativo. Si se utilizaron sustituciones de ZooKeeper en los archivos de configuración pero ZooKeeper no está disponible en el inicio del servidor, el servidor carga la configuración desde el archivo preprocesado.

El servidor realiza un seguimiento de los cambios en los archivos de configuración, así como archivos y nodos ZooKeeper que se utilizaron al realizar sustituciones y anulaciones, y vuelve a cargar la configuración de los usuarios y clústeres sobre la marcha. Esto significa que puede modificar el clúster, los usuarios y su configuración sin reiniciar el servidor.

[Artículo Original](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
