---
description: 'Esta página explica cómo se puede configurar ClickHouse server con archivos
  de configuración en sintaxis XML o YAML.'
sidebar_label: 'Archivos de configuración'
sidebar_position: 50
slug: /operations/configuration-files
title: 'Archivos de configuración'
doc_type: 'guide'
---

:::note
Los perfiles de configuración basados en XML y los archivos de configuración no son compatibles con ClickHouse Cloud. Por lo tanto, en ClickHouse Cloud no encontrarás un archivo `config.xml`. En su lugar, debes usar comandos SQL para gestionar la configuración mediante perfiles de configuración.

Para obtener más información, consulta [&quot;Configuración de ajustes&quot;](/es/manage/settings)
:::

ClickHouse server se puede configurar con archivos de configuración en sintaxis XML o YAML.
En la mayoría de los tipos de instalación, ClickHouse server se ejecuta con `/etc/clickhouse-server/config.xml` como archivo de configuración predeterminado, pero también es posible especificar manualmente la ubicación del archivo de configuración al iniciar el servidor mediante la opción de línea de comandos `--config-file` o `-C`.
Los archivos de configuración adicionales pueden colocarse en el directorio `config.d/`, relativo al archivo de configuración principal; por ejemplo, en el directorio `/etc/clickhouse-server/config.d/`.
Los archivos de este directorio y la configuración principal se fusionan en una etapa de preprocesamiento antes de que la configuración se aplique en ClickHouse server.
Los archivos de configuración se fusionan en orden alfabético.
Para simplificar las actualizaciones y mejorar la modularidad, una práctica recomendada es mantener sin modificar el archivo predeterminado `config.xml` y colocar la personalización adicional en `config.d/`.
La configuración de ClickHouse Keeper se encuentra en `/etc/clickhouse-keeper/keeper_config.xml`.
Del mismo modo, los archivos de configuración adicionales de Keeper deben colocarse en `/etc/clickhouse-keeper/keeper_config.d/`.

Es posible mezclar archivos de configuración XML y YAML; por ejemplo, podrías tener un archivo de configuración principal `config.xml` y archivos de configuración adicionales `config.d/network.xml`, `config.d/timezone.yaml` y `config.d/keeper.yaml`.
No se admite mezclar XML y YAML dentro de un mismo archivo de configuración.
Los archivos de configuración XML deben usar `<clickhouse>...</clickhouse>` como etiqueta de nivel superior.
En los archivos de configuración YAML, `clickhouse:` es opcional; si no está presente, el analizador lo inserta automáticamente.

<div id="merging">
  ## Combinación de archivos de configuración
</div>

Dos archivos de configuración (normalmente, el archivo de configuración principal y otro archivo de configuración de `config.d/`) se combinan de la siguiente manera:

* Si un nodo (es decir, una ruta que conduce a un elemento) aparece en ambos archivos y no tiene los atributos `replace` ni `remove`, se incluye en el archivo de configuración combinado, y los nodos hijo de ambos nodos se incluyen y se combinan de forma recursiva.
* Si uno de los dos nodos contiene el atributo `replace`, se incluye en el archivo de configuración combinado, pero solo se incluyen los nodos hijo del nodo que tiene el atributo `replace`.
* Si uno de los dos nodos contiene el atributo `remove`, el nodo no se incluye en el archivo de configuración combinado (si ya existe, se elimina).

Por ejemplo, dados dos archivos de configuración:

```xml title="config.xml"
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
    </config_a>
    <config_b>
        <setting_2>2</setting_2>
    </config_b>
    <config_c>
        <setting_3>3</setting_3>
    </config_c>
</clickhouse>
```

y

```xml title="config.d/other_config.xml"
<clickhouse>
    <config_a>
        <setting_4>4</setting_4>
    </config_a>
    <config_b replace="replace">
        <setting_5>5</setting_5>
    </config_b>
    <config_c remove="remove">
        <setting_6>6</setting_6>
    </config_c>
</clickhouse>
```

El archivo de configuración resultante de la combinación será:

```xml
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
        <setting_4>4</setting_4>
    </config_a>
    <config_b>
        <setting_5>5</setting_5>
    </config_b>
</clickhouse>
```

<div id="from_env_zk">
  ### Sustitución mediante variables de entorno y nodos de ZooKeeper
</div>

Para indicar que el valor de un elemento debe sustituirse por el de una variable de entorno, puede usar el atributo `from_env`.

Por ejemplo, con la variable de entorno `$MAX_QUERY_SIZE = 150000`:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

La configuración resultante será:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

Lo mismo puede hacerse usando `from_zk` (nodo de ZooKeeper):

```xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```shell
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

Lo que da como resultado la siguiente configuración:

```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

<div id="default-values">
  #### Valores predeterminados
</div>

Un elemento con los atributos `from_env` o `from_zk` también puede tener el atributo `replace="1"` (este último debe aparecer antes de `from_env`/`from_zk`).
En ese caso, el elemento puede definir un valor predeterminado.
El elemento toma el valor de la variable de entorno o del nodo de ZooKeeper si está definido; de lo contrario, toma el valor predeterminado.

Se repite el ejemplo anterior, pero suponiendo que `MAX_QUERY_SIZE` no está definido:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

El resultado es la siguiente configuración:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

<div id="substitution-with-file-content">
  ## Sustitución con contenido de archivos
</div>

También es posible sustituir partes de la configuración con el contenido de archivos. Esto se puede hacer de dos maneras:

* *Sustitución de valores*: Si un elemento tiene el atributo `incl`, su valor se sustituirá por el contenido del archivo referenciado. De forma predeterminada, la ruta al archivo con sustituciones es `/etc/metrika.xml`. Esto se puede cambiar en el elemento [`include_from`](../operations/server-configuration-parameters/settings.md#include_from) de la configuración del server. Los valores de sustitución se especifican en elementos `/clickhouse/substitution_name` dentro de este archivo. Si una sustitución especificada en `incl` no existe, se registra en el log. Para evitar que ClickHouse registre sustituciones faltantes, especifique el atributo `optional="true"` (por ejemplo, la configuración de [macros](../operations/server-configuration-parameters/settings.md#macros)).
* *Sustitución de elementos*: Si desea sustituir el elemento completo por una sustitución, use `include` como nombre del elemento. El nombre de elemento `include` se puede combinar con el atributo `from_zk = "/path/to/node"`. En este caso, el valor del elemento se sustituye por el contenido del nodo de ZooKeeper en `/path/to/node`. Esto también funciona si almacena un subárbol XML completo como un nodo de ZooKeeper; se insertará por completo en el elemento de origen.

A continuación se muestra un ejemplo de esto:

```xml
<clickhouse>
    <!-- Appends XML subtree found at `/profiles-in-zookeeper` ZK path to `<profiles>` element. -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- Replaces `include` element with the subtree found at `/users-in-zookeeper` ZK path. -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

Si quieres fusionar el contenido sustituido con la configuración existente en lugar de añadirlo, puedes usar el atributo `merge="true"`. Por ejemplo: `<include from_zk="/some_path" merge="true">`. En este caso, la configuración existente se fusionará con el contenido de la sustitución y los ajustes de configuración existentes se sustituirán por los valores de la sustitución.

<div id="encryption">
  ## Cifrado y ocultación de la configuración
</div>

Puede utilizar cifrado simétrico para cifrar un elemento de configuración, por ejemplo, una contraseña en texto plano o una clave privada.
Para ello, primero configure el [códec de cifrado](../sql-reference/statements/create/table.md#encryption-codecs) y, a continuación, añada al elemento que desea cifrar el atributo `encrypted_by`, con el nombre del códec de cifrado como valor.

A diferencia de los atributos `from_zk`, `from_env` e `incl`, o del elemento `include`, en el archivo preprocesado no se realiza ninguna sustitución (es decir, el descifrado del valor cifrado).
El descifrado solo ocurre en tiempo de ejecución, en el proceso del servidor.

Por ejemplo:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>00112233445566778899aabbccddeeff</key_hex>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Los atributos [`from_env`](#from_env_zk) y [`from_zk`](#from_env_zk) también pueden aplicarse a `encryption_codecs`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_env="CLICKHOUSE_KEY_HEX"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Las claves de cifrado y los valores cifrados pueden definirse en cualquiera de los dos archivos de configuración.

A continuación se muestra un ejemplo de `config.xml`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

A continuación se muestra un ejemplo de `users.xml`:

```xml
<clickhouse>

    <users>
        <test_user>
            <password encrypted_by="AES_128_GCM_SIV">96280000000D000000000030D4632962295D46C6FA4ABF007CCEC9C1D0E19DA5AF719C1D9A46C446</password>
            <profile>default</profile>
        </test_user>
    </users>

</clickhouse>
```

Para cifrar un valor, puede usar el programa (de ejemplo) `encrypt_decrypt`:

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

Incluso con elementos de configuración cifrados, estos siguen apareciendo en el archivo de configuración preprocesado.
Si esto supone un problema para su implementación de ClickHouse, hay dos alternativas: establecer los permisos del archivo preprocesado en 600 o usar el atributo `hide_in_preprocessed`.

Por ejemplo:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

<div id="user-settings">
  ## Ajustes de usuario
</div>

El archivo `config.xml` puede especificar una configuración independiente con ajustes de usuario, perfiles y cuotas. La ruta relativa a esta configuración se establece en el elemento `users_config`. De forma predeterminada, es `users.xml`. Si se omite `users_config`, los ajustes de usuario, perfiles y cuotas se especifican directamente en `config.xml`.

La configuración de usuario puede dividirse en archivos separados, de forma similar a `config.xml` y `config.d/`.
El nombre del directorio se define como el valor de `users_config` sin el sufijo `.xml`, concatenado con `.d`.
De forma predeterminada, se usa el directorio `users.d`, ya que `users_config` tiene como valor predeterminado `users.xml`.

Tenga en cuenta que los archivos de configuración primero se [combinan](#merging) teniendo en cuenta los ajustes, y después se procesan las inclusiones.

<div id="example">
  ## Ejemplo en XML
</div>

Por ejemplo, puede tener un archivo de configuración independiente para cada usuario de esta forma:

```bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

```xml
<clickhouse>
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
</clickhouse>
```

<div id="example-1">
  ## Ejemplos de YAML
</div>

Aquí puedes ver la configuración predeterminada escrita en YAML: [`config.yaml.example`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

Hay algunas diferencias entre los formatos YAML y XML en lo que respecta a la configuración de ClickHouse.
A continuación, se presentan algunos consejos para escribir la configuración en formato YAML.

Una etiqueta XML con un valor de texto se representa mediante un par clave-valor en YAML

```yaml
key: value
```

XML correspondiente:

```xml
<key>value</key>
```

Un nodo XML anidado se representa mediante un mapa YAML:

```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

XML correspondiente:

```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

Para crear varias veces la misma etiqueta XML, use una secuencia YAML:

```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

XML correspondiente:

```xml
<seq_key>val1</seq_key>
<seq_key>val2</seq_key>
<seq_key>
    <key1>val3</key1>
</seq_key>
<seq_key>
    <map>
        <key2>val4</key2>
        <key3>val5</key3>
    </map>
</seq_key>
```

Para especificar un atributo XML, puedes usar una clave de atributo con el prefijo `@`. Ten en cuenta que `@` está reservado por el estándar YAML, por lo que debe ir entre comillas dobles:

```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

XML correspondiente:

```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

También se pueden usar atributos en una secuencia de YAML:

```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

El XML correspondiente:

```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

La sintaxis mencionada anteriormente no permite expresar como YAML nodos de texto XML con atributos XML. Este caso especial puede lograrse usando una
clave de atributo `#text`:

```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

XML correspondiente:

```xml
<map_key attr1="value1">value2</map>
```

<div id="implementation-details">
  ## Detalles de implementación
</div>

Para cada archivo de configuración, el servidor también genera archivos `file-preprocessed.xml` al iniciarse. Estos archivos contienen todas las sustituciones y sobrescrituras ya aplicadas, y están pensados únicamente con fines informativos. Si se usaron sustituciones de ZooKeeper en los archivos de configuración, pero ZooKeeper no está disponible al iniciar el servidor, este carga la configuración desde el archivo preprocesado.

El servidor supervisa los cambios en los archivos de configuración, así como en los archivos y nodos de ZooKeeper que se utilizaron al realizar sustituciones y sobrescrituras, y recarga sobre la marcha la configuración de usuarios y clústeres. Esto significa que puede modificar el clúster, los usuarios y su configuración sin reiniciar el servidor.