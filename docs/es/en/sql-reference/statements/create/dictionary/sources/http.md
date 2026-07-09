---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'Fuente HTTP(S) de diccionario'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'Configure un endpoint HTTP o HTTPS como fuente de diccionario en ClickHouse.'
doc_type: 'referencia'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

El trabajo con un servidor HTTP(S) depende de [cómo se almacena el diccionario en memoria](../layouts/). Si el diccionario se almacena con `cache` y `complex_key_cache`, ClickHouse solicita las claves necesarias enviando una solicitud mediante el método `POST`.

Ejemplo de configuración:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Para que ClickHouse pueda acceder a un recurso HTTPS, debe [configurar OpenSSL](/es/operations/server-configuration-parameters/settings#openssl) en la configuración del servidor.

Campos de configuración:

| Configuración | Descripción                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------ |
| `url`         | La URL de origen.                                                                                      |
| `format`      | El formato del archivo. Se admiten todos los formatos descritos en [Formatos](/es/sql-reference/formats). |
| `credentials` | Autenticación HTTP básica. Opcional.                                                                   |
| `user`        | Nombre de usuario requerido para la autenticación.                                                     |
| `password`    | Contraseña requerida para la autenticación.                                                            |
| `headers`     | Todas las entradas de cabeceras HTTP personalizadas usadas en la solicitud HTTP. Opcional.             |
| `header`      | Una única entrada de cabecera HTTP.                                                                    |
| `name`        | Nombre del identificador usado para la cabecera enviada en la solicitud.                               |
| `value`       | Valor establecido para un nombre de identificador específico.                                          |

Al crear un diccionario con el comando DDL (`CREATE DICTIONARY ...`), los hosts remotos de los diccionarios HTTP se comprueban con el contenido de la sección `remote_url_allow_hosts` de la configuración para evitar que los usuarios de base de datos accedan a servidores HTTP arbitrarios.