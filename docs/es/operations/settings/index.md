---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Configuraci\xF3n"
toc_priority: 55
toc_title: "Implantaci\xF3n"
---

# Configuración {#session-settings-intro}

Hay varias maneras de realizar todos los ajustes descritos en esta sección de documentación.

Los ajustes se configuran en capas, por lo que cada capa subsiguiente redefine los ajustes anteriores.

Formas de configurar los ajustes, por orden de prioridad:

-   Ajustes en el `users.xml` archivo de configuración del servidor.

    Establecer en el elemento `<profiles>`.

-   Configuración de la sesión.

    Enviar `SET setting=value` desde el cliente de consola ClickHouse en modo interactivo.
    Del mismo modo, puede utilizar sesiones ClickHouse en el protocolo HTTP. Para hacer esto, debe especificar el `session_id` Parámetro HTTP.

-   Configuración de consulta.

    -   Al iniciar el cliente de consola de ClickHouse en modo no interactivo, establezca el parámetro de inicio `--setting=value`.
    -   Al usar la API HTTP, pase los parámetros CGI (`URL?setting_1=value&setting_2=value...`).

Los ajustes que solo se pueden realizar en el archivo de configuración del servidor no se tratan en esta sección.

[Artículo Original](https://clickhouse.tech/docs/en/operations/settings/) <!--hide-->
