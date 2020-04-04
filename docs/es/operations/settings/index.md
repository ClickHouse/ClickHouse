---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_folder_title: Settings
toc_priority: 55
toc_title: "Implantaci\xF3n"
---

# Configuración {#settings}

Hay varias formas de realizar todos los ajustes que se describen a continuación.
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
