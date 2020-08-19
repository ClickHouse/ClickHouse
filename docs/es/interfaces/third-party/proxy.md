---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 29
toc_title: Proxy
---

# Servidores proxy de desarrolladores de terceros {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy), es un proxy HTTP y equilibrador de carga para la base de datos ClickHouse.

Función:

-   Enrutamiento por usuario y almacenamiento en caché de respuestas.
-   Flexible límites.
-   Renovación automática del certificado SSL.

Implementado en Go.

## Bienvenido a WordPress {#kittenhouse}

[Bienvenido a WordPress.](https://github.com/VKCOM/kittenhouse) está diseñado para ser un proxy local entre ClickHouse y el servidor de aplicaciones en caso de que sea imposible o inconveniente almacenar los datos INSERT en el lado de su aplicación.

Función:

-   Almacenamiento en búfer de datos en memoria y en disco.
-   Enrutamiento por tabla.
-   Equilibrio de carga y comprobación de estado.

Implementado en Go.

## Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica {#clickhouse-bulk}

[Bienvenidos al Portal de LicitaciÃ³n ElectrÃ³nica de LicitaciÃ³n ElectrÃ³nica](https://github.com/nikepan/clickhouse-bulk) es un simple colector de insertos ClickHouse.

Función:

-   Agrupe las solicitudes y envíe por umbral o intervalo.
-   Múltiples servidores remotos.
-   Autenticación básica.

Implementado en Go.

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/third-party/proxy/) <!--hide-->
