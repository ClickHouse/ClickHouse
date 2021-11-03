---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 14
toc_title: Infantil
---

# Zona de juegos ClickHouse {#clickhouse-playground}

[Zona de juegos ClickHouse](https://play.clickhouse.tech?file=welcome) permite a las personas experimentar con ClickHouse ejecutando consultas al instante, sin configurar su servidor o clúster.
Varios conjuntos de datos de ejemplo están disponibles en Playground, así como consultas de ejemplo que muestran las características de ClickHouse.

Las consultas se ejecutan como un usuario de sólo lectura. Implica algunas limitaciones:

-   No se permiten consultas DDL
-   Las consultas INSERT no están permitidas

También se aplican los siguientes valores:
- [`max_result_bytes=10485760`](../operations/settings/query_complexity/#max-result-bytes)
- [`max_result_rows=2000`](../operations/settings/query_complexity/#setting-max_result_rows)
- [`result_overflow_mode=break`](../operations/settings/query_complexity/#result-overflow-mode)
- [`max_execution_time=60000`](../operations/settings/query_complexity/#max-execution-time)

ClickHouse Playground da la experiencia de m2.pequeño
[Servicio administrado para ClickHouse](https://cloud.yandex.com/services/managed-clickhouse)
instancia alojada en [El Yandex.Nube](https://cloud.yandex.com/).
Más información sobre [proveedores de la nube](../commercial/cloud.md).

La interfaz web de ClickHouse Playground realiza solicitudes a través de ClickHouse [HTTP API](../interfaces/http.md).
El backend Playground es solo un clúster ClickHouse sin ninguna aplicación adicional del lado del servidor.
El punto final HTTPS de ClickHouse también está disponible como parte de Playground.

Puede realizar consultas al patio de recreo utilizando cualquier cliente HTTP, por ejemplo [rizo](https://curl.haxx.se) o [wget](https://www.gnu.org/software/wget/), o configurar una conexión usando [JDBC](../interfaces/jdbc.md) o [ODBC](../interfaces/odbc.md) controlador.
Más información sobre los productos de software compatibles con ClickHouse está disponible [aqui](../interfaces/index.md).

| Parámetro   | Valor                                         |
|:------------|:----------------------------------------------|
| Punto final | https://play-api.casa de clic.tecnología:8443 |
| Usuario     | `playground`                                  |
| Contraseña  | `clickhouse`                                  |

Tenga en cuenta que este extremo requiere una conexión segura.

Ejemplo:

``` bash
curl "https://play-api.clickhouse.tech:8443/?query=SELECT+'Play+ClickHouse!';&user=playground&password=clickhouse&database=datasets"
```
