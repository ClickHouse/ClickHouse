---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 76
toc_title: Seguridad Changelog
---

## Corregido en la versión de ClickHouse 19.14.3.3, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

Аn attacker that has write access to ZooKeeper and who ican run a custom server available from the network where ClickHouse runs, can create a custom-built malicious server that will act as a ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from the malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

### CVE-2019-16535 {#cve-2019-16535}

Аn OOB read, OOB write and integer underflow in decompression algorithms can be used to achieve RCE or DoS via native protocol.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

### CVE-2019-16536 {#cve-2019-16536}

Un cliente autenticado malintencionado puede desencadenar el desbordamiento de pila que conduce a DoS.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 19.13.6.1, 2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

Función de la tabla `url` la vulnerabilidad permitió al atacante inyectar encabezados HTTP arbitrarios en la solicitud.

Crédito: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## Corregido en la versión de ClickHouse 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

Las funciones para cargar modelos CatBoost permitieron el recorrido de ruta y la lectura de archivos arbitrarios a través de mensajes de error.

Créditos: Andrey Krasichkov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 18.10.3, 2018-08-13 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC permitía cargar objetos compartidos arbitrarios desde el sistema de archivos, lo que provocó una vulnerabilidad de ejecución remota de código.

Créditos: Andrey Krasichkov y Evgeny Sidorov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54388, 2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

“remote” función de tabla permitió símbolos arbitrarios en “user”, “password” y “default_database” campos que llevaron a ataques de falsificación de solicitudes de protocolo cruzado.

Créditos: Andrey Krasichkov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54390, 2018-07-06 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

ClickHouse cliente MySQL tenía “LOAD DATA LOCAL INFILE” funcionalidad habilitada que permitió a una base de datos MySQL maliciosa leer archivos arbitrarios desde el servidor ClickHouse conectado.

Créditos: Andrey Krasichkov y Evgeny Sidorov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54131, 2017-01-10 {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

Una configuración incorrecta en el paquete deb podría conducir al uso no autorizado de la base de datos.

Créditos: Centro Nacional de Seguridad Cibernética del Reino Unido (NCSC)

{## [Artículo Original](https://clickhouse.tech/docs/en/security_changelog/) ##}
