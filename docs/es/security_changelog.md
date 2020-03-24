## Corregido en la versión de ClickHouse 19.14.3.3, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### ¿Qué puedes encontrar en Neodigit {#cve-2019-15024}

Un atacante que tenga acceso de escritura a ZooKeeper y que pueda ejecutar un servidor personalizado disponible desde la red donde se ejecuta ClickHouse, puede crear un servidor malicioso personalizado que actuará como una réplica de ClickHouse y lo registrará en ZooKeeper. Cuando otra réplica recuperará la parte de datos de la réplica maliciosa, puede forzar a clickhouse-server a escribir en una ruta arbitraria en el sistema de archivos.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

### ¿Qué puedes encontrar en Neodigit {#cve-2019-16535}

Аn La lectura OOB, la escritura OOB y el desbordamiento de enteros en los algoritmos de descompresión se pueden usar para lograr RCE o DoS a través del protocolo nativo.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

### ¿Qué puedes encontrar en Neodigit {#cve-2019-16536}

Un cliente autenticado malintencionado puede desencadenar el desbordamiento de pila que conduce a DoS.

Créditos: Eldar Zaitov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 19.13.6.1, 2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### ¿Qué puedes encontrar en Neodigit {#cve-2019-18657}

Función de la tabla `url` la vulnerabilidad permitió al atacante inyectar encabezados HTTP arbitrarios en la solicitud.

Crédito: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## Corregido en la versión de ClickHouse 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### ¿Qué puedes encontrar en Neodigit {#cve-2018-14672}

Las funciones para cargar modelos CatBoost permitieron el recorrido de ruta y la lectura de archivos arbitrarios a través de mensajes de error.

Créditos: Andrey Krasichkov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 18.10.3, 2018-08-13 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### ¿Qué puedes encontrar en Neodigit {#cve-2018-14671}

unixODBC permitía cargar objetos compartidos arbitrarios desde el sistema de archivos, lo que provocó una vulnerabilidad de ejecución remota de código.

Créditos: Andrey Krasichkov y Evgeny Sidorov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54388, 2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### ¿Qué puedes encontrar en Neodigit {#cve-2018-14668}

“remote” función de tabla permitió símbolos arbitrarios en “user”, “password” y “default\_database” campos que llevaron a ataques de falsificación de solicitudes de protocolo cruzado.

Créditos: Andrey Krasichkov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54390, 2018-07-06 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### ¿Qué puedes encontrar en Neodigit {#cve-2018-14669}

ClickHouse cliente MySQL tenía “LOAD DATA LOCAL INFILE” funcionalidad habilitada que permitió a una base de datos MySQL maliciosa leer archivos arbitrarios desde el servidor ClickHouse conectado.

Créditos: Andrey Krasichkov y Evgeny Sidorov del equipo de seguridad de la información de Yandex

## Corregido en la versión de ClickHouse 1.1.54131, 2017-01-10 {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### ¿Qué puedes encontrar en Neodigit {#cve-2018-14670}

Una configuración incorrecta en el paquete deb podría conducir al uso no autorizado de la base de datos.

Créditos: Centro Nacional de Seguridad Cibernética del Reino Unido (NCSC)

[Artículo Original](https://clickhouse.tech/docs/es/security_changelog/) <!--hide-->
