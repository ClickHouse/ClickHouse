---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Prueba de hardware
---

# Cómo probar su hardware con ClickHouse {#how-to-test-your-hardware-with-clickhouse}

Con esta instrucción, puede ejecutar una prueba de rendimiento básica de ClickHouse en cualquier servidor sin instalar paquetes de ClickHouse.

1.  Ir a “commits” página: https://github.com/ClickHouse/ClickHouse/commits/master

2.  Haga clic en la primera marca de verificación verde o cruz roja con verde “ClickHouse Build Check” y haga clic en el “Details” enlace cerca “ClickHouse Build Check”. No existe tal enlace en algunas confirmaciones, por ejemplo, confirmaciones con documentación. En este caso, elija la confirmación más cercana que tenga este enlace.

3.  Copie el enlace a “clickhouse” binario para amd64 o aarch64.

4.  ssh al servidor y descargarlo con wget:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  Descargar configs:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  Descargar archivos de referencia:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  Descargue los datos de prueba de acuerdo con el [El Yandex.Conjunto de datos de Metrica](../getting-started/example-datasets/metrica.md) instrucción (“hits” tabla que contiene 100 millones de filas).

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  Ejecute el servidor:

<!-- -->

      ./clickhouse server

1.  Verifique los datos: ssh al servidor en otro terminal

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  Edite el benchmark-new.sh, cambie `clickhouse-client` a `./clickhouse client` y añadir `–-max_memory_usage 100000000000` parámetro.

<!-- -->

      mcedit benchmark-new.sh

1.  Ejecute el punto de referencia:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  Envíe los números y la información sobre la configuración de su hardware a clickhouse-feedback@yandex-team.com

Todos los resultados se publican aquí: https://clickhouse.tecnología/punto de referencia/hardware/
