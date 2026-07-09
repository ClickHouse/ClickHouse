---
description: 'Documentación sobre la actualización'
sidebar_title: 'Actualización autogestionada'
slug: /operations/update
title: 'Actualización autogestionada'
doc_type: 'guide'
---

<div id="clickhouse-upgrade-overview">
  ## Resumen de la actualización de ClickHouse
</div>

Este documento contiene:

* directrices generales
* un plan recomendado
* detalles específicos para actualizar los binarios en sus sistemas

<div id="general-guidelines">
  ## Pautas generales
</div>

Estas notas deberían ayudarle a planificar y a entender por qué hacemos las recomendaciones que presentamos más adelante en el documento.

<div id="upgrade-clickhouse-server-separately-from-clickhouse-keeper-or-zookeeper">
  ### Actualice servidor de ClickHouse por separado de ClickHouse Keeper o ZooKeeper
</div>

A menos que sea necesaria una corrección de seguridad para ClickHouse Keeper o Apache ZooKeeper, no es necesario actualizar Keeper cuando actualice servidor de ClickHouse. La estabilidad de Keeper es fundamental durante el proceso de actualización, así que complete primero las actualizaciones de servidor de ClickHouse antes de plantearse actualizar Keeper.

<div id="minor-version-upgrades-should-be-adopted-often">
  ### Las actualizaciones de versiones menores deberían aplicarse con frecuencia
</div>

Se recomienda encarecidamente actualizar siempre a la versión menor más reciente en cuanto esté disponible. Las versiones menores no introducen cambios incompatibles, pero sí incluyen correcciones de errores importantes (y pueden incluir correcciones de seguridad).

<div id="test-experimental-features-on-a-separate-clickhouse-server-running-the-target-version">
  ### Pruebe las funciones experimentales en un servidor de ClickHouse independiente que ejecute la versión de destino
</div>

La compatibilidad de las funciones experimentales puede verse afectada en cualquier momento y de cualquier forma. Si utiliza funciones experimentales, consulte los changelogs y considere configurar un servidor de ClickHouse independiente con la versión de destino instalada para probar allí su uso de estas funciones experimentales.

<div id="downgrades">
  ### Reversiones de versión
</div>

Si actualiza y luego se da cuenta de que la nueva versión no es compatible con alguna funcionalidad de la que depende, es posible que pueda volver a una versión reciente (con menos de un año de antigüedad) si todavía no ha empezado a usar ninguna de las nuevas funcionalidades. Una vez que se utilicen las nuevas funcionalidades, la reversión de versión no funcionará.

<div id="multiple-clickhouse-server-versions-in-a-cluster">
  ### Varias versiones del servidor de ClickHouse en un clúster
</div>

Nos esforzamos por mantener una ventana de compatibilidad de un año (que incluye 2 versiones LTS). Esto significa que dos versiones cualesquiera deberían poder funcionar juntas en un clúster si la diferencia entre ellas es inferior a un año (o si hay menos de dos versiones LTS entre ellas). Sin embargo, se recomienda actualizar todos los miembros de un clúster a la misma versión lo antes posible, ya que pueden surgir algunos problemas menores (como una ralentización de las consultas distribuidas, errores reintentables en algunas operaciones en segundo plano de ReplicatedMergeTree, etc.).

Nunca recomendamos ejecutar versiones distintas en el mismo clúster cuando sus fechas de lanzamiento difieren en más de un año. Aunque no esperamos que se produzca pérdida de datos, el clúster puede quedar inutilizable. Los problemas que cabe esperar si hay más de un año de diferencia entre versiones incluyen:

* es posible que el clúster no funcione
* algunas consultas (o incluso todas) pueden fallar con errores arbitrarios
* pueden aparecer errores/advertencias arbitrarios en los logs
* puede ser imposible volver a una versión anterior

<div id="incremental-upgrades">
  ### Actualizaciones incrementales
</div>

Si la diferencia entre la versión actual y la versión de destino es superior a un año, se recomienda una de estas opciones:

* Actualizar con tiempo de inactividad (detener todos los servidores, actualizar todos los servidores e iniciar todos los servidores).
* O actualizar mediante una versión intermedia (una versión con menos de un año de antigüedad respecto de la versión actual).

<div id="recommended-plan">
  ## Plan recomendado
</div>

Estos son los pasos recomendados para una actualización de ClickHouse sin tiempo de inactividad:

1. Asegúrese de que los cambios de configuración no estén en el archivo predeterminado `/etc/clickhouse-server/config.xml`, sino en `/etc/clickhouse-server/config.d/`, ya que `/etc/clickhouse-server/config.xml` podría sobrescribirse durante una actualización.
2. Revise los [registros de cambios](/es/whats-new/changelog/index.md) para identificar cambios incompatibles (desde la versión de destino hasta la versión que usa actualmente).
3. Realice antes de la actualización todos los cambios identificados en los cambios incompatibles que puedan aplicarse con antelación, y haga una lista de los cambios que deberán realizarse después de la actualización.
4. Identifique una o más réplicas de cada segmento para mantenerlas en funcionamiento mientras se actualiza el resto de las réplicas de cada segmento.
5. En las réplicas que se actualizarán, una a la vez:

* detenga servidor de ClickHouse
* actualice el servidor a la versión de destino
* vuelva a iniciar servidor de ClickHouse
* espere a que los mensajes de Keeper indiquen que el sistema está estable
* continúe con la siguiente réplica6. Compruebe si hay errores en el log de Keeper y en el log de ClickHouse

7. Actualice a la nueva versión las réplicas identificadas en el paso 4
8. Consulte la lista de cambios de los pasos 1 a 3 y realice los cambios que deban hacerse después de la actualización.

:::note
Es normal ver este mensaje de error cuando hay varias versiones de ClickHouse ejecutándose en un entorno replicado. Dejará de aparecer cuando todas las réplicas se hayan actualizado a la misma versión.

```text
MergeFromLogEntryTask: Code: 40. DB::Exception: Checksums of parts don't match:
hash of uncompressed files doesn't match. (CHECKSUM_DOESNT_MATCH)  Data after merge is not
byte-identical to data on another replicas.
```

:::

<div id="clickhouse-server-binary-upgrade-process">
  ## Proceso de actualización del binario del servidor de ClickHouse
</div>

Si ClickHouse se instaló a partir de paquetes `deb`, ejecute los siguientes comandos en el servidor:

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Si instaló ClickHouse con un método distinto de los paquetes `deb` recomendados, use el método de actualización correspondiente.

:::note
Puede actualizar varios servidores a la vez, siempre que no haya ningún momento en que todas las réplicas de un segmento estén fuera de línea.
:::

Actualización de una versión anterior de ClickHouse a una versión específica:

Por ejemplo:

`xx.yy.a.b` corresponde a la versión estable actual. La versión estable más reciente puede encontrarse [aquí](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```