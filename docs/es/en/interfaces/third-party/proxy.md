---
description: 'Describe las soluciones proxy de terceros disponibles para ClickHouse'
sidebar_label: 'Proxies'
sidebar_position: 29
slug: /interfaces/third-party/proxy
title: 'Servidores proxy de terceros'
doc_type: 'reference'
---

<div id="chproxy">
  ## chproxy
</div>

[chproxy](https://github.com/Vertamedia/chproxy) es un proxy HTTP y un balanceador de carga para la base de datos ClickHouse.

Características:

* Enrutamiento por usuario y almacenamiento en caché de respuestas.
* Límites flexibles.
* Renovación automática de certificados SSL.

Implementado en Go.

<div id="kittenhouse">
  ## KittenHouse
</div>

[KittenHouse](https://github.com/VKCOM/kittenhouse) está diseñado para actuar como proxy local entre ClickHouse y el servidor de aplicaciones cuando es imposible o poco práctico almacenar en búfer los datos de INSERT del lado de la aplicación.

Características:

* Almacenamiento en búfer de datos en memoria y en disco.
* Enrutamiento por tabla.
* Balanceo de carga y comprobaciones de estado.

Implementado en Go.

<div id="clickhouse-bulk">
  ## ClickHouse-Bulk
</div>

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) es un collector sencillo para insert en ClickHouse.

Características:

* Agrupa las solicitudes y las envía según un umbral o un intervalo.
* Varios servidores remotos.
* Autenticación básica.

Implementado en Go.