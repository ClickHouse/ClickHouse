---
description: 'Компонуемые протоколы обеспечивает более гибкую настройку TCP-доступа
  к серверу ClickHouse.'
sidebar_label: 'Компонуемые протоколы'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Компонуемые протоколы'
doc_type: 'reference'
---

<div id="overview">
  ## Обзор
</div>

Компонуемые протоколы позволяют более гибко настраивать TCP-доступ к
серверу ClickHouse. Эта конфигурация может использоваться наряду с
традиционной или вместо неё.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## Настройка компонуемых протоколов
</div>

Компонуемые протоколы можно настраивать в XML-файле конфигурации. Раздел protocols
в XML-файле конфигурации обозначается тегами `protocols`:

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### Настройка слоёв протокола
</div>

Слои протокола можно определять с помощью базовых модулей. Например, чтобы определить
слой HTTP, добавьте новый базовый модуль в раздел `protocols`:

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

Модули можно настроить по следующим параметрам:

* `plain_http` — имя, на которое может ссылаться другой слой
* `type` — указывает обработчик протокола, который будет создан для обработки данных.
  Доступен следующий набор предопределённых обработчиков протоколов:
  * `tcp` — обработчик native-протокола ClickHouse
  * `http` — обработчик HTTP-протокола ClickHouse
  * `tls` — слой шифрования TLS
  * `proxy1` — слой PROXYv1
  * `mysql` — обработчик протокола совместимости MySQL
  * `postgres` — обработчик протокола совместимости PostgreSQL
  * `prometheus` — обработчик протокола Prometheus
  * `interserver` — межсерверный обработчик ClickHouse

:::note
Обработчик протокола `gRPC` не реализован для `Компонуемые протоколы`
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### Настройка конечных точек
</div>

Конечные точки (прослушиваемые порты) обозначаются тегами `<port>` и необязательным `<host>`.
Например, чтобы настроить конечную точку в ранее добавленном HTTP-слое, мы
можем изменить конфигурацию следующим образом:

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

Если тег `<host>` не указан, используется `<listen_host>` из корневой конфигурации.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### Настройка последовательностей слоёв
</div>

Последовательности слоёв задаются с помощью тега `<impl>` и ссылки на другой
модуль. Например, чтобы настроить слой TLS поверх нашего модуля plain&#95;http,
можно дополнительно изменить конфигурацию следующим образом:

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### Привязка конечных точек к слоям
</div>

Конечные точки можно привязать к любому слою. Например, можно определить конечные точки для
HTTP (порт 8123) и HTTPS (порт 8443):

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### Определение дополнительных конечных точек
</div>

Дополнительные конечные точки можно определить, указав любой модуль и опустив
тег `<type>`. Например, можно определить конечную точку `another_http` для
модуля `plain_http` следующим образом:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### Пользовательские HTTP-обработчики для каждой конечной точки
</div>

По умолчанию все записи протокола с `type=http` используют общую конфигурацию
`<http_handlers>`. Это можно переопределить, добавив тег `<handlers>`, который ссылается
на другой раздел конфигурации. Благодаря этому для каждого HTTP-порта можно задать
собственный набор правил маршрутизации HTTP.

Например, чтобы запустить альтернативный HTTP API на порту 8124 с собственными обработчиками:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

В этом примере запросы к порту 8123 используют стандартные правила `<http_handlers>`,
а запросы к порту 8124 — правила `<http_handlers_alt>`. Если `<handlers>`
опущен, конечная точка по умолчанию использует `<http_handlers>`.

Раздел пользовательских обработчиков имеет тот же формат, что и
[`<http_handlers>`](/ru/docs/operations/server-configuration-parameters/settings#http_handlers).
Изменения в разделе пользовательских обработчиков обнаруживаются при перезагрузке конфигурации, и
соответствующая конечная точка автоматически перезапускается.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### Указание дополнительных параметров слоя
</div>

Некоторые модули могут содержать дополнительные параметры слоя. Например, слой TLS
позволяет указать закрытый ключ (`privateKeyFile`) и файлы сертификатов (`certificateFile`)
следующим образом:

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```