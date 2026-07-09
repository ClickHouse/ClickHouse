---
description: 'Вы можете отслеживать использование аппаратных ресурсов, а также метрики
  сервера ClickHouse.'
keywords: ['мониторинг', 'обсервабилити', 'Advanced dashboard', 'панель мониторинга', 'панель
    обсервабилити']
sidebar_label: 'Мониторинг'
sidebar_position: 45
slug: /operations/monitoring
title: 'Мониторинг'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # Мониторинг
</div>

:::note
Данные мониторинга, описанные в этом руководстве, доступны в ClickHouse Cloud. Помимо встроенной панели мониторинга, описанной ниже, базовые и расширенные метрики производительности также можно просматривать напрямую в главной консоли сервиса.
:::

Вы можете отслеживать:

* Использование аппаратных ресурсов.
* Метрики сервера ClickHouse.

<div id="built-in-advanced-observability-dashboard">
  ## Встроенная расширенная панель обсервабилити
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="Снимок экрана 2023-11-12 в 6 08 58 PM" size="md" />

В ClickHouse есть встроенная расширенная панель обсервабилити, доступная по адресу `$HOST:$PORT/dashboard` (требуются имя пользователя и пароль), в которой отображаются следующие метрики:

* Запросы/с
* Использование CPU (ядра)
* Выполняющиеся запросы
* Выполняющиеся слияния
* Выбранные байты/с
* Ожидание IO
* Ожидание CPU
* Использование CPU ОС (userspace)
* Использование CPU ОС (kernel)
* Чтение с диска
* Чтение из файловой системы
* Память (tracked)
* Вставленные строки/с
* Общее количество частей MergeTree
* Максимальное количество частей в партиции

<div id="resource-utilization">
  ## Использование ресурсов
</div>

ClickHouse также самостоятельно отслеживает состояние аппаратных ресурсов, таких как:

* Загрузка и температура процессоров.
* Использование дисковой подсистемы, оперативной памяти и сети.

Эти данные собираются в таблице `system.asynchronous_metric_log`.

<div id="clickhouse-server-metrics">
  ## Метрики сервера ClickHouse
</div>

Сервер ClickHouse имеет встроенные средства для мониторинга собственного состояния.

Чтобы отслеживать события сервера, используйте журналы сервера. См. раздел [logger](../operations/server-configuration-parameters/settings.md#logger) в файле конфигурации.

ClickHouse собирает:

* Различные метрики использования сервером вычислительных ресурсов.
* Общую статистику по обработке запросов.

Метрики можно найти в таблицах [system.metrics](/ru/operations/system-tables/metrics), [system.events](/ru/operations/system-tables/events) и [system.asynchronous&#95;metrics](/ru/operations/system-tables/asynchronous_metrics).

Вы можете настроить экспорт метрик из ClickHouse в [Graphite](https://github.com/graphite-project). См. раздел [Graphite](../operations/server-configuration-parameters/settings.md#graphite) в файле конфигурации сервера ClickHouse. Перед настройкой экспорта метрик необходимо настроить Graphite, следуя официальному [руководству](https://graphite.readthedocs.io/en/latest/install.html).

Вы можете настроить экспорт метрик из ClickHouse в [Prometheus](https://prometheus.io). См. раздел [Prometheus](../operations/server-configuration-parameters/settings.md#prometheus) в файле конфигурации сервера ClickHouse. Перед настройкой экспорта метрик необходимо настроить Prometheus, следуя официальному [руководству](https://prometheus.io/docs/prometheus/latest/installation/).

Кроме того, вы можете контролировать доступность сервера через HTTP API. Отправьте запрос `HTTP GET` к `/ping`. Если сервер доступен, он отвечает `200 OK`.

Чтобы отслеживать серверы в конфигурации кластера, следует задать параметр [max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) и использовать HTTP-ресурс `/replicas_status`. Запрос к `/replicas_status` возвращает `200 OK`, если реплика доступна и не отстает от других реплик. Если реплика отстает, возвращается `503 HTTP_SERVICE_UNAVAILABLE` с информацией о величине отставания.