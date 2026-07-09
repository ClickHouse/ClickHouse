---
description: 'Управление поведением при перегрузке CPU на сервере.'
sidebar_label: 'Перегрузка сервера'
slug: /operations/settings/server-overload
title: 'Перегрузка сервера'
doc_type: 'reference'
---

<div id="overview">
  ## Обзор
</div>

Иногда сервер может быть перегружен по разным причинам. Чтобы определить текущую перегрузку CPU,
сервер ClickHouse вычисляет отношение времени ожидания CPU (метрика `OSCPUWaitMicroseconds`) ко времени активной работы
(метрика `OSCPUVirtualTimeMicroseconds`). Когда сервер перегружен выше определённого уровня,
имеет смысл отклонять некоторые запросы или даже сбрасывать запросы на подключение, чтобы не увеличивать нагрузку ещё сильнее.

Существует настройка сервера `os_cpu_busy_time_threshold`, которая задаёт минимальное время активной работы CPU,
при котором считается, что он выполняет полезную работу. Если текущее значение метрики `OSCPUVirtualTimeMicroseconds` ниже этого значения,
считается, что перегрузка CPU равна 0.

<div id="rejecting-queries">
  ## Отклонение запросов
</div>

Отклонение запросов управляется настройками уровня запроса `min_os_cpu_wait_time_ratio_to_throw` и
`max_os_cpu_wait_time_ratio_to_throw`. Если эти настройки заданы и `min_os_cpu_wait_time_ratio_to_throw` меньше
`max_os_cpu_wait_time_ratio_to_throw`, то запрос отклоняется, и генерируется исключение `SERVER_OVERLOADED`
с некоторой вероятностью, если коэффициент перегрузки не меньше `min_os_cpu_wait_time_ratio_to_throw`. Вероятность
определяется с помощью линейной интерполяции между минимальным и максимальным коэффициентами. Например, если `min_os_cpu_wait_time_ratio_to_throw = 2`,
`max_os_cpu_wait_time_ratio_to_throw = 6` и `cpu_overload = 4`, то запрос будет отклонён с вероятностью `0.5`.

<div id="dropping-connections">
  ## Разрыв соединений
</div>

Разрыв соединений управляется настройками уровня сервера `min_os_cpu_wait_time_ratio_to_drop_connection` и
`max_os_cpu_wait_time_ratio_to_drop_connection`. Эти настройки можно изменять без перезапуска сервера. Принцип работы
этих настроек аналогичен механизму отклонения запросов. Разница лишь в том, что в данном случае при перегрузке сервера
попытка установить соединение будет отклонена на стороне сервера.

<div id="resource-overload-warnings">
  ## Предупреждения о перегрузке ресурсов
</div>

При перегрузке сервера ClickHouse также записывает предупреждения о перегрузке CPU и памяти в таблицу `system.warnings`. Эти пороговые значения можно настроить в конфигурации сервера.

**Пример**

```xml

<resource_overload_warnings>
    <cpu_overload_warn_ratio>0.9</cpu_overload_warn_ratio>
    <cpu_overload_clear_ratio>0.8</cpu_overload_clear_ratio>
    <cpu_overload_duration_seconds>600</cpu_overload_duration_seconds>
    <memory_overload_warn_ratio>0.9</memory_overload_warn_ratio>
    <memory_overload_clear_ratio>0.8</memory_overload_clear_ratio>
    <memory_overload_duration_seconds>600</memory_overload_duration_seconds>
</resource_overload_warnings>
```