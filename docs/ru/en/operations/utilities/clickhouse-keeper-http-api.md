---
description: 'Документация по HTTP API ClickHouse Keeper и встроенной веб-панели мониторинга'
sidebar_label: 'HTTP API ClickHouse Keeper'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'HTTP API и панель мониторинга ClickHouse Keeper'
doc_type: 'reference'
---

ClickHouse Keeper предоставляет HTTP API и встроенную веб-панель мониторинга для мониторинга, проверок работоспособности и управления хранилищем.
Этот интерфейс позволяет операторам просматривать статус кластера, выполнять команды и управлять хранилищем Keeper через веб-браузер или HTTP-клиенты.

<div id="configuration">
  ## Конфигурация
</div>

Чтобы включить HTTP API, добавьте раздел `http_control` в конфигурацию `keeper_server`:

```xml
<keeper_server>
    <!-- Other keeper_server configuration -->

    <http_control>
        <port>9182</port>
        <!-- <secure_port>9443</secure_port> -->
    </http_control>
</keeper_server>
```

<div id="configuration-options">
  ### Параметры конфигурации
</div>

| Параметр                                  | По умолчанию | Описание                                      |
| ----------------------------------------- | ------------ | --------------------------------------------- |
| `http_control.port`                       | -            | HTTP-порт для панели мониторинга и API        |
| `http_control.secure_port`                | -            | HTTPS-порт (требуется настройка SSL)          |
| `http_control.readiness.endpoint`         | `/ready`     | Пользовательский путь для проверки готовности |
| `http_control.storage.session_timeout_ms` | `30000`      | Тайм-аут сеанса для операций API хранилища    |

<div id="endpoints">
  ## Конечные точки
</div>

<div id="dashboard">
  ### Панель мониторинга
</div>

* **Путь**: `/dashboard`
* **Метод**: GET
* **Описание**: Отдает встроенную веб-панель мониторинга для наблюдения за Keeper и управления им

Панель мониторинга предоставляет:

* Визуализацию статуса кластера в реальном времени
* Мониторинг узлов (роль, задержка, соединения)
* Обозреватель хранилища
* Интерфейс выполнения команд

<div id="readiness-probe">
  ### Проверка готовности
</div>

* **Путь**: `/ready` (настраивается)
* **Метод**: GET
* **Описание**: Конечная точка для проверки работоспособности

Успешный ответ (HTTP 200):

```json
{
  "status": "ok",
  "details": {
    "role": "leader",
    "hasLeader": true
  }
}
```

<div id="commands-api">
  ### API команд
</div>

* **Путь**: `/api/v1/commands/{command}`
* **Методы**: GET, POST
* **Описание**: Выполняет команды Four-Letter Word или команды CLI клиента ClickHouse Keeper

Параметры запроса:

* `command` - Команда для выполнения
* `cwd` - Текущий рабочий каталог для команд, использующих пути (по умолчанию: `/`)

Примеры:

```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

<div id="storage-api">
  ### API хранилища
</div>

* **Базовый путь**: `/api/v1/storage`
* **Описание**: REST API для операций с хранилищем Keeper

API хранилища следует соглашениям REST, в которых HTTP-методы определяют тип операции:

| Операция  | Путь                                   | Метод  | Код состояния | Описание                       |
| --------- | -------------------------------------- | ------ | ------------- | ------------------------------ |
| Получить  | `/api/v1/storage/{path}`               | GET    | 200           | Получить данные узла           |
| Список    | `/api/v1/storage/{path}?children=true` | GET    | 200           | Получить список дочерних узлов |
| Проверить | `/api/v1/storage/{path}`               | HEAD   | 200           | Проверить, существует ли узел  |
| Создать   | `/api/v1/storage/{path}`               | POST   | 201           | Создать новый узел             |
| Обновить  | `/api/v1/storage/{path}?version={v}`   | PUT    | 200           | Обновить данные узла           |
| Удалить   | `/api/v1/storage/{path}?version={v}`   | DELETE | 204           | Удалить узел                   |