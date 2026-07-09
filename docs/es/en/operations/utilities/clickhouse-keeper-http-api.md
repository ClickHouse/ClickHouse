---
description: 'Documentación de la API HTTP de ClickHouse Keeper y del dashboard web integrado'
sidebar_label: 'API HTTP de Keeper'
sidebar_position: 70
slug: /operations/utilities/clickhouse-keeper-http-api
title: 'API HTTP de Keeper y dashboard'
doc_type: 'reference'
---

ClickHouse Keeper proporciona una API HTTP y un dashboard web integrado para monitorización, comprobaciones de estado y gestión del almacenamiento.
Esta interfaz permite a los operadores inspeccionar el estado del clúster, ejecutar comandos y gestionar el almacenamiento de Keeper mediante un navegador web o clientes HTTP.

<div id="configuration">
  ## Configuración
</div>

Para habilitar la API HTTP, añada la sección `http_control` a la configuración de `keeper_server`:

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
  ### Opciones de configuración
</div>

| Configuración                             | Predeterminado | Descripción                                                                |
| ----------------------------------------- | -------------- | -------------------------------------------------------------------------- |
| `http_control.port`                       | -              | Puerto HTTP para el dashboard y la API                                     |
| `http_control.secure_port`                | -              | Puerto HTTPS (requiere configuración de SSL)                               |
| `http_control.readiness.endpoint`         | `/ready`       | Ruta personalizada para la sonda de preparación                            |
| `http_control.storage.session_timeout_ms` | `30000`        | Tiempo de espera de la sesión para operaciones de la API de almacenamiento |

<div id="endpoints">
  ## Endpoints
</div>

<div id="dashboard">
  ### Dashboard
</div>

* **Ruta**: `/dashboard`
* **Método**: GET
* **Descripción**: Ofrece un dashboard web integrado para monitorizar y gestionar Keeper

El dashboard ofrece:

* Visualización en tiempo real del estado del clúster
* Monitorización de nodos (rol, latencia, conexiones)
* Explorador de almacenamiento
* Interfaz para ejecutar comandos

<div id="readiness-probe">
  ### Sonda de preparación
</div>

* **Ruta**: `/ready` (configurable)
* **Método**: GET
* **Descripción**: endpoint de verificación de estado

Respuesta exitosa (HTTP 200):

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
  ### API de comandos
</div>

* **Ruta**: `/api/v1/commands/{command}`
* **Métodos**: GET, POST
* **Descripción**: Ejecuta comandos Four-Letter Word o comandos de la CLI del client de ClickHouse Keeper

Parámetros de consulta:

* `command` - El comando que se va a ejecutar
* `cwd` - Directorio de trabajo actual para comandos basados en rutas (por defecto: `/`)

Ejemplos:

```bash
# Four-Letter Word command
curl http://localhost:9182/api/v1/commands/stat

# ZooKeeper CLI command
curl "http://localhost:9182/api/v1/commands/ls?command=ls%20'/'&cwd=/"
```

<div id="storage-api">
  ### API de almacenamiento
</div>

* **Ruta base**: `/api/v1/storage`
* **Descripción**: API REST para operaciones de almacenamiento de Keeper

La API de almacenamiento sigue las convenciones REST, donde los métodos HTTP indican el tipo de operación:

| Operación  | Path                                   | Método | Código de estado | Descripción                   |
| ---------- | -------------------------------------- | ------ | ---------------- | ----------------------------- |
| Obtener    | `/api/v1/storage/{path}`               | GET    | 200              | Obtener los datos del nodo    |
| Listar     | `/api/v1/storage/{path}?children=true` | GET    | 200              | Listar nodos hijo             |
| Existe     | `/api/v1/storage/{path}`               | HEAD   | 200              | Comprobar si el nodo existe   |
| Crear      | `/api/v1/storage/{path}`               | POST   | 201              | Crear un nodo nuevo           |
| Actualizar | `/api/v1/storage/{path}?version={v}`   | PUT    | 200              | Actualizar los datos del nodo |
| Eliminar   | `/api/v1/storage/{path}?version={v}`   | DELETE | 204              | Eliminar el nodo              |