---
description: 'Documentación sobre las funciones de IA'
sidebar_label: 'IA'
slug: /sql-reference/functions/ai-functions
title: 'Funciones de IA'
doc_type: 'reference'
---

Las funciones de IA son funciones integradas de ClickHouse que puedes usar para invocar IA o generar embeddings para trabajar con tus datos, extraer información, clasificar datos, etc...

:::note
Las funciones de IA son experimentales. Establece [`allow_experimental_ai_functions`](/es/operations/settings/settings#allow_experimental_ai_functions) para habilitarlas.
:::

:::note
Las funciones de IA pueden devolver resultados impredecibles. El resultado dependerá en gran medida de la calidad del prompt y del modelo utilizado.
:::

Todas las funciones comparten una infraestructura común que proporciona:

* **Aplicación de cuotas**: Límites por consulta de tokens ([`ai_function_max_input_tokens_per_query`](/es/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/es/operations/settings/settings#ai_function_max_output_tokens_per_query)) y llamadas a la API ([`ai_function_max_api_calls_per_query`](/es/operations/settings/settings#ai_function_max_api_calls_per_query)).
* **Reintentos con backoff**: Los fallos transitorios se reintentan ([`ai_function_max_retries`](/es/operations/settings/settings#ai_function_max_retries)) con backoff exponencial ([`ai_function_retry_initial_delay_ms`](/es/operations/settings/settings#ai_function_retry_initial_delay_ms)).

<div id="configuration">
  ## Configuración
</div>

Las funciones de IA hacen referencia a una **named collection** que almacena las credenciales del proveedor y la configuración. Se pueden crear y usar distintas named collections para diferentes funciones o llamadas a funciones. Por ejemplo, puede que desee definir una named collection distinta para usar con las funciones de texto (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) frente a la función `aiEmbed`, ya que requieren endpoints diferentes y normalmente usan modelos distintos.

Ejemplo de sentencia para crear una named collection con credenciales del proveedor: una con un endpoint de chat y otra con un endpoint de embedding:

```sql
CREATE NAMED COLLECTION ai_text_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/chat/completions',
    model = 'gpt-4o-mini',
    api_key = 'sk-...';

CREATE NAMED COLLECTION ai_embedding_credentials AS
    provider = 'openai',
    endpoint = 'https://api.openai.com/v1/embeddings',
    model = 'text-embedding-3-small',
    api_key = 'sk-...';
```

<div id="named-collection-parameters">
  ### Parámetros de la named collection
</div>

| Parámetro     | Tipo   | Predeterminado | Descripción                                                                                                                                                                                |
| ------------- | ------ | -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `provider`    | String | —              | Proveedor del modelo. Valores admitidos: `'openai'`, `'anthropic'`. Consulta la nota a continuación.                                                                                       |
| `endpoint`    | String | —              | URL del endpoint de la API.                                                                                                                                                                |
| `model`       | String | —              | Nombre del modelo (p. ej. `'gpt-4o-mini'`, `'text-embedding-3-small'`).                                                                                                                    |
| `api_key`     | String | —              | Clave de autenticación del proveedor. Opcional: si se omite, no se envía el header de autenticación, lo que permite usar servidores compatibles con OpenAI que no requieren autenticación. |
| `max_tokens`  | UInt64 | `1024`         | Número máximo de tokens de salida por llamada a la API.                                                                                                                                    |
| `api_version` | String | —              | Cadena de versión de la API. La utiliza Anthropic (`'2023-06-01'`).                                                                                                                        |

:::note
Puede usar cualquier API compatible con OpenAI (p. ej. vLLM, Ollama, LiteLLM) configurando `provider = 'openai'` y apuntando `endpoint` a su servicio.
:::

<div id="selecting-credentials">
  ### Selección de credenciales
</div>

Una función determina la named collection que debe usar a partir de, en este orden:

1. la clave `credentials` de su mapa de parámetros, cuando está presente;
2. en caso contrario, la configuración predeterminada de credenciales aplicable:
   * [`ai_function_text_default_credentials`](/es/operations/settings/settings#ai_function_text_default_credentials) para las funciones de texto (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`);
   * [`ai_function_embedding_default_credentials`](/es/operations/settings/settings#ai_function_embedding_default_credentials) para `aiEmbed`.

Si no se establece ninguna de las dos, la llamada falla. Las funciones de texto y de embeddings usan configuraciones predeterminadas distintas porque el endpoint y el modelo de chat completions difieren de los de embeddings.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### Mapa de parámetros
</div>

Cada función acepta opcionalmente un `Map(String, String)` de parámetros al final. Todos los valores son cadenas (ponga los números entre comillas; por ejemplo, `'0.2'`). Las claves desconocidas se rechazan. Si una clave está presente, sobrescribe el valor correspondiente de la named collection; si no está presente, se usa la named collection (para `model`/`max_tokens`) o el valor predeterminado integrado.

Los siguientes parámetros son comunes a todas las funciones de IA:

| Clave         | Descripción                                      |
| ------------- | ------------------------------------------------ |
| `credentials` | Named collection que se usará (consulte arriba). |
| `model`       | Sobrescribe el `model` de la colección.          |

Las funciones individuales aceptan parámetros adicionales específicos de cada función (como `max_tokens`, `temperature`, `system_prompt`, `instructions` y `dimensions`). Consulte la referencia de cada función a continuación para ver qué parámetros acepta y cuáles son sus valores predeterminados.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### Configuración a nivel de consulta
</div>

Toda la configuración relacionada con la IA se encuentra en [Configuración](/es/operations/settings/settings), bajo el prefijo `ai_function_`.

<div id="restricting-endpoint-hosts">
  ### Restringir los hosts del `endpoint`
</div>

La URL del `endpoint` en una named collection de IA es un destino saliente al que el servidor se conecta con su propia identidad y que, si se especifica, puede incluir la `api_key` de la named collection en los encabezados de la solicitud. De forma predeterminada, ClickHouse permite cualquier host. Para restringir las funciones a un conjunto específico de proveedores, configure [`remote_url_allow_hosts`](/es/operations/server-configuration-parameters/settings#remote_url_allow_hosts) en la configuración del servidor; por ejemplo:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

Ten en cuenta que esta configuración se aplica a todo el servidor y a todas las funcionalidades que usan HTTP.

<div id="transport-security">
  ### Seguridad del transporte (HTTP frente a HTTPS)
</div>

El transporte viene determinado únicamente por el esquema de la URL del `endpoint`. No hay cifrado del payload de la solicitud a nivel de aplicación; la protección de los datos en tránsito depende por completo del esquema:

* `https://` — la conexión usa TLS. El cuerpo de la solicitud (texto de entrada, prompts) y la `api_key` en los encabezados de la solicitud se cifran en tránsito, y se valida el certificado del proveedor. Utilice esta opción para cualquier proveedor remoto.
* `http://` — la conexión **no está cifrada**. El cuerpo de la solicitud y la `api_key` se envían en texto claro. Utilice esta opción solo con un proveedor de confianza en una red privada (por ejemplo, una instancia local de `vLLM` u `Ollama`).

Las funciones de IA no fuerzan HTTPS: se acepta un `endpoint` con `http://` y los datos se envían sin cifrar. Actualmente no existe ninguna configuración del lado del servidor que rechace endpoints de IA en texto claro: [`remote_url_allow_hosts`](/es/operations/server-configuration-parameters/settings#remote_url_allow_hosts) restringe únicamente el host de destino y no inspecciona el esquema de la URL, por lo que un `endpoint` `http://` hacia un host permitido seguirá pasando. Para garantizar un transporte cifrado, configure named collections con endpoints `https://`.

Tenga en cuenta que, en cualquiera de los dos casos, el proveedor recibe los datos de entrada en texto claro después de la terminación de TLS; TLS protege los datos solo en la ruta de red entre el servidor y el proveedor.

<div id="supported-providers">
  ## Proveedores compatibles
</div>

| Proveedor | valor de `provider` | Funciones de chat | Notas                           |
| --------- | ------------------- | ----------------- | ------------------------------- |
| OpenAI    | `'openai'`          | Sí                | Proveedor por defecto.          |
| Anthropic | `'anthropic'`       | Sí                | Usa el endpoint `/v1/messages`. |

<div id="observability">
  ## Observabilidad
</div>

La actividad de la función de IA se registra mediante [ProfileEvents](/es/operations/system-tables/query_log) de ClickHouse:

| ProfileEvent      | Descripción                                                                                         |
| ----------------- | --------------------------------------------------------------------------------------------------- |
| `AIAPICalls`      | Número de peticiones HTTP realizadas al proveedor de IA.                                            |
| `AIInputTokens`   | Total de tokens de entrada consumidos.                                                              |
| `AIOutputTokens`  | Total de tokens de salida consumidos.                                                               |
| `AIRowsProcessed` | Número de filas que recibieron un resultado.                                                        |
| `AIRowsSkipped`   | Número de filas omitidas (se superó la cuota o hubo un error con `ai_function_throw_on_error = 0`). |

Consulta estos eventos:

```sql
SELECT
    ProfileEvents['AIAPICalls'] AS api_calls,
    ProfileEvents['AIInputTokens'] AS input_tokens,
    ProfileEvents['AIOutputTokens'] AS output_tokens
FROM system.query_log
WHERE query_id = 'query_id'
AND type = 'QueryFinish'
ORDER BY event_time DESC;
```

{/*
  El contenido interno de las etiquetas de abajo se reemplaza durante la compilación del framework de documentación con
  documentación generada a partir de system.functions. No modifique ni elimine las etiquetas.
  Consulte: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }