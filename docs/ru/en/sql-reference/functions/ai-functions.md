---
description: 'Документация по функциям ИИ'
sidebar_label: 'ИИ'
slug: /sql-reference/functions/ai-functions
title: 'Функции ИИ'
doc_type: 'reference'
---

Функции ИИ — это встроенные функции ClickHouse, которые можно использовать для обращения к ИИ или генерации эмбеддинга при работе с данными, извлечении информации, классификации данных и т. д...

:::note
Функции ИИ являются экспериментальными. Чтобы включить их, установите [`allow_experimental_ai_functions`](/ru/operations/settings/settings#allow_experimental_ai_functions).
:::

:::note
Функции ИИ могут возвращать непредсказуемые результаты. Результат во многом зависит от качества промпта и используемой модели.
:::

Все функции используют общую инфраструктуру, которая обеспечивает:

* **Контроль квот**: Лимиты на количество токенов в рамках одного запроса ([`ai_function_max_input_tokens_per_query`](/ru/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/ru/operations/settings/settings#ai_function_max_output_tokens_per_query)) и вызовов API ([`ai_function_max_api_calls_per_query`](/ru/operations/settings/settings#ai_function_max_api_calls_per_query)).
* **Повторные попытки с задержкой**: При временных сбоях выполняются повторные попытки ([`ai_function_max_retries`](/ru/operations/settings/settings#ai_function_max_retries)) с экспоненциально увеличивающейся задержкой ([`ai_function_retry_initial_delay_ms`](/ru/operations/settings/settings#ai_function_retry_initial_delay_ms)).

<div id="configuration">
  ## Конфигурация
</div>

Функции ИИ используют **именованную коллекцию**, в которой хранятся учетные данные провайдера и параметры конфигурации. Для разных функций или их вызовов можно создавать и использовать разные именованные коллекции. Например, может понадобиться отдельная именованная коллекция для текстовых функций (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) и другая — для функции `aiEmbed`, поскольку для них требуются разные конечные точки и обычно используются разные модели.

Пример оператора для создания именованной коллекции с учетными данными провайдера: одна — с конечной точкой чата, другая — с конечной точкой для эмбеддингов:

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
  ### Параметры именованной коллекции
</div>

| Параметр      | Тип    | По умолчанию | Описание                                                                                                                                                                                                  |
| ------------- | ------ | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `provider`    | String | —            | Провайдер модели. Поддерживаются: `'openai'`, `'anthropic'`. См. примечание ниже.                                                                                                                         |
| `endpoint`    | String | —            | URL конечной точки API.                                                                                                                                                                                   |
| `model`       | String | —            | Имя модели (например, `'gpt-4o-mini'`, `'text-embedding-3-small'`).                                                                                                                                       |
| `api_key`     | String | —            | Ключ аутентификации для провайдера. Необязательно: если параметр не указан, заголовок авторизации не отправляется, что позволяет использовать серверы, совместимые с OpenAI, не требующие аутентификации. |
| `max_tokens`  | UInt64 | `1024`       | Максимальное количество токенов в ответе на один вызов API.                                                                                                                                               |
| `api_version` | String | —            | Строка версии API. Используется Anthropic (`'2023-06-01'`).                                                                                                                                               |

:::note
Любой API, совместимый с OpenAI (например, vLLM, Ollama, LiteLLM), можно использовать, задав `provider = 'openai'` и указав в `endpoint` адрес вашего сервиса.
:::

<div id="selecting-credentials">
  ### Выбор учетных данных
</div>

Функция определяет, какую именованную коллекцию использовать, в следующем порядке:

1. ключ `credentials` в ее карте параметров, если он указан;
2. в противном случае — применимую настройку учетных данных по умолчанию:
   * [`ai_function_text_default_credentials`](/ru/operations/settings/settings#ai_function_text_default_credentials) для текстовых функций (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`);
   * [`ai_function_embedding_default_credentials`](/ru/operations/settings/settings#ai_function_embedding_default_credentials) для `aiEmbed`.

Если не задано ни одно из них, вызов завершится ошибкой. Для текстовых функций и функций эмбеддинга используются разные настройки по умолчанию, поскольку конечная точка и модель для chat completions отличаются от конечной точки и модели для эмбеддинга.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### карта параметров
</div>

Каждая функция принимает необязательный завершающий параметр `Map(String, String)`. Все значения — строки (числа заключайте в кавычки, например `'0.2'`). Неизвестные ключи не допускаются. Если ключ указан, он переопределяет соответствующее значение из именованной коллекции; если ключ не указан, используется значение из именованной коллекции (для `model`/`max_tokens`) или встроенное значение по умолчанию.

Следующие параметры являются общими для всех функций ИИ:

| Key           | Description                                                     |
| ------------- | --------------------------------------------------------------- |
| `credentials` | Именованная коллекция, которую следует использовать (см. выше). |
| `model`       | Переопределяет `model` из коллекции.                            |

Отдельные функции принимают дополнительные параметры, специфичные для конкретной функции (например, `max_tokens`, `temperature`, `system_prompt`, `instructions` и `dimensions`). Сведения о параметрах, которые принимает каждая функция, и об их значениях по умолчанию см. в справочном описании соответствующей функции ниже.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### Настройки уровня запроса
</div>

Все настройки, связанные с ИИ, перечислены в разделе [Настройки](/ru/operations/settings/settings) и имеют префикс `ai_function_`.

<div id="restricting-endpoint-hosts">
  ### Ограничение хостов конечных точек
</div>

URL `endpoint` в AI именованной коллекции — это внешний пункт назначения, к которому сервер подключается от собственного имени и при этом может передавать (если указан) `api_key` этой именованной коллекции в заголовках запроса. По умолчанию ClickHouse разрешает любой хост. Чтобы ограничить функции определённым набором провайдеров, настройте [`remote_url_allow_hosts`](/ru/operations/server-configuration-parameters/settings#remote_url_allow_hosts) в конфигурации сервера, например:

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

Обратите внимание, что этот параметр действует на уровне всего сервера и применяется ко всем возможностям, использующим HTTP.

<div id="transport-security">
  ### Безопасность транспортного уровня (HTTP vs HTTPS)
</div>

Транспорт определяется исключительно схемой URL конечной точки `endpoint`. Шифрование полезной нагрузки запроса на уровне приложения отсутствует; защита данных при передаче полностью зависит от схемы:

* `https://` — соединение использует TLS. Тело запроса (входной текст, промпты) и `api_key` в заголовках запроса шифруются при передаче, а сертификат провайдера проверяется. Используйте этот вариант для любого удалённого провайдера.
* `http://` — соединение **не шифруется**. Тело запроса и `api_key` передаются в открытом виде. Используйте этот вариант только для доверенного провайдера в частной сети (например, локального экземпляра `vLLM` или `Ollama`).

Функции ИИ не принудительно требуют HTTPS: конечная точка `http://` принимается, и данные отправляются без шифрования. В настоящее время нет настройки на стороне сервера, которая отклоняла бы незашифрованные конечные точки ИИ — [`remote_url_allow_hosts`](/ru/operations/server-configuration-parameters/settings#remote_url_allow_hosts) ограничивает только хост пункта назначения и не проверяет схему URL, поэтому конечная точка `http://` на разрешённом хосте всё равно будет принята. Чтобы обеспечить шифрование транспортного уровня, настройте именованные коллекции с конечными точками `https://`.

Обратите внимание, что в обоих случаях провайдер получает входные данные в открытом виде после завершения TLS; TLS защищает данные только на сетевом участке между сервером и провайдером.

<div id="supported-providers">
  ## Поддерживаемые провайдеры
</div>

| Провайдер | значение `provider` | Чат-функции | Примечания                                |
| --------- | ------------------- | ----------- | ----------------------------------------- |
| OpenAI    | `'openai'`          | Да          | Провайдер по умолчанию.                   |
| Anthropic | `'anthropic'`       | Да          | Использует конечную точку `/v1/messages`. |

<div id="observability">
  ## Обсервабилити
</div>

Активность функции ИИ отслеживается с помощью [ProfileEvents](/ru/operations/system-tables/query_log) в ClickHouse:

| ProfileEvent      | Описание                                                                                                  |
| ----------------- | --------------------------------------------------------------------------------------------------------- |
| `AIAPICalls`      | Количество HTTP-запросов, отправленных провайдеру ИИ.                                                     |
| `AIInputTokens`   | Общее количество использованных входных токенов.                                                          |
| `AIOutputTokens`  | Общее количество использованных выходных токенов.                                                         |
| `AIRowsProcessed` | Количество строк, по которым был получен результат.                                                       |
| `AIRowsSkipped`   | Количество пропущенных строк (превышена квота или произошла ошибка при `ai_function_throw_on_error = 0`). |

Выполните запрос к этим событиям:

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
  Содержимое внутри тегов ниже во время сборки документации заменяется
  документацией, сгенерированной из system.functions. Пожалуйста, не изменяйте и не удаляйте эти теги.
  См.: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }