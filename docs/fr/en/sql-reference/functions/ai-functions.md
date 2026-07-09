---
description: 'Documentation sur les fonctions d’IA'
sidebar_label: 'IA'
slug: /sql-reference/functions/ai-functions
title: 'Fonctions d’IA'
doc_type: 'reference'
---

Les fonctions d’IA sont des fonctions intégrées à ClickHouse que vous pouvez utiliser pour faire appel à l’IA ou générer des embeddings afin de travailler avec vos données, d’en extraire des informations, de les classer, etc.

:::note
Les fonctions d’IA sont expérimentales. Définissez [`allow_experimental_ai_functions`](/fr/operations/settings/settings#allow_experimental_ai_functions) pour les activer.
:::

:::note
Les fonctions d’IA peuvent produire des résultats imprévisibles. Le résultat dépendra fortement de la qualité du prompt et du modèle utilisé.
:::

Toutes les fonctions reposent sur une infrastructure commune qui fournit :

* **Application des quotas** : limites par requête sur les tokens ([`ai_function_max_input_tokens_per_query`](/fr/operations/settings/settings#ai_function_max_input_tokens_per_query), [`ai_function_max_output_tokens_per_query`](/fr/operations/settings/settings#ai_function_max_output_tokens_per_query)) et sur les appels d’API ([`ai_function_max_api_calls_per_query`](/fr/operations/settings/settings#ai_function_max_api_calls_per_query)).
* **Réessais avec backoff** : les échecs transitoires donnent lieu à de nouvelles tentatives ([`ai_function_max_retries`](/fr/operations/settings/settings#ai_function_max_retries)) avec un backoff exponentiel ([`ai_function_retry_initial_delay_ms`](/fr/operations/settings/settings#ai_function_retry_initial_delay_ms)).

<div id="configuration">
  ## Configuration
</div>

Les fonctions d’IA font référence à une **collection nommée** qui stocke les informations d’authentification du fournisseur et la configuration. Différentes collections nommées peuvent être créées et utilisées pour différentes fonctions ou différents appels de fonction. Par exemple, vous pouvez définir une collection nommée distincte pour les fonctions de texte (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`), et une autre pour la fonction `aiEmbed`, qui nécessite des endpoints différents et utilise généralement des modèles différents.

Exemple d’instruction pour créer une collection nommée avec les informations d’authentification du fournisseur, l’une avec un endpoint de chat et l’autre avec un endpoint d’embedding :

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
  ### Paramètres de la collection nommée
</div>

| Parameter     | Type   | Default | Description                                                                                                                                                                                                              |
| ------------- | ------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `provider`    | String | —       | Fournisseur du modèle. Prise en charge : `'openai'`, `'anthropic'`. Voir la note ci-dessous.                                                                                                                             |
| `endpoint`    | String | —       | URL de l’endpoint de l’API.                                                                                                                                                                                              |
| `model`       | String | —       | Nom du modèle (p. ex. `'gpt-4o-mini'`, `'text-embedding-3-small'`).                                                                                                                                                      |
| `api_key`     | String | —       | Clé d’authentification du fournisseur. Facultatif : si elle est omise, l’en-tête d’authentification n’est pas envoyé, ce qui permet de cibler des serveurs compatibles OpenAI qui ne nécessitent pas d’authentification. |
| `max_tokens`  | UInt64 | `1024`  | Nombre maximal de tokens de sortie par appel d’API.                                                                                                                                                                      |
| `api_version` | String | —       | Chaîne de version de l’API. Utilisée par Anthropic (`'2023-06-01'`).                                                                                                                                                     |

:::note
Toute API compatible OpenAI (p. ex. vLLM, Ollama, LiteLLM) peut être utilisée en définissant `provider = 'openai'` et en faisant pointer `endpoint` vers votre service.
:::

<div id="selecting-credentials">
  ### Sélection des identifiants d’authentification
</div>

Une fonction détermine la collection nommée à utiliser dans l’ordre suivant :

1. la clé `credentials` de sa map de paramètres, lorsqu’elle est présente ;
2. sinon, le réglage par défaut applicable pour les identifiants d’authentification :
   * [`ai_function_text_default_credentials`](/fr/operations/settings/settings#ai_function_text_default_credentials) pour les fonctions de texte (`aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`) ;
   * [`ai_function_embedding_default_credentials`](/fr/operations/settings/settings#ai_function_embedding_default_credentials) pour `aiEmbed`.

Si aucun des deux n’est défini, l’appel échoue. Les fonctions de texte et d’embedding utilisent des réglages par défaut distincts, car l’endpoint et le modèle de chat completions diffèrent de ceux des embeddings.

```sql
SET ai_function_text_default_credentials = 'ai_text_credentials';

-- Uses ai_text_credentials from the setting:
SELECT aiGenerate('What is 2 + 2? Reply with just the number.');

-- Overrides the default for this call:
SELECT aiGenerate('Bonjour', map('credentials', 'other_credentials'));
```

<div id="parameter-map">
  ### Map de paramètres
</div>

Chaque fonction accepte, en dernier argument, un `Map(String, String)` optionnel contenant des paramètres. Toutes les valeurs sont des chaînes de caractères (mettez les nombres entre guillemets, par exemple `'0.2'`). Les clés inconnues sont rejetées. Une clé présente remplace la valeur correspondante de la collection nommée ; une clé absente reprend la valeur de la collection nommée (pour `model`/`max_tokens`) ou la valeur par défaut intégrée.

Les paramètres suivants sont communs à toutes les fonctions d’IA :

| Clé           | Description                                   |
| ------------- | --------------------------------------------- |
| `credentials` | Collection nommée à utiliser (voir ci-dessus). |
| `model`       | Remplace le `model` de la collection.         |

Chaque fonction accepte également des paramètres supplémentaires qui lui sont propres (comme `max_tokens`, `temperature`, `system_prompt`, `instructions` et `dimensions`). Consultez la référence de chaque fonction ci-dessous pour connaître les paramètres qu’elle accepte et leurs valeurs par défaut.

```sql
SELECT aiGenerate(body, map('temperature', '0.2', 'system_prompt', 'You are terse.')) FROM articles;
```

<div id="query-level-settings">
  ### Paramètres au niveau des requêtes
</div>

Tous les paramètres liés à l’IA sont répertoriés dans [Paramètres](/fr/operations/settings/settings), sous le préfixe `ai_function_`.

<div id="restricting-endpoint-hosts">
  ### Restriction des hôtes de l’endpoint
</div>

L’URL `endpoint` dans une `collection nommée` d’IA est une destination sortante à laquelle le serveur se connecte avec sa propre identité, en transmettant potentiellement (si elle est spécifiée) l’`api_key` de la `collection nommée` dans les en-têtes de la requête. Par défaut, ClickHouse autorise n’importe quel hôte. Pour restreindre les fonctions à un ensemble spécifique de fournisseurs, configurez [`remote_url_allow_hosts`](/fr/operations/server-configuration-parameters/settings#remote_url_allow_hosts) dans la configuration du serveur, par exemple :

```xml
<remote_url_allow_hosts>
    <host>api.openai.com</host>
    <host>api.anthropic.com</host>
</remote_url_allow_hosts>
```

Notez que ce paramètre s&#39;applique à l&#39;ensemble du serveur et à toutes les fonctionnalités qui utilisent HTTP.

<div id="transport-security">
  ### Sécurité du transport (HTTP vs HTTPS)
</div>

Le transport est déterminé uniquement par le schéma de l’URL de l’`endpoint`. Il n’existe aucun chiffrement de la charge utile de la requête au niveau de l’application ; la protection des données en transit dépend entièrement du schéma :

* `https://` — la connexion utilise TLS. Le corps de la requête (texte d’entrée, prompts) et la `api_key` dans les en-têtes de la requête sont chiffrés en transit, et le certificat du fournisseur est validé. Utilisez cette option pour tout fournisseur distant.
* `http://` — la connexion n’est **pas chiffrée**. Le corps de la requête et la `api_key` sont envoyés en clair. N’utilisez cette option que pour un fournisseur de confiance sur un réseau privé (par exemple, une instance locale `vLLM` ou `Ollama`).

Les fonctions d’IA n’imposent pas HTTPS : un endpoint `http://` est accepté et envoie les données sans chiffrement. Il n’existe actuellement aucun paramètre côté serveur qui rejette les endpoints IA en clair — [`remote_url_allow_hosts`](/fr/operations/server-configuration-parameters/settings#remote_url_allow_hosts) restreint uniquement l’hôte de destination et n’inspecte pas le schéma de l’URL ; un endpoint `http://` vers un hôte autorisé passe donc quand même. Pour garantir un transport chiffré, configurez des collections nommées avec des endpoints `https://`.

Notez que, dans les deux cas, le fournisseur reçoit les données d’entrée en clair après la terminaison TLS ; TLS protège les données uniquement sur le trajet réseau entre le serveur et le fournisseur.

<div id="supported-providers">
  ## Fournisseurs pris en charge
</div>

| Fournisseur | Valeur `provider` | Fonctions de chat | Remarques                          |
| ----------- | ----------------- | ----------------- | ---------------------------------- |
| OpenAI      | `'openai'`        | Oui               | Fournisseur par défaut.            |
| Anthropic   | `'anthropic'`     | Oui               | Utilise l’endpoint `/v1/messages`. |

<div id="observability">
  ## Observabilité
</div>

L’activité de la fonction d’IA est suivie dans les [ProfileEvents](/fr/operations/system-tables/query_log) de ClickHouse :

| ProfileEvent      | Description                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------ |
| `AIAPICalls`      | Nombre de requêtes HTTP envoyées au fournisseur d’IA.                                      |
| `AIInputTokens`   | Nombre total de tokens d’entrée consommés.                                                 |
| `AIOutputTokens`  | Nombre total de tokens de sortie consommés.                                                |
| `AIRowsProcessed` | Nombre de lignes ayant reçu un résultat.                                                   |
| `AIRowsSkipped`   | Nombre de lignes ignorées (quota dépassé ou erreur avec `ai_function_throw_on_error = 0`). |

Interrogez ces événements :

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
  Le contenu interne des balises ci-dessous est remplacé, lors de la compilation du framework de documentation, par
  la documentation générée à partir de system.functions. Veuillez ne pas modifier ni supprimer les balises.
  Voir : https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }