---
description: 'Documentation sur HTTP'
slug: /operations/external-authenticators/http
title: 'HTTP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Le serveur HTTP peut être utilisé pour authentifier les utilisateurs de ClickHouse. L’authentification HTTP ne peut être utilisée que comme authentificateur HTTP externe pour des utilisateurs existants, définis dans `users.xml` ou dans les chemins locaux de contrôle d’accès. Actuellement, seul le schéma d’authentification [Basic](https://datatracker.ietf.org/doc/html/rfc7617) utilisant la méthode GET est pris en charge.

<div id="http-auth-server-definition">
  ## Définition du serveur d&#39;authentification HTTP
</div>

Pour définir un serveur d&#39;authentification HTTP, vous devez ajouter la section `http_authentication_servers` au fichier `config.xml`.

**Exemple**

```xml
<clickhouse>
    <!- ... -->
    <http_authentication_servers>
        <basic_auth_server>
          <uri>http://localhost:8000/auth</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <forward_headers>
            <name>Custom-Auth-Header-1</name>
            <name>Custom-Auth-Header-2</name>
          </forward_headers>

        </basic_auth_server>
    </http_authentication_servers>
</clickhouse>

```

Notez que vous pouvez définir plusieurs serveurs HTTP dans la section `http_authentication_servers` en utilisant des noms distincts.

**Paramètres**

* `uri` - URI à utiliser pour effectuer la requête d&#39;authentification

Délais d&#39;expiration en millisecondes du socket utilisé pour communiquer avec le serveur :

* `connection_timeout_ms` - Par défaut : 1000 ms.
* `receive_timeout_ms` - Par défaut : 1000 ms.
* `send_timeout_ms` - Par défaut : 1000 ms.

Paramètres de réessai :

* `max_tries` - Nombre maximal de tentatives pour effectuer une requête d&#39;authentification. Par défaut : 3
* `retry_initial_backoff_ms` - Intervalle initial de backoff lors d&#39;un réessai. Par défaut : 50 ms
* `retry_max_backoff_ms` - Intervalle maximal de backoff. Par défaut : 1000 ms

Transfert des en-têtes :

Cette section définit quels en-têtes seront transférés depuis les en-têtes de la requête client vers l&#39;authentificateur HTTP externe. Notez que les en-têtes seront mis en correspondance avec ceux de la configuration sans tenir compte de la casse, mais transférés tels quels, c.-à-d. sans modification.

<div id="enabling-http-auth-in-users-xml">
  ### Activation de l’authentification HTTP dans `users.xml`
</div>

Pour activer l’authentification HTTP pour un utilisateur, spécifiez la section `http_authentication` à la place de `password` ou de sections similaires dans la définition de l’utilisateur.

Paramètres :

* `server` - Nom du serveur d’authentification HTTP configuré dans le fichier principal `config.xml`, comme décrit précédemment.
* `scheme` - Schéma d’authentification HTTP. Seul `Basic` est actuellement pris en charge. Par défaut : Basic

Exemple (à placer dans `users.xml`) :

```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <http_authentication>
            <server>basic_server</server>
            <scheme>basic</scheme>
        </http_authentication>
    </test_user_2>
</clickhouse>
```

:::note
Notez que l’authentification HTTP ne peut pas être utilisée avec un autre mécanisme d’authentification. La présence de toute autre section, telle que `password`, en plus de `http_authentication`, entraînera l’arrêt de ClickHouse.
:::

<div id="enabling-http-auth-using-sql">
  ### Activation de l’authentification HTTP à l’aide de SQL
</div>

Lorsque la [gestion des comptes et le contrôle d’accès pilotés par SQL](/fr/operations/access-rights#access-control-usage) sont activés dans ClickHouse, il est également possible de créer avec des instructions SQL des utilisateurs identifiés par authentification HTTP.

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server' SCHEME 'Basic'
```

...ou, `Basic` est utilisé par défaut si aucun schéma d’authentification n’est explicitement défini

```sql
CREATE USER my_user IDENTIFIED WITH HTTP SERVER 'basic_server'
```

<div id="passing-session-settings">
  ### Transmission des paramètres de session
</div>

Si le corps de réponse du serveur d&#39;authentification HTTP est au format JSON et contient le sous-objet `settings`, ClickHouse tentera d&#39;interpréter ses paires clé-valeur comme des valeurs de type chaîne et de les appliquer comme paramètres de session à la session en cours de l&#39;utilisateur authentifié. Si l&#39;analyse échoue, le corps de réponse du serveur sera ignoré.