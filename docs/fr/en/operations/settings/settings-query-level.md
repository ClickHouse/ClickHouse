---
description: 'Paramètres au niveau de la requête'
sidebar_label: 'Paramètres de session au niveau de la requête'
slug: /operations/settings/query-level
title: 'Paramètres de session au niveau de la requête'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

Il existe plusieurs façons d’exécuter des instructions SQL avec des paramètres spécifiques.
Les paramètres sont définis en couches, et chaque couche suivante redéfinit les valeurs du paramètre définies par la couche précédente.

<div id="order-of-priority">
  ## Ordre de priorité
</div>

L’ordre de priorité pour définir un paramètre est le suivant :

1. Appliquer un paramètre directement à un utilisateur, ou dans un profil de paramètres

   * SQL (recommandé)
   * ajout d’un ou de plusieurs fichiers XML ou YAML dans `/etc/clickhouse-server/users.d`

2. Paramètres de session

   * Envoyez `SET setting=value` depuis la SQL Console de ClickHouse Cloud ou
     `clickhouse client` en mode interactif. De la même manière, vous pouvez utiliser des sessions ClickHouse
     avec le protocole HTTP. Pour cela, vous devez spécifier le
     paramètre HTTP `session_id`.

3. Paramètres de requête

   * Au démarrage de `clickhouse client` en mode non interactif, définissez le
     paramètre de démarrage `--setting=value`.
   * Lorsque vous utilisez l’API HTTP, transmettez des paramètres CGI (`URL?setting_1=value&setting_2=value...`).
   * Définissez les paramètres dans la
     clause [SETTINGS](../../sql-reference/statements/select/index.md#settings-in-select-query)
     de la requête SELECT. La valeur du paramètre s’applique uniquement à cette requête
     et revient à la valeur par défaut ou à la valeur précédente une fois la requête exécutée.

<div id="converting-a-setting-to-its-default-value">
  ## Rétablir un paramètre à sa valeur par défaut
</div>

Si vous modifiez un paramètre et souhaitez revenir à sa valeur par défaut, définissez sa valeur sur `DEFAULT`. La syntaxe est la suivante :

```sql
SET setting_name = DEFAULT
```

Par exemple, la valeur par défaut de `async_insert` est `0`. Supposons que vous la définissiez sur `1` :

```sql
SET async_insert = 1;

SELECT value FROM system.settings where name='async_insert';
```

La réponse est :

```response
┌─value──┐
│ 1      │
└────────┘
```

La commande suivante remet sa valeur à 0 :

```sql
SET async_insert = DEFAULT;

SELECT value FROM system.settings where name='async_insert';
```

Le paramètre est de nouveau défini sur sa valeur par défaut :

```response
┌─value───┐
│ 0       │
└─────────┘
```

<div id="custom_settings">
  ## Paramètres personnalisés
</div>

En plus des [paramètres](/fr/operations/settings/settings.md) courants, les utilisateurs peuvent définir des paramètres personnalisés.
Les paramètres personnalisés vous permettent de transmettre des **paramètres propres à la session** qui peuvent être utilisés dans des requêtes, des politiques ou des fonctions. Cela est utile lorsque vous devez :

* Filtrer les données en fonction de l&#39;identité de l&#39;utilisateur ou de l&#39;organisation
* Appliquer une logique métier différente selon le contexte
* Conserver des informations avec état d&#39;une requête à l&#39;autre au sein d&#39;une session

Le nom d&#39;un paramètre personnalisé doit commencer par l&#39;un des préfixes prédéfinis dans une liste que vous définissez.
La liste des préfixes peut être spécifiée à l&#39;aide du paramètre du serveur [`custom_settings_prefixes`](../../operations/server-configuration-parameters/settings.md#custom_settings_prefixes), défini dans votre fichier de configuration du serveur.

Dans l&#39;exemple ci-dessous, `SQL_` est choisi comme préfixe personnalisé :

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

:::note
Dans ClickHouse Cloud, il n’est pas possible de définir un préfixe personnalisé.
Tous les paramètres utilisateur personnalisés commencent par le préfixe `SQL_`.
:::

Pour définir un paramètre personnalisé, utilisez la commande `SET` :

```sql
SET SQL_a = 123;
```

Pour obtenir la valeur actuelle d’un paramètre personnalisé, utilisez la fonction `getSetting()` :

```sql
SELECT getSetting('SQL_a');
```

<div id="examples">
  ## Exemples
</div>

Ces exemples définissent tous la valeur du paramètre `async_insert` sur `1` et
montrent comment examiner les paramètres sur un système en fonctionnement.

<div id="using-sql-to-apply-a-setting-to-a-user-directly">
  ### Utiliser SQL pour définir directement un paramètre pour un utilisateur
</div>

Cela crée l’utilisateur `ingester` avec le paramètre `async_inset = 1` :

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS async_insert = 1
```

<div id="examine-the-settings-profile-and-assignment">
  #### Examiner le profil de paramètres et son attribution
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ ...                                                                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS async_insert = true  │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="using-sql-to-create-a-settings-profile-and-assign-to-a-user">
  ### Utiliser SQL pour créer un profil de paramètres et l’attribuer à un utilisateur
</div>

Cette commande crée le profil `log_ingest` avec le paramètre `async_inset = 1` :

```sql
CREATE
SETTINGS PROFILE log_ingest SETTINGS async_insert = 1
```

Cela crée l’utilisateur `ingester` et lui attribue le profil de paramètres `log_ingest` :

```sql
CREATE USER ingester
IDENTIFIED WITH sha256_hash BY '7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3'
-- highlight-next-line
SETTINGS PROFILE log_ingest
```

<div id="using-xml-to-create-a-settings-profile-and-user">
  ### Utiliser le format XML pour créer un profil de paramètres et un utilisateur
</div>

```xml title=/etc/clickhouse-server/users.d/users.xml
<clickhouse>
# highlight-start
    <profiles>
        <log_ingest>
            <async_insert>1</async_insert>
        </log_ingest>
    </profiles>
# highlight-end

    <users>
        <ingester>
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
# highlight-start
            <profile>log_ingest</profile>
# highlight-end
        </ingester>
        <default replace="true">
            <password_sha256_hex>7e099f39b84ea79559b3e85ea046804e63725fd1f46b37f281276aae20f86dc3</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
```

<div id="examine-the-settings-profile-and-assignment">
  #### Examiner le profil de paramètres et son attribution
</div>

```sql
SHOW ACCESS
```

```response
┌─ACCESS─────────────────────────────────────────────────────────────────────────────┐
│ CREATE USER default IDENTIFIED WITH sha256_password                                │
# highlight-next-line
│ CREATE USER ingester IDENTIFIED WITH sha256_password SETTINGS PROFILE log_ingest   │
│ CREATE SETTINGS PROFILE default                                                    │
# highlight-next-line
│ CREATE SETTINGS PROFILE log_ingest SETTINGS async_insert = true                    │
│ CREATE SETTINGS PROFILE readonly SETTINGS readonly = 1                             │
│ ...                                                                                │
└────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="assign-a-setting-to-a-session">
  ### Assigner un paramètre à une session
</div>

```sql
SET async_insert =1;
SELECT value FROM system.settings where name='async_insert';
```

```response
┌─value──┐
│ 1      │
└────────┘
```

<div id="assign-a-setting-during-a-query">
  ### Définir un paramètre dans une requête
</div>

```sql
INSERT INTO YourTable
-- highlight-next-line
SETTINGS async_insert=1
VALUES (...)
```

<div id="see-also">
  ## Voir aussi
</div>

* Consultez la page [Paramètres](/fr/operations/settings/settings.md) pour une description des paramètres de ClickHouse.
* [Paramètres globaux du serveur](/fr/operations/server-configuration-parameters/settings.md)