---
description: 'Documentation pour créer et configurer des dictionnaires'
sidebar_label: 'Vue d’ensemble'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

Un dictionnaire est une correspondance (`key -> attributes`) pratique pour différents types de listes de référence.
ClickHouse prend en charge des fonctions spéciales pour manipuler les dictionnaires, utilisables dans les requêtes. Il est plus simple et plus efficace d’utiliser les dictionnaires via ces fonctions que d’effectuer un `JOIN` avec des tables de référence.

Les dictionnaires peuvent être créés de deux façons :

* [Avec une requête DDL](#creating-a-dictionary-with-a-ddl-query) (recommandé)
* [Avec un fichier de configuration](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## Création d&#39;un dictionnaire avec une requête DDL
</div>

<CloudSupportedBadge />

Les dictionnaires peuvent être créés à l&#39;aide de requêtes DDL.
C&#39;est la méthode recommandée, car avec les dictionnaires créés via DDL :

* Aucun enregistrement supplémentaire n&#39;est ajouté aux fichiers de configuration du serveur.
* Les dictionnaires peuvent être utilisés comme des entités à part entière, au même titre que les tables ou les vues.
* Les données peuvent être lues directement à l&#39;aide de la syntaxe `SELECT` habituelle, plutôt que via les fonctions de table des dictionnaires. Notez que lors d&#39;un accès direct à un dictionnaire via une instruction `SELECT`, un dictionnaire mis en cache ne renverra que les données en cache, tandis qu&#39;un dictionnaire non mis en cache renverra toutes les données qu&#39;il stocke.
* Les dictionnaires peuvent être renommés facilement.

<div id="syntax">
  ### Syntaxe
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Clause                                      | Description                                                                                                                                                                                    |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Attributs](./attributes.md)                | Les attributs du dictionnaire sont définis de manière similaire aux colonnes d&#39;une table. La seule propriété requise est le type ; toutes les autres peuvent avoir des valeurs par défaut. |
| PRIMARY KEY                                 | Définit la ou les colonnes clés pour les recherches dans le dictionnaire. Selon le layout, un ou plusieurs attributs peuvent être spécifiés comme clés.                                        |
| [`SOURCE`](./sources/overview.md)           | Définit la source de données du dictionnaire (par exemple, une table ClickHouse, HTTP, PostgreSQL).                                                                                            |
| [`LAYOUT`](./layouts/overview.md)           | Contrôle la manière dont le dictionnaire est stocké en mémoire (par exemple, `FLAT`, `HASHED`, `CACHE`).                                                                                       |
| [`LIFETIME`](./lifetime.md)                 | Définit l&#39;intervalle de rafraîchissement du dictionnaire.                                                                                                                                  |
| [`ON CLUSTER`](../../../distributed-ddl.md) | Crée le dictionnaire sur un cluster. Facultatif.                                                                                                                                               |
| `SETTINGS`                                  | Paramètres supplémentaires du dictionnaire. Facultatif.                                                                                                                                        |
| `COMMENT`                                   | Ajoute un commentaire au dictionnaire. Facultatif.                                                                                                                                             |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## Créer un dictionnaire à l’aide d’un fichier de configuration
</div>

<CloudNotSupportedBadge />

:::note
La création d’un dictionnaire à l’aide d’un fichier de configuration n’est pas prise en charge dans ClickHouse Cloud. Veuillez utiliser le DDL (voir ci-dessus) et créer votre dictionnaire en tant qu’utilisateur `default`.
:::

Le fichier de configuration du dictionnaire a le format suivant :

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

Vous pouvez configurer autant de dictionnaires que nécessaire dans le même fichier.

<div id="related-content">
  ## Contenu connexe
</div>

* [layout](/fr/sql-reference/statements/create/dictionary/layouts) — Stockage des dictionnaires en mémoire
* [Sources](/fr/sql-reference/statements/create/dictionary/sources) — Connexion à des sources de données
* [Durée de vie](./lifetime.md) — Configuration de l’actualisation automatique
* [Attributs](./attributes.md) — Configuration des clés et des attributs
* [Dictionnaires intégrés](./embedded.md) — Dictionnaires geobase intégrés
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — Table système contenant des informations sur les dictionnaires