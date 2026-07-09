---
description: 'La fonction de table permet de lire des données à partir du cluster YTsaurus.'
sidebar_label: 'ytsaurus'
sidebar_position: 85
slug: /sql-reference/table-functions/ytsaurus
title: 'ytsaurus'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="ytsaurus-table-function">
  # Fonction de table ytsaurus
</div>

<ExperimentalBadge />

La fonction de table permet de lire des données à partir du cluster YTsaurus.

<div id="syntax">
  ## Syntaxe
</div>

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

:::info
Il s’agit d’une fonctionnalité expérimentale qui pourra évoluer de manière incompatible avec les versions précédentes dans les versions ultérieures.
Activez l’utilisation de la fonction de table YTsaurus
avec le paramètre [allow&#95;experimental&#95;ytsaurus&#95;table&#95;function](/fr/operations/settings/settings#allow_experimental_ytsaurus_table_engine).
Entrez la commande `set allow_experimental_ytsaurus_table_function = 1`.
:::

<div id="arguments">
  ## Arguments
</div>

* `http_proxy_url` — URL du proxy HTTP de YTsaurus.
* `cypress_path` — Chemin Cypress de la source de données.
* `oauth_token` — Jeton OAuth.
* `format` — Le [format](/fr/interfaces/formats) de la source de données.

**Valeur retournée**

Table ayant la structure spécifiée pour lire les données du chemin Cypress YTsaurus spécifié dans le cluster YTsaurus.

**Voir aussi**

* [moteur YTsaurus](/fr/engines/table-engines/integrations/ytsaurus.md)