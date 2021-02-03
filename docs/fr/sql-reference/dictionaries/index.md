---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Dictionnaire
toc_priority: 35
toc_title: Introduction
---

# Dictionnaire {#dictionaries}

Un dictionnaire est une cartographie (`key -> attributes`) qui est pratique pour différents types de listes de référence.

ClickHouse prend en charge des fonctions spéciales pour travailler avec des dictionnaires qui peuvent être utilisés dans les requêtes. Il est plus facile et plus efficace d'utiliser des dictionnaires avec des fonctions que par une `JOIN` avec des tableaux de référence.

[NULL](../../sql-reference/syntax.md#null-literal) les valeurs ne peuvent pas être stockées dans un dictionnaire.

Supports ClickHouse:

-   [Construit-dans les dictionnaires](internal-dicts.md#internal_dicts) avec un [ensemble de fonctions](../../sql-reference/functions/ym-dict-functions.md).
-   [Plug-in (externe) dictionnaires](external-dictionaries/external-dicts.md#dicts-external-dicts) avec un [ensemble de fonctions](../../sql-reference/functions/ext-dict-functions.md).

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
