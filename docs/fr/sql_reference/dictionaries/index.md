---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_folder_title: Dictionaries
toc_priority: 35
toc_title: Introduction
---

# Dictionnaire {#dictionaries}

Un dictionnaire est une cartographie (`key -> attributes`) qui est pratique pour différents types de listes de référence.

ClickHouse prend en charge des fonctions spéciales pour travailler avec des dictionnaires qui peuvent être utilisés dans les requêtes. Il est plus facile et plus efficace d'utiliser des dictionnaires avec des fonctions que par une `JOIN` avec des tableaux de référence.

[NULL](../syntax.md#null) les valeurs ne peuvent pas être stockées dans un dictionnaire.

Supports ClickHouse:

-   [Construit-dans les dictionnaires](internal_dicts.md#internal_dicts) avec un [ensemble de fonctions](../../sql_reference/functions/ym_dict_functions.md).
-   [Plug-in (externe) dictionnaires](external_dictionaries/external_dicts.md) avec un [net de fonctions](../../sql_reference/functions/ext_dict_functions.md).

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
