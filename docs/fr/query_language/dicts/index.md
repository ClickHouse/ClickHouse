---
machine_translated: true
---

# Dictionnaire {#dictionaries}

Un dictionnaire est une cartographie (`key -> attributes`) qui est pratique pour différents types de listes de référence.

ClickHouse prend en charge des fonctions spéciales pour travailler avec des dictionnaires qui peuvent être utilisés dans les requêtes. Il est plus facile et plus efficace d'utiliser des dictionnaires avec des fonctions que par une `JOIN` avec des tableaux de référence.

[NULL](../syntax.md#null) les valeurs ne peuvent pas être stockées dans un dictionnaire.

Supports ClickHouse:

-   [Construit-dans les dictionnaires](internal_dicts.md#internal_dicts) avec un [ensemble de fonctions](../functions/ym_dict_functions.md).
-   [Plug-in (externe) dictionnaires](external_dicts.md) avec un [ensemble de fonctions](../functions/ext_dict_functions.md).

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
