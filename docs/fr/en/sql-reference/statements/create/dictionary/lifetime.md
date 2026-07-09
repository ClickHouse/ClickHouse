---
description: 'Configuration LIFETIME du dictionnaire pour l’actualisation automatique'
sidebar_label: 'LIFETIME'
sidebar_position: 5
slug: /sql-reference/statements/create/dictionary/lifetime
title: 'Actualisation des données du dictionnaire avec LIFETIME'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';

ClickHouse met régulièrement à jour les dictionnaires selon la balise `LIFETIME` (définie en secondes).
`LIFETIME` correspond à l’intervalle de mise à jour des dictionnaires entièrement téléchargés et à l’intervalle d’invalidation des dictionnaires mis en cache.

Pendant les mises à jour, l’ancienne version d’un dictionnaire reste interrogeable.
Les mises à jour de dictionnaires ne bloquent pas les requêtes, sauf lors de leur chargement initial.
Si une erreur survient pendant une mise à jour, elle est consignée dans le journal du serveur, et les requêtes peuvent continuer à utiliser l’ancienne version du dictionnaire.
Si la mise à jour d’un dictionnaire réussit, l’ancienne version du dictionnaire est remplacée [de manière atomique](/fr/concepts/glossary#atomicity).

Exemple de paramètres :

<CloudDetails />

```xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

ou

```sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Définir `<lifetime>0</lifetime>` (`LIFETIME(0)`) empêche les dictionnaires de se mettre à jour.

Vous pouvez définir un intervalle de temps pour les mises à jour, et ClickHouse choisira un instant aléatoire suivant une distribution uniforme dans cette plage. Cela est nécessaire pour répartir la charge sur la source du dictionnaire lors des mises à jour sur un grand nombre de serveurs.

Exemple de paramètres :

```xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

or

```sql
LIFETIME(MIN 300 MAX 360)
```

Si `<min>0</min>` et `<max>0</max>`, ClickHouse ne recharge pas le dictionnaire à l’expiration du délai.
Dans ce cas, ClickHouse peut recharger le dictionnaire plus tôt si le fichier de configuration du dictionnaire a été modifié ou si la commande `SYSTEM RELOAD DICTIONARY` a été exécutée.

Lors de la mise à jour des dictionnaires, le serveur ClickHouse applique une logique différente selon le type de [source](./sources/) :

* Pour un fichier texte, il vérifie l’heure de modification. Si cette heure diffère de celle enregistrée précédemment, le dictionnaire est mis à jour.
* Les dictionnaires provenant d’autres sources sont mis à jour à chaque fois par défaut.

Pour les autres sources (ODBC, PostgreSQL, ClickHouse, etc.), vous pouvez définir une requête afin de ne mettre à jour les dictionnaires que s’ils ont réellement changé, plutôt qu’à chaque fois. Pour cela, suivez ces étapes :

* La table du dictionnaire doit comporter un champ dont la valeur change systématiquement lorsque les données source sont mises à jour.
* Les paramètres de la source doivent spécifier une requête qui récupère ce champ. Le serveur ClickHouse interprète le résultat de la requête comme une ligne et, si cette ligne a changé par rapport à son état précédent, le dictionnaire est mis à jour. Spécifiez la requête dans le champ `<invalidate_query>` des paramètres de la [source](./sources/).

Exemple de paramètres :

```xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

ou

```sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

Pour les dictionnaires `Cache`, `ComplexKeyCache`, `SSDCache` et `SSDComplexKeyCache`, les mises à jour synchrones et asynchrones sont toutes deux prises en charge.

Il est également possible, pour les dictionnaires `Flat`, `Hashed`, `HashedArray` et `ComplexKeyHashed`, de demander uniquement les données modifiées depuis la mise à jour précédente. Si `update_field` est spécifié dans la configuration de la source du dictionnaire, la valeur de l’horodatage de la mise à jour précédente, en secondes, sera ajoutée à la requête de données. Selon le type de source (Executable, HTTP, MySQL, PostgreSQL, ClickHouse ou ODBC), une logique différente sera appliquée à `update_field` avant d’interroger une source externe.

* Si la source est HTTP, `update_field` sera ajouté comme paramètre de requête, avec l’heure de la dernière mise à jour comme valeur.
* Si la source est Executable, `update_field` sera ajouté comme argument de script exécutable, avec l’heure de la dernière mise à jour comme valeur de l’argument.
* Si la source est ClickHouse, MySQL, PostgreSQL ou ODBC, une clause `WHERE` supplémentaire sera ajoutée, dans laquelle `update_field` est comparé à l’heure de la dernière mise à jour avec l’opérateur supérieur ou égal.
  * Par défaut, cette condition `WHERE` est vérifiée au niveau le plus élevé de la requête SQL. Il est également possible de vérifier la condition dans n’importe quelle autre clause `WHERE` de la requête à l’aide du mot-clé `{condition}`. Exemple :
    ```sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

Si l’option `update_field` est définie, vous pouvez également définir l’option `update_lag`. La valeur de l’option `update_lag` est soustraite de l’heure de la mise à jour précédente avant la requête des données mises à jour.

Exemple de paramètres :

```xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

ou

```sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```