---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Dictionnaires de polygones'
sidebar_label: 'Polygon'
sidebar_position: 12
description: 'Configurez les dictionnaires de polygones pour les recherches de type point-dans-polygone.'
doc_type: 'référence'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Le dictionnaire `polygon` (`POLYGON`) est optimisé pour les requêtes de type point-dans-polygone, autrement dit des recherches de « géocodage inversé ».
À partir d’une coordonnée (latitude/longitude), il détermine efficacement quel polygone/région (parmi un grand nombre de polygones, comme des frontières de pays ou de régions) contient ce point.
Il est particulièrement bien adapté pour associer des coordonnées géographiques à leur région correspondante.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="Dictionnaires Polygon dans ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

Exemple de configuration d’un dictionnaire polygon :

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Fichier de configuration">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
            </key>

            <attribute>
                <name>name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>

        <layout>
            <polygon>
                <store_polygon_key_column>1</store_polygon_key_column>
            </polygon>
        </layout>

        ...
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />

Lors de la configuration du dictionnaire polygon, la clé doit avoir l’un des deux types suivants :

* Un polygone simple. Il s’agit d’un tableau de points.
* MultiPolygon. Il s’agit d’un tableau de polygones. Chaque polygone est un tableau bidimensionnel de points. Le premier élément de ce tableau correspond au contour extérieur du polygone, et les éléments suivants définissent les zones à en exclure.

Les points peuvent être spécifiés sous la forme d’un tableau ou d’un tuple de coordonnées. Dans l’implémentation actuelle, seuls les points bidimensionnels sont pris en charge.

L’utilisateur peut téléverser ses propres données dans tous les formats pris en charge par ClickHouse.

Il existe 3 types de [stockage en mémoire](./#storing-dictionaries-in-memory) :

| Layout               | Description                                                                                                                                                                                                                                                                                                                                                                                                                  |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | Implémentation naïve. Un parcours linéaire de tous les polygones est effectué pour chaque requête, en vérifiant l’appartenance sans index supplémentaire.                                                                                                                                                                                                                                                                    |
| `POLYGON_INDEX_EACH` | Un index distinct est créé pour chaque polygone, ce qui permet des vérifications d’appartenance rapides dans la plupart des cas (optimisé pour les régions géographiques). Une grille est superposée à la zone, divisant récursivement les cellules en 16 parties égales. La division s’arrête lorsque la profondeur de récursion atteint `MAX_DEPTH` ou qu’une cellule ne croise pas plus de `MIN_INTERSECTIONS` polygones. |
| `POLYGON_INDEX_CELL` | Crée également la grille décrite ci-dessus avec les mêmes options. Pour chaque cellule feuille, un index est construit sur tous les fragments de polygone qu’elle contient, ce qui permet d’obtenir rapidement une réponse aux requêtes.                                                                                                                                                                                     |
| `POLYGON`            | Synonyme de `POLYGON_INDEX_CELL`.                                                                                                                                                                                                                                                                                                                                                                                            |

Les requêtes sur les dictionnaires s’effectuent à l’aide des [fonctions](/fr/sql-reference/functions/ext-dict-functions.md) standard de manipulation des dictionnaires.
Une différence importante ici est que les clés sont les points pour lesquels vous voulez trouver le polygone qui les contient.

**Exemple**

Exemple d’utilisation du dictionnaire défini ci-dessus :

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

Lors de l’exécution de la dernière commande pour chaque point de la table &#39;points&#39;, un polygone d’aire minimale contenant ce point sera déterminé, et les attributs demandés seront renvoyés.

**Exemple**

Vous pouvez lire les colonnes des dictionnaires de polygones via une requête SELECT : il vous suffit d’activer `store_polygon_key_column = 1` dans la configuration du dictionnaire ou dans la requête DDL correspondante.

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```