---
alias: []
description: 'Documentation sur le format JSON'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `JSON` lit et produit des données au format JSON.

Le format `JSON` renvoie les éléments suivants :

| Paramètre                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meta`                       | Noms et types des colonnes.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `data`                       | Tables de données                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `rows`                       | Le nombre total de lignes en sortie.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rows_before_limit_at_least` | L&#39;estimation minimale du nombre de lignes qu&#39;il y aurait eu sans LIMIT. Elle n&#39;est renvoyée que si la query contient LIMIT. Cette estimation est calculée à partir des blocks de données traités dans le query pipeline avant la transformation LIMIT, mais ces blocks peuvent ensuite être ignorés par cette transformation. Si les blocks n&#39;atteignent même pas la transformation LIMIT dans le query pipeline, ils ne sont pas pris en compte dans l&#39;estimation. |
| `statistics`                 | Des statistiques telles que `elapsed`, `rows_read`, `bytes_read`.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `totals`                     | Valeurs totales (lors de l&#39;utilisation de WITH TOTALS).                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `extremes`                   | Valeurs extrêmes (lorsque `extremes` vaut 1).                                                                                                                                                                                                                                                                                                                                                                                                                                           |

Le type `JSON` est compatible avec JavaScript. Pour le garantir, certains caractères sont également échappés :

* la barre oblique `/` est échappée sous la forme `\/`
* les sauts de ligne alternatifs `U+2028` et `U+2029`, qui posent problème dans certains navigateurs, sont échappés sous la forme `\uXXXX`.
* les caractères de contrôle ASCII sont échappés : retour arrière, saut de page, line feed, retour chariot et tabulation horizontale sont remplacés par `\b`, `\f`, `\n`, `\r`, `\t`, tandis que les autres octets de la plage 00-1F sont remplacés par des séquences `\uXXXX`.
* les séquences UTF-8 invalides sont remplacées par le caractère de remplacement � afin que le texte de sortie soit constitué de séquences UTF-8 valides.

Pour des raisons de compatibilité avec JavaScript, les entiers Int64 et UInt64 sont entourés de guillemets doubles par défaut.
Pour supprimer ces guillemets, vous pouvez définir le paramètre de configuration [`output_format_json_quote_64bit_integers`](/fr/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) sur `0`.

ClickHouse prend en charge [NULL](/fr/sql-reference/syntax.md), qui s&#39;affiche comme `null` dans la sortie JSON. Pour activer les valeurs `+nan`, `-nan`, `+inf`, `-inf` dans la sortie, définissez [output&#95;format&#95;json&#95;quote&#95;denormals](/fr/operations/settings/settings-formats.md/#output_format_json_quote_denormals) sur `1`.

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple :

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## Paramètres de format
</div>

Pour le format d&#39;entrée JSON, si le paramètre [`input_format_json_validate_types_from_metadata`](/fr/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) est défini sur `1`,
les types issus des métadonnées des données d&#39;entrée seront comparés aux types des colonnes correspondantes de la table.

<div id="see-also">
  ## Voir aussi
</div>

* format [JSONEachRow](/fr/interfaces/formats/JSONEachRow)
* réglage [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/fr/operations/settings/settings-formats.md/#output_format_json_array_of_rows)