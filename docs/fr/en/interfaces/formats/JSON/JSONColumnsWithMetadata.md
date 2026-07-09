---
alias: []
description: 'Documentation sur le format JSONColumnsWithMetadata'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Se distingue du format [`JSONColumns`](./JSONColumns.md) en ce qu’il contient aussi des métadonnées et des statistiques (comme le format [`JSON`](./JSON.md)).

:::note
Le format `JSONColumnsWithMetadata` met toutes les données en mémoire tampon, puis les restitue en un seul bloc, ce qui peut entraîner une consommation mémoire élevée.
:::

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Exemple :

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
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

Pour le format d&#39;entrée `JSONColumnsWithMetadata`, si le paramètre [`input_format_json_validate_types_from_metadata`](/fr/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) est défini sur `1`,
les types indiqués dans les métadonnées des données d&#39;entrée seront comparés à ceux des colonnes correspondantes de la table.

<div id="format-settings">
  ## Paramètres de format
</div>
