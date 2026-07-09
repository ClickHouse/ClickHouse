---
alias: []
description: 'Documentation sur le format Npy'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'référence'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

Le format `Npy` est conçu pour charger un tableau NumPy à partir d’un fichier `.npy` dans ClickHouse.
Le format de fichier NumPy est un format binaire utilisé pour stocker efficacement des tableaux de données numériques.
Lors de l’importation, ClickHouse considère la dimension de plus haut niveau comme un tableau de lignes à colonne unique.

Le tableau ci-dessous présente les types de données Npy pris en charge ainsi que leur type correspondant dans ClickHouse :

<div id="data_types-matching">
  ## Correspondance des types de données
</div>

| Type de données Npy (`INSERT`) | Type de données ClickHouse                              | Type de données Npy (`SELECT`) |
| ------------------------------ | ------------------------------------------------------- | ------------------------------ |
| `i1`                           | [Int8](/fr/sql-reference/data-types/int-uint.md)           | `i1`                           |
| `i2`                           | [Int16](/fr/sql-reference/data-types/int-uint.md)          | `i2`                           |
| `i4`                           | [Int32](/fr/sql-reference/data-types/int-uint.md)          | `i4`                           |
| `i8`                           | [Int64](/fr/sql-reference/data-types/int-uint.md)          | `i8`                           |
| `u1`, `b1`                     | [UInt8](/fr/sql-reference/data-types/int-uint.md)          | `u1`                           |
| `u2`                           | [UInt16](/fr/sql-reference/data-types/int-uint.md)         | `u2`                           |
| `u4`                           | [UInt32](/fr/sql-reference/data-types/int-uint.md)         | `u4`                           |
| `u8`                           | [UInt64](/fr/sql-reference/data-types/int-uint.md)         | `u8`                           |
| `f2`, `f4`                     | [Float32](/fr/sql-reference/data-types/float.md)           | `f4`                           |
| `f8`                           | [Float64](/fr/sql-reference/data-types/float.md)           | `f8`                           |
| `S`, `U`                       | [String](/fr/sql-reference/data-types/string.md)           | `S`                            |
|                                | [FixedString](/fr/sql-reference/data-types/fixedstring.md) | `S`                            |

<div id="example-usage">
  ## Exemple d’utilisation
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### Enregistrer un tableau au format .npy avec Python
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### Lire un fichier NumPy dans ClickHouse
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### Sélection de données
</div>

Vous pouvez sélectionner des données à partir d’une table ClickHouse et les enregistrer dans un fichier au format Npy à l’aide de la commande suivante avec clickhouse-client :

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## Paramètres du format
</div>
