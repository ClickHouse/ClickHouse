---
alias: []
description: 'Documentation sur le format Buffers'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✔      |       |

<div id="description">
  ## Description
</div>

`Buffers` est un format binaire très simple pour l’échange de données **éphémères**, dans lequel le consumer et le producteur connaissent déjà le schéma et l’ordre des colonnes.

Contrairement à [Native](./Native.md), il ne stocke **pas** les noms de colonnes, les types de colonnes ni aucune métadonnée supplémentaire.

Dans ce format, les données sont écrites et lues par [blocs](/fr/development/architecture#block), au format binaire. Buffers utilise la même représentation binaire par colonne que le format [Native](./Native.md) et respecte les mêmes paramètres du format Native.

Pour chaque bloc, la séquence suivante est écrite :

1. Nombre de colonnes (UInt64, little-endian).
2. Nombre de lignes (UInt64, little-endian).
3. Pour chaque colonne :

* Taille totale, en octets, des données de colonne sérialisées (UInt64, little-endian).
* Octets des données de colonne sérialisées, exactement comme dans le format [Native](./Native.md).

<div id="example-usage">
  ## Exemple d’utilisation
</div>

Écrire dans un fichier :

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

Relisez en indiquant explicitement les types de colonnes :

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

Si vous avez une table avec les mêmes types de colonnes, vous pouvez la remplir directement :

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

Examinez la table :

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## Paramètres de format
</div>
