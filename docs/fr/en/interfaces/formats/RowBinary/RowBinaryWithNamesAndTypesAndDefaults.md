---
alias: []
description: 'Documentation sur le format RowBinaryWithNamesAndTypesAndDefaults'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrée | Sortie | Alias |
| ------ | ------ | ----- |
| ✔      | ✗      |       |

<div id="description">
  ## Description
</div>

Semblable au format [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md), mais avec un octet supplémentaire avant chaque cellule indiquant si la valeur `DEFAULT` de la colonne doit être utilisée — exactement comme dans le format [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md). Cette combinaison prend en charge les `INSERT` avec évolution du schéma : l’émetteur peut omettre des colonnes de l’en-tête (elles reçoivent alors le `DEFAULT` de la colonne cible) et, pour toute colonne qu’il envoie, il peut marquer certaines cellules individuelles comme devant utiliser le `DEFAULT` de la colonne, sans les confondre avec `NULL`.

Ce format est utilisé uniquement en entrée.

<div id="wire-format">
  ## Format de transmission
</div>

L’en-tête est identique à [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) :

1. Un `VarUInt` indiquant le nombre de colonnes `N`.
2. `N` chaînes `String` préfixées par leur longueur, contenant les noms de colonnes.
3. `N` types de colonnes — soit des noms textuels, soit un encodage binaire compact, contrôlé par les paramètres `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format`.

Après l’en-tête, chaque ligne se compose de `N` cellules. Pour chaque cellule :

* Un unique octet marqueur `UInt8`.
  * `0x01` — utilise l’expression `DEFAULT` de la colonne cible. Aucun octet de valeur ne suit.
  * `0x00` — une valeur suit, sérialisée à l’aide du sérialiseur `RowBinary` du type de colonne. Pour `Nullable(T)`, les octets de valeur commencent par l’octet NULL de `Nullable` (`0` pour non-NULL, `1` pour NULL), puis par la valeur interne si elle n’est pas NULL.

<div id="defaults-vs-null">
  ## Valeurs par défaut vs NULL
</div>

Le marqueur par défaut de chaque cellule et l’octet nul intégré à `Nullable` sont indépendants. Une colonne `Nullable(UInt32) DEFAULT 42` peut être envoyée de trois façons différentes pour chaque ligne :

| Octets    | Signification                                         |
| --------- | ----------------------------------------------------- |
| `01`      | Utiliser `DEFAULT 42`.                                |
| `00 01`   | Chemin de valeur, puis `NULL` via le type `Nullable`. |
| `00 00 …` | Chemin de valeur, puis une valeur interne non nulle.  |

<div id="schema-evolution">
  ## Évolution du schéma
</div>

| Cas                                                                  | Comportement                                                                                                                                                                                           |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Colonne entièrement absente de l&#39;en-tête du fichier              | Remplie dans la cible via `insertDefaultsForNotSeenColumns` ; contrôlée par `defaults_for_omitted_fields`.                                                                                             |
| Colonne présente dans l&#39;en-tête, marqueur de cellule `0x01`      | `insertDefault` pour chaque ligne.                                                                                                                                                                     |
| Colonne présente dans l&#39;en-tête, marqueur de cellule `0x00`      | La valeur est interprétée normalement.                                                                                                                                                                 |
| Colonne supplémentaire dans l&#39;en-tête, absente de la table cible | Ignorée silencieusement lorsque `input_format_skip_unknown_fields = 1` (le marqueur est d&#39;abord consommé ; si `0x01`, rien d&#39;autre ; si `0x00`, la valeur typée est interprétée puis ignorée). |

<div id="example-usage">
  ## Exemple d’utilisation
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* L’en-tête contient une colonne nommée `x` de type `Nullable(UInt32)`.
* L’unique cellule utilise le marqueur `0x01`, ce qui signifie &quot;utiliser `DEFAULT 42`&quot;.

<div id="format-settings">
  ## Paramètres de format
</div>

<RowBinaryFormatSettings />