---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "Fractionnement et fusion de cha\xEEnes et de tableaux"
---

# Fonctions pour diviser et fusionner des chaînes et des tableaux {#functions-for-splitting-and-merging-strings-and-arrays}

## splitByChar (séparateur, s) {#splitbycharseparator-s}

Divise une chaîne en sous-chaînes séparées par un caractère spécifique. Il utilise une chaîne constante `separator` qui composé d'un seul caractère.
Retourne un tableau de certaines chaînes. Les sous-chaînes vides peuvent être sélectionnées si le séparateur se produit au début ou à la fin de la chaîne, ou s'il existe plusieurs séparateurs consécutifs.

**Syntaxe**

``` sql
splitByChar(<separator>, <s>)
```

**Paramètre**

-   `separator` — The separator which should contain exactly one character. [Chaîne](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée(s)**

Retourne un tableau de certaines chaînes. Des sous-chaînes vides peuvent être sélectionnées lorsque:

-   Un séparateur se produit au début ou à la fin de la chaîne;
-   Il existe plusieurs séparateurs consécutifs;
-   La chaîne d'origine `s` est vide.

Type: [Tableau](../../sql-reference/data-types/array.md) de [Chaîne](../../sql-reference/data-types/string.md).

**Exemple**

``` sql
SELECT splitByChar(',', '1,2,3,abcde')
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString(séparateur, s) {#splitbystringseparator-s}

Divise une chaîne en sous-chaînes séparées par une chaîne. Il utilise une chaîne constante `separator` de plusieurs caractères comme séparateur. Si la chaîne `separator` est vide, il va diviser la chaîne `s` dans un tableau de caractères uniques.

**Syntaxe**

``` sql
splitByString(<separator>, <s>)
```

**Paramètre**

-   `separator` — The separator. [Chaîne](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [Chaîne](../../sql-reference/data-types/string.md).

**Valeur renvoyée(s)**

Retourne un tableau de certaines chaînes. Des sous-chaînes vides peuvent être sélectionnées lorsque:

Type: [Tableau](../../sql-reference/data-types/array.md) de [Chaîne](../../sql-reference/data-types/string.md).

-   Un séparateur non vide se produit au début ou à la fin de la chaîne;
-   Il existe plusieurs séparateurs consécutifs non vides;
-   La chaîne d'origine `s` est vide tandis que le séparateur n'est pas vide.

**Exemple**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde')
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## arrayStringConcat(arr \[, séparateur\]) {#arraystringconcatarr-separator}

Concatène les chaînes répertoriées dans le tableau avec le séparateur."séparateur" est un paramètre facultatif: une chaîne constante, définie à une chaîne vide par défaut.
Retourne une chaîne de caractères.

## alphaTokens (s) {#alphatokenss}

Sélectionne des sous-chaînes d'octets consécutifs dans les plages A-z et A-Z. retourne un tableau de sous-chaînes.

**Exemple**

``` sql
SELECT alphaTokens('abca1abc')
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
