---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: Bit
---

# Peu De Fonctions {#bit-functions}

Les fonctions Bit fonctionnent pour n'importe quelle paire de types de UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32 ou Float64.

Le type de résultat est un entier avec des bits égaux aux bits maximum de ses arguments. Si au moins l'un des arguments est signé, le résultat est un signé nombre. Si un argument est un nombre à virgule flottante, Il est converti en Int64.

## bitAnd (a, b) {#bitanda-b}

## bitOr (a, b) {#bitora-b}

## bitXor (a, b) {#bitxora-b}

## bitNot (a) {#bitnota}

## bitShiftLeft (A, b) {#bitshiftlefta-b}

## bitShiftRight (A, b) {#bitshiftrighta-b}

## bitRotateLeft (a, b) {#bitrotatelefta-b}

## bitRotateRight (a, b) {#bitrotaterighta-b}

## bitTest {#bittest}

Prend tout entier et le convertit en [forme binaire](https://en.wikipedia.org/wiki/Binary_number) renvoie la valeur d'un bit à la position spécifiée. Le compte à rebours commence à partir de 0 de la droite vers la gauche.

**Syntaxe**

``` sql
SELECT bitTest(number, index)
```

**Paramètre**

-   `number` – integer number.
-   `index` – position of bit.

**Valeurs renvoyées**

Renvoie une valeur de bit à la position spécifiée.

Type: `UInt8`.

**Exemple**

Par exemple, le nombre 43 dans le système numérique de base-2 (binaire) est 101011.

Requête:

``` sql
SELECT bitTest(43, 1)
```

Résultat:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Un autre exemple:

Requête:

``` sql
SELECT bitTest(43, 2)
```

Résultat:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Renvoie le résultat de [logique de conjonction](https://en.wikipedia.org/wiki/Logical_conjunction) (Et opérateur) de tous les bits à des positions données. Le compte à rebours commence à partir de 0 de la droite vers la gauche.

La conjonction pour les opérations bit à bit:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**Syntaxe**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Paramètre**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) est vrai si et seulement si toutes ses positions sont remplies (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**Valeurs renvoyées**

Retourne le résultat de la conjonction logique.

Type: `UInt8`.

**Exemple**

Par exemple, le nombre 43 dans le système numérique de base-2 (binaire) est 101011.

Requête:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

Résultat:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Un autre exemple:

Requête:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

Résultat:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Renvoie le résultat de [disjonction logique](https://en.wikipedia.org/wiki/Logical_disjunction) (Ou opérateur) de tous les bits à des positions données. Le compte à rebours commence à partir de 0 de la droite vers la gauche.

La disjonction pour les opérations binaires:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**Syntaxe**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Paramètre**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit.

**Valeurs renvoyées**

Renvoie le résultat de la disjuction logique.

Type: `UInt8`.

**Exemple**

Par exemple, le nombre 43 dans le système numérique de base-2 (binaire) est 101011.

Requête:

``` sql
SELECT bitTestAny(43, 0, 2)
```

Résultat:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Un autre exemple:

Requête:

``` sql
SELECT bitTestAny(43, 4, 2)
```

Résultat:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

Calcule le nombre de bits mis à un dans la représentation binaire d'un nombre.

**Syntaxe**

``` sql
bitCount(x)
```

**Paramètre**

-   `x` — [Entier](../../sql-reference/data-types/int-uint.md) ou [virgule flottante](../../sql-reference/data-types/float.md) nombre. La fonction utilise la représentation de la valeur en mémoire. Il permet de financer les nombres à virgule flottante.

**Valeur renvoyée**

-   Nombre de bits défini sur un dans le numéro d'entrée.

La fonction ne convertit pas la valeur d'entrée en un type plus grand ([l'extension du signe](https://en.wikipedia.org/wiki/Sign_extension)). Ainsi, par exemple, `bitCount(toUInt8(-1)) = 8`.

Type: `UInt8`.

**Exemple**

Prenez par exemple le numéro 333. Sa représentation binaire: 0000000101001101.

Requête:

``` sql
SELECT bitCount(333)
```

Résultat:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
