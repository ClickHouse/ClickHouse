---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "La G\xE9n\xE9ration De Nombres Pseudo-Al\xE9atoires"
---

# Fonctions pour générer des nombres Pseudo-aléatoires {#functions-for-generating-pseudo-random-numbers}

Des générateurs Non cryptographiques de nombres pseudo-aléatoires sont utilisés.

Toutes les fonctions acceptent zéro argument ou un argument.
Si un argument est passé, il peut être de n'importe quel type, et sa valeur n'est utilisée pour rien.
Le seul but de cet argument est d'empêcher l'élimination des sous-expressions courantes, de sorte que deux instances différentes de la même fonction renvoient des colonnes différentes avec des nombres aléatoires différents.

## Rand {#rand}

Renvoie un nombre UInt32 pseudo-aléatoire, réparti uniformément entre tous les nombres de type UInt32.
Utilise un générateur congruentiel linéaire.

## rand64 {#rand64}

Renvoie un nombre UInt64 pseudo-aléatoire, réparti uniformément entre tous les nombres de type UInt64.
Utilise un générateur congruentiel linéaire.

## randConstant {#randconstant}

Produit une colonne constante avec une valeur aléatoire.

**Syntaxe**

``` sql
randConstant([x])
```

**Paramètre**

-   `x` — [Expression](../syntax.md#syntax-expressions) résultant de la [types de données pris en charge](../data-types/index.md#data_types). La valeur résultante est ignorée, mais l'expression elle-même si elle est utilisée pour contourner [élimination des sous-expressions courantes](index.md#common-subexpression-elimination) si la fonction est appelée plusieurs fois dans une seule requête. Paramètre facultatif.

**Valeur renvoyée**

-   Nombre Pseudo-aléatoire.

Type: [UInt32](../data-types/int-uint.md).

**Exemple**

Requête:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

Résultat:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
