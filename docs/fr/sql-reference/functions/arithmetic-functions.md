---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "Arithm\xE9tique"
---

# Fonctions Arithmétiques {#arithmetic-functions}

Pour toutes les fonctions arithmétiques, le type de résultat est calculé comme le plus petit type de nombre dans lequel le résultat correspond, s'il existe un tel type. Le minimum est pris simultanément sur la base du nombre de bits, s'il est signé, et s'il flotte. S'il n'y a pas assez de bits, le type de bits le plus élevé est pris.

Exemple:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Les fonctions arithmétiques fonctionnent pour n'importe quelle paire de types de UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32 ou Float64.

Le débordement est produit de la même manière qu'en C++.

## plus (A, B), opérateur a + b {#plusa-b-a-b-operator}

Calcule la somme des nombres.
Vous pouvez également ajouter des nombres entiers avec une date ou la date et l'heure. Dans le cas d'une date, Ajouter un entier signifie ajouter le nombre de jours correspondant. Pour une date avec l'heure, cela signifie ajouter le nombre de secondes correspondant.

## moins (A, B), opérateur a - b {#minusa-b-a-b-operator}

Calcule la différence. Le résultat est toujours signé.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## la multiplication(a, b), a \* et b \* de l'opérateur {#multiplya-b-a-b-operator}

Calcule le produit des nombres.

## diviser (A, B), opérateur a / b {#dividea-b-a-b-operator}

Calcule le quotient des nombres. Le type de résultat est toujours un type à virgule flottante.
Il n'est pas de division entière. Pour la division entière, utilisez le ‘intDiv’ fonction.
En divisant par zéro vous obtenez ‘inf’, ‘-inf’, ou ‘nan’.

## intDiv (a, b) {#intdiva-b}

Calcule le quotient des nombres. Divise en entiers, arrondi vers le bas (par la valeur absolue).
Une exception est levée en divisant par zéro ou en divisant un nombre négatif minimal par moins un.

## intDivOrZero(a, b) {#intdivorzeroa-b}

Diffère de ‘intDiv’ en ce sens qu'il renvoie zéro en divisant par zéro ou en divisant un nombre négatif minimal par moins un.

## opérateur modulo(A, B), A % B {#moduloa-b-a-b-operator}

Calcule le reste après la division.
Si les arguments sont des nombres à virgule flottante, ils sont pré-convertis en entiers en supprimant la partie décimale.
Le reste est pris dans le même sens qu'en C++. La division tronquée est utilisée pour les nombres négatifs.
Une exception est levée en divisant par zéro ou en divisant un nombre négatif minimal par moins un.

## moduloOrZero (a, b) {#moduloorzeroa-b}

Diffère de ‘modulo’ en ce sens qu'il renvoie zéro lorsque le diviseur est nul.

## annuler (a), - un opérateur {#negatea-a-operator}

Calcule un nombre avec le signe inverse. Le résultat est toujours signé.

## abs(un) {#arithm_func-abs}

Calcule la valeur absolue d'un nombre (un). Autrement dit, si un \< 0, Il renvoie-A. pour les types non signés, il ne fait rien. Pour les types entiers signés, il renvoie un nombre non signé.

## pgcd(a, b) {#gcda-b}

Renvoie le plus grand diviseur commun des nombres.
Une exception est levée en divisant par zéro ou en divisant un nombre négatif minimal par moins un.

## ppcm(a, b) {#lcma-b}

Renvoie le multiple le moins commun des nombres.
Une exception est levée en divisant par zéro ou en divisant un nombre négatif minimal par moins un.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
