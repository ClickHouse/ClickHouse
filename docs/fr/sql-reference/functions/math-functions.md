---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "Math\xE9matique"
---

# Fonctions Mathématiques {#mathematical-functions}

Toutes les fonctions renvoient un nombre Float64. La précision du résultat est proche de la précision maximale possible, mais le résultat peut ne pas coïncider avec le nombre représentable de la machine le plus proche du nombre réel correspondant.

## e() {#e}

Renvoie un nombre Float64 proche du nombre E.

## pi() {#pi}

Returns a Float64 number that is close to the number π.

## exp (x) {#expx}

Accepte un argument numérique et renvoie un Float64 nombre proche de l'exposant de l'argument.

## log(x), ln (x) {#logx-lnx}

Accepte un argument numérique et renvoie un nombre Float64 proche du logarithme naturel de l'argument.

## exp2 (x) {#exp2x}

Accepte un argument numérique et renvoie un nombre Float64 proche de 2 à la puissance de X.

## log2 (x) {#log2x}

Accepte un argument numérique et renvoie un Float64 nombre proximité du logarithme binaire de l'argument.

## exp10 (x) {#exp10x}

Accepte un argument numérique et renvoie un nombre Float64 proche de 10 à la puissance de X.

## log10 (x) {#log10x}

Accepte un argument numérique et renvoie un nombre Float64 proche du logarithme décimal de l'argument.

## sqrt (x) {#sqrtx}

Accepte un argument numérique et renvoie un Float64 nombre proche de la racine carrée de l'argument.

## cbrt (x) {#cbrtx}

Accepte un argument numérique et renvoie un Float64 nombre proche de la racine cubique de l'argument.

## erf (x) {#erfx}

Si ‘x’ est non négatif, alors `erf(x / σ√2)` est la probabilité qu'une variable aléatoire ayant une distribution normale avec un écart type ‘σ’ prend la valeur qui est séparée de la valeur attendue par plus de ‘x’.

Exemple (règle de trois sigma):

``` sql
SELECT erf(3 / sqrt(2))
```

``` text
┌─erf(divide(3, sqrt(2)))─┐
│      0.9973002039367398 │
└─────────────────────────┘
```

## erfc (x) {#erfcx}

Accepte un argument numérique et renvoie un nombre Float64 proche de 1-erf (x), mais sans perte de précision pour ‘x’ valeur.

## lgamma (x) {#lgammax}

Le logarithme de la fonction gamma.

## tgamma (x) {#tgammax}

La fonction Gamma.

## sin (x) {#sinx}

Sine.

## cos (x) {#cosx}

Cosinus.

## tan (x) {#tanx}

Tangente.

## asin (x) {#asinx}

Le sinus d'arc.

## acos (x) {#acosx}

Le cosinus de l'arc.

## atan (x) {#atanx}

L'arc tangente.

## pow(x, y), la puissance(x, y) {#powx-y-powerx-y}

Prend deux arguments numériques x et Y. renvoie un nombre Float64 proche de x à la puissance de Y.

## intExp2 {#intexp2}

Accepte un argument numérique et renvoie un nombre UInt64 proche de 2 à la puissance de X.

## intExp10 {#intexp10}

Accepte un argument numérique et renvoie un nombre UInt64 proche de 10 à la puissance de X.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/math_functions/) <!--hide-->
