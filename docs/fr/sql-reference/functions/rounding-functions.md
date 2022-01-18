---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Arrondi
---

# Fonctions D'Arrondi {#rounding-functions}

## floor(x\[, N\]) {#floorx-n}

Renvoie le plus grand nombre rond inférieur ou égal à `x`. Un nombre rond est un multiple de 1 / 10N, ou le nombre le plus proche du type de données approprié si 1 / 10N n'est pas exact.
‘N’ est une constante entière, paramètre facultatif. Par défaut, il est zéro, ce qui signifie arrondir à un entier.
‘N’ peut être négative.

Exemple: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` est n'importe quel type numérique. Le résultat est un nombre du même type.
Pour les arguments entiers, il est logique d'arrondir avec un négatif `N` valeur (pour non négatif `N`, la fonction ne fait rien).
Si l'arrondi provoque un débordement (par exemple, floor(-128, -1)), un résultat spécifique à l'implémentation est renvoyé.

## ceil(x\[, n\]), plafond (x\[, n\]) {#ceilx-n-ceilingx-n}

Renvoie le plus petit nombre rond supérieur ou égal à `x`. Dans tous les autres sens, il est le même que le `floor` fonction (voir ci-dessus).

## trunc(x \[, N\]), truncate(x \[, N\]) {#truncx-n-truncatex-n}

Renvoie le nombre rond avec la plus grande valeur absolue qui a une valeur absolue inférieure ou égale à `x`‘s. In every other way, it is the same as the ’floor’ fonction (voir ci-dessus).

## round(x\[, N\]) {#rounding_functions-round}

Arrondit une valeur à un nombre spécifié de décimales.

La fonction renvoie le nombre plus proche de l'ordre spécifié. Dans le cas où un nombre donné a une distance égale aux nombres environnants, la fonction utilise l'arrondi de banquier pour les types de nombres flottants et arrondit à partir de zéro pour les autres types de nombres.

``` sql
round(expression [, decimal_places])
```

**Paramètre:**

-   `expression` — A number to be rounded. Can be any [expression](../syntax.md#syntax-expressions) retour du numérique [type de données](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   Si `decimal-places > 0` alors la fonction arrondit la valeur à droite du point décimal.
    -   Si `decimal-places < 0` alors la fonction arrondit la valeur à gauche de la virgule décimale.
    -   Si `decimal-places = 0` alors la fonction arrondit la valeur à l'entier. Dans ce cas, l'argument peut être omis.

**Valeur renvoyée:**

Le nombre arrondi du même type que le nombre d'entrée.

### Exemple {#examples}

**Exemple d'utilisation**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**Des exemples de l'arrondissement**

Le résultat est arrondi au plus proche.

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Le Banquier arrondit.

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**Voir Aussi**

-   [roundBankers](#roundbankers)

## roundBankers {#roundbankers}

Arrondit un nombre à une position décimale spécifiée.

-   Si le nombre est arrondi à mi-chemin entre deux nombres, la fonction utilise l'arrondi.

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   Dans d'autres cas, la fonction arrondit les nombres à l'entier le plus proche.

À l'aide de l'arrondi, vous pouvez réduire l'effet qu'arrondir les nombres sur les résultats d'additionner ou de soustraire ces chiffres.

Par exemple, les nombres de somme 1.5, 2.5, 3.5, 4.5 avec des arrondis différents:

-   Pas d'arrondi: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   Arrondi du banquier: 2 + 2 + 4 + 4 = 12.
-   Arrondi à l'entier le plus proche: 2 + 3 + 4 + 5 = 14.

**Syntaxe**

``` sql
roundBankers(expression [, decimal_places])
```

**Paramètre**

-   `expression` — A number to be rounded. Can be any [expression](../syntax.md#syntax-expressions) retour du numérique [type de données](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**Valeur renvoyée**

Valeur arrondie par la méthode d'arrondi du banquier.

### Exemple {#examples-1}

**Exemple d'utilisation**

Requête:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

Résultat:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**Exemples d'arrondi bancaire**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**Voir Aussi**

-   [rond](#rounding_functions-round)

## roundToExp2 (num) {#roundtoexp2num}

Accepte un certain nombre. Si le nombre est inférieur à un, elle renvoie 0. Sinon, il arrondit le nombre au degré le plus proche (entier non négatif) de deux.

## roundDuration (num) {#rounddurationnum}

Accepte un certain nombre. Si le nombre est inférieur à un, elle renvoie 0. Sinon, il arrondit le nombre vers le bas pour les nombres de l'ensemble: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. Cette fonction est spécifique à Yandex.Metrica et utilisé pour la mise en œuvre du rapport sur la durée de la session.

## roundAge (num) {#roundagenum}

Accepte un certain nombre. Si le nombre est inférieur à 18, il renvoie 0. Sinon, il arrondit le nombre à un nombre de l'ensemble: 18, 25, 35, 45, 55. Cette fonction est spécifique à Yandex.Metrica et utilisé pour la mise en œuvre du rapport sur l'âge des utilisateurs.

## roundDown(num, arr) {#rounddownnum-arr}

Accepte un nombre et l'arrondit à un élément dans le tableau spécifié. Si la valeur est inférieure à la plus basse, la plus basse lié est retourné.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
