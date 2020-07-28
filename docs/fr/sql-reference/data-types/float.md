---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: Float32, Float64
---

# Float32, Float64 {#float32-float64}

[Les nombres à virgule flottante](https://en.wikipedia.org/wiki/IEEE_754).

Les Types sont équivalents aux types de C:

-   `Float32` - `float`
-   `Float64` - `double`

Nous vous recommandons de stocker les données sous forme entière chaque fois que possible. Par exemple, convertissez des nombres de précision fixes en valeurs entières, telles que des montants monétaires ou des temps de chargement de page en millisecondes.

## Utilisation de nombres à virgule flottante {#using-floating-point-numbers}

-   Calculs avec des nombres à virgule flottante peut produire une erreur d'arrondi.

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   Le résultat du calcul dépend de la méthode de calcul (le type de processeur et de l'architecture du système informatique).
-   Les calculs à virgule flottante peuvent entraîner des nombres tels que l'infini (`Inf`) et “not-a-number” (`NaN`). Cela doit être pris en compte lors du traitement des résultats de calculs.
-   Lors de l'analyse de nombres à virgule flottante à partir de texte, le résultat peut ne pas être le nombre représentable par machine le plus proche.

## NaN et Inf {#data_type-float-nan-inf}

Contrairement à SQL standard, ClickHouse prend en charge les catégories suivantes de nombres à virgule flottante:

-   `Inf` – Infinity.

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

-   `-Inf` – Negative infinity.

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

-   `NaN` – Not a number.

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

    See the rules for `NaN` sorting in the section [ORDER BY clause](../sql_reference/statements/select/order-by.md).

[Article Original](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
