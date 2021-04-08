---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: Intervalle
---

# Intervalle {#data-type-interval}

Famille de types de données représentant des intervalles d'heure et de date. Les types de la [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) opérateur.

!!! warning "Avertissement"
    `Interval` les valeurs de type de données ne peuvent pas être stockées dans les tables.

Structure:

-   Intervalle de temps en tant que valeur entière non signée.
-   Type de l'intervalle.

Types d'intervalles pris en charge:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

Pour chaque type d'intervalle, il existe un type de données distinct. Par exemple, l' `DAY` l'intervalle correspond au `IntervalDay` type de données:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Utilisation Remarques {#data-type-interval-usage-remarks}

Vous pouvez utiliser `Interval`-tapez des valeurs dans des opérations arithmétiques avec [Date](../../../sql-reference/data-types/date.md) et [DateTime](../../../sql-reference/data-types/datetime.md)-type de valeurs. Par exemple, vous pouvez ajouter 4 jours à l'heure actuelle:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Les intervalles avec différents types ne peuvent pas être combinés. Vous ne pouvez pas utiliser des intervalles comme `4 DAY 1 HOUR`. Spécifiez des intervalles en unités inférieures ou égales à la plus petite unité de l'intervalle, par exemple, l'intervalle `1 day and an hour` l'intervalle peut être exprimée comme `25 HOUR` ou `90000 SECOND`.

Vous ne pouvez pas effectuer d'opérations arithmétiques avec `Interval`- tapez des valeurs, mais vous pouvez ajouter des intervalles de différents types par conséquent aux valeurs dans `Date` ou `DateTime` types de données. Exemple:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

La requête suivante provoque une exception:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## Voir Aussi {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) opérateur
-   [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) type fonctions de conversion
