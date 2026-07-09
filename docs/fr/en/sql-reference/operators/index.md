---
description: 'Documentation des opérateurs'
sidebar_label: 'Opérateurs'
sidebar_position: 38
slug: /sql-reference/operators/
title: 'Opérateurs'
doc_type: 'reference'
---

ClickHouse transforme les opérateurs en fonctions correspondantes lors de l’analyse syntaxique de la requête, selon leur priorité, leur préséance et leur associativité.

<div id="access-operators">
  ## Opérateurs d&#39;accès
</div>

`a[N]` – Accès à un élément d’un tableau. La fonction `arrayElement(a, N)`.

`a.N` – Accès à un élément d’un tuple. La fonction `tupleElement(a, N)`.

<div id="numeric-negation-operator">
  ## Opérateur de négation numérique
</div>

`-a` – La fonction `negate(a)`.

Pour la négation d’un tuple : [tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate).

<div id="multiplication-and-division-operators">
  ## Opérateurs de multiplication et de division
</div>

`a * b` – La fonction `multiply (a, b)`.

Pour multiplier un tuple par un nombre : [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber) ; pour le produit scalaire : [dotProduct](/fr/sql-reference/functions/array-functions#arrayDotProduct).

`a / b` – La fonction `divide(a, b)`.

Pour diviser un tuple par un nombre : [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber).

`a % b` – La fonction `modulo(a, b)`.

<div id="addition-and-subtraction-operators">
  ## Opérateurs d’addition et de soustraction
</div>

`a + b` – La fonction `plus(a, b)`.

Pour les tuples : [tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus).

`a - b` – La fonction `minus(a, b)`.

Pour les tuples : [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus).

<div id="comparison-operators">
  ## Opérateurs de comparaison
</div>

<div id="equals-function">
  ### fonction equals
</div>

`a = b` – La fonction `equals(a, b)`.

`a == b` – La fonction `equals(a, b)`.

<div id="notequals-function">
  ### Fonction notEquals
</div>

`a != b` – Fonction `notEquals(a, b)`.

`a <> b` – Fonction `notEquals(a, b)`.

<div id="lessorequals-function">
  ### fonction lessOrEquals
</div>

`a <= b` – La fonction `lessOrEquals(a, b)`.

<div id="greaterorequals-function">
  ### fonction greaterOrEquals
</div>

`a >= b` – La fonction `greaterOrEquals(a, b)`.

<div id="less-function">
  ### Fonction less
</div>

`a < b` – La fonction `less(a, b)`.

<div id="greater-function">
  ### fonction greater
</div>

`a > b` – La fonction `greater(a, b)`.

<div id="like-function">
  ### fonction like
</div>

`a LIKE b` – La fonction `like(a, b)`.

<div id="notlike-function">
  ### Fonction notLike
</div>

`a NOT LIKE b` – La fonction `notLike(a, b)`.

<div id="ilike-function">
  ### Fonction ilike
</div>

`a ILIKE b` – La fonction `ilike(a, b)`.

<div id="between-function">
  ### Fonction BETWEEN
</div>

`a BETWEEN b AND c` – Identique à `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – Identique à `a < b OR a > c`.

<div id="is-not-distinct-from">
  ### opérateur « is not distinct from » (`<=>`)
</div>

:::note
À partir de la version 25.10, vous pouvez utiliser `<=>` comme n’importe quel autre opérateur.
Avant la version 25.10, il ne pouvait être utilisé que dans des expressions JOIN, par exemple :

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

L’opérateur `<=>` est l’opérateur d’égalité sûr vis-à-vis de `NULL`, équivalent à `IS NOT DISTINCT FROM`.
Il fonctionne comme l’opérateur d’égalité classique (`=`), mais il considère les valeurs `NULL` comme comparables.
Deux valeurs `NULL` sont considérées comme égales, et la comparaison entre une valeur `NULL` et toute valeur non `NULL` renvoie 0 (`false`) au lieu de `NULL`.

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

<div id="operators-for-working-with-strings">
  ## Opérateurs de manipulation des chaînes de caractères
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` - La fonction `overlay(string, replacement, offset)`.
* `OVERLAY(string PLACING replacement FROM offset FOR length)` - La fonction `overlay(string, replacement, offset, length)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset)` - La fonction `overlayUTF8(string, replacement, offset)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - La fonction `overlayUTF8(string, replacement, offset, length)`.

<div id="operators-for-working-with-data-sets">
  ## Opérateurs pour manipuler des jeux de données
</div>

Voir les [opérateurs IN](../../sql-reference/operators/in.md) et l’opérateur [EXISTS](../../sql-reference/operators/exists.md).

<div id="in-function">
  ### Fonction in
</div>

`a IN ...` – La fonction `in(a, b)`.

<div id="notin-function">
  ### fonction notIn
</div>

`a NOT IN ...` – La fonction `notIn(a, b)`.

<div id="globalin-function">
  ### fonction globalIn
</div>

`a GLOBAL IN ...` – Fonction `globalIn(a, b)`.

<div id="globalnotin-function">
  ### fonction globalNotIn
</div>

`a GLOBAL NOT IN ...` – fonction `globalNotIn(a, b)`.

<div id="in-subquery-function">
  ### fonction in avec sous-requête
</div>

`a = ANY (subquery)` – La fonction `in(a, subquery)`.

<div id="notin-subquery-function">
  ### notIn sous-requête function
</div>

`a != ANY (subquery)` – Équivalent à `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="in-subquery-function-1">
  ### in sous-requête function
</div>

`a = ALL (subquery)` – Équivalent à `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="notin-subquery-function">
  ### notIn sous-requête function
</div>

`a != ALL (subquery)` – Fonction `notIn(a, subquery)`.

**Exemples**

Requête utilisant ALL :

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

Requête avec ANY :

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

<div id="some-all-on-arrays">
  ### `SOME` / `ALL` sur les tableaux
</div>

En plus de la forme avec sous-requête décrite ci-dessus, le côté droit de `SOME` / `ALL` peut être une expression de tableau (un littéral de tableau, une colonne de type tableau ou toute expression renvoyant un tableau). Il s&#39;agit de la syntaxe de quantificateur de tableau de style PostgreSQL. Elle est reconnue lors de l&#39;analyse syntaxique et réécrite en fonctions de tableau, sans nécessiter de réécriture manuelle :

| Syntaxe                                                   | Réécrit en                         |
| --------------------------------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                                        | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                                        | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (tout autre opérateur pris en charge) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (tout autre opérateur pris en charge)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` est le quantificateur existentiel (le synonyme SQL de `ANY`). `=` et `<>` sont traités à part et réécrits en `has` / `NOT has`, car ils disposent d&#39;une implémentation optimisée ; la forme générale repose sur les fonctions d&#39;ordre supérieur `arrayExists` / `arrayAll`.

La forme tableau est reconnue pour les opérateurs de comparaison `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`, les prédicats de comparaison par mot-clé `IS DISTINCT FROM` et `IS NOT DISTINCT FROM`, ainsi que les prédicats de recherche de chaînes `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE` et `REGEXP`. Les prédicats de comparaison par mot-clé et les prédicats de recherche de chaînes sont reconnus uniquement pour la forme tableau, et non pour la forme avec sous-requête (qui est ramenée à `IN`/`NOT IN`). Les opérateurs qui n&#39;ont pas de sens de quantificateur de tableau — par exemple `IN` lui-même — ne sont **pas** réécrits et conservent leur sens habituel.

Les prédicats de recherche de chaînes fonctionnent parce que `MatchImpl` (l&#39;implémentation utilisée par `LIKE` / `ILIKE` / `REGEXP`) prend en charge une chaîne source constante avec un motif non constant. Par exemple, `'abc' LIKE SOME(['a%', 'b%'])` est réécrit en `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])`, et `'abc' NOT LIKE ALL(['x%', 'y%'])` en `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])`. Cela permet de comparer une chaîne à plusieurs motifs ; pour effectuer la correspondance en un seul passage combiné, vous pouvez toujours utiliser une fonction de recherche multi-motifs comme `multiMatchAny` (expressions régulières) ou `multiSearchAny` (sous-chaînes).

:::note `ANY` n&#39;est pas pris en charge pour la forme tableau
Seuls `SOME` et `ALL` acceptent un tableau à droite. `ANY` est exclu, car `any` est aussi une fonction d&#39;agrégation ; une expression de la forme `expr = any(x)` conserve donc son sens d&#39;appel de fonction. Utilisez `SOME` pour le quantificateur de tableau.
:::

```sql title="Query"
SELECT
    3 = SOME([1, 2, 3, 4])         AS in_array,
    5 < SOME([1, 2, 6])            AS less_than_some,
    5 > ALL([1, 2, 3])             AS greater_than_all,
    'abc' LIKE SOME(['a%', 'z%'])  AS like_some;
```

```text title="Response"
┌─in_array─┬─less_than_some─┬─greater_than_all─┬─like_some─┐
│        1 │              1 │                1 │         1 │
└──────────┴────────────────┴──────────────────┴───────────┘
```

:::note la gestion de `NULL` diffère de celle de la forme avec sous-requête
Comme la forme tableau est réécrite dans le parseur (où les paramètres de requête tels que `transform_null_in` ne sont pas disponibles et où une array column par ligne ne peut pas emprunter le chemin `IN` null-safe de l’analyzer), elle utilise la sémantique à deux valeurs de `has` (pour `=` / `<>`) et de `arrayExists` / `arrayAll` (qui ramènent à `0` un résultat inconnu issu d’une comparaison avec `NULL`). Cela peut différer de la forme avec sous-requête, dont la gestion de `NULL` est ramenée à `IN` / `NOT IN` et dépend de `transform_null_in` :

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## Opérateurs pour manipuler les dates et les heures
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

Extrait des composantes d’une date donnée. Par exemple, vous pouvez extraire le mois d’une date donnée ou la seconde d’une valeur temporelle.

Le paramètre `part` indique quelle composante de la date extraire. Les valeurs suivantes sont disponibles :

* `NANOSECOND` — La nanoseconde. Valeurs possibles : 0–999999999.
* `MICROSECOND` — La microseconde. Valeurs possibles : 0–999999.
* `MILLISECOND` — La milliseconde. Valeurs possibles : 0–999.
* `SECOND` — La seconde. Valeurs possibles : 0–59.
* `MINUTE` — La minute. Valeurs possibles : 0–59.
* `HOUR` — L’heure. Valeurs possibles : 0–23.
* `DAY` — Le jour du mois. Valeurs possibles : 1–31.
* `WEEK` — Le numéro de semaine ISO 8601. Valeurs possibles : 1–53.
* `MONTH` — Le numéro du mois. Valeurs possibles : 1–12.
* `QUARTER` — Le trimestre. Valeurs possibles : 1–4.
* `YEAR` — L’année.
* `EPOCH` — Le timestamp Unix (secondes depuis 1970-01-01 00:00:00 UTC). Remarque : pour `DateTime64`, la fraction de seconde est tronquée.
* `DOW` — Le jour de la semaine (compatible PostgreSQL). 0 = dimanche, 6 = samedi.
* `DOY` — Le jour de l’année. Valeurs possibles : 1–366.
* `ISODOW` — Le jour ISO de la semaine. 1 = lundi, 7 = dimanche.
* `ISOYEAR` — L’année de numérotation des semaines ISO 8601.
* `CENTURY` — Le siècle. Par exemple, l’année 2024 se trouve au 21e siècle.
* `DECADE` — La décennie (année divisée par 10). Par exemple, l’année 2024 a pour décennie 202.
* `MILLENNIUM` — Le millénaire. Par exemple, l’année 2024 se trouve dans le 3e millénaire.
* `TIMEZONE_HOUR` — La partie heure signée du décalage UTC du fuseau horaire de l’opérande. Par exemple, `+5:30` renvoie `5`, `-3:30` renvoie `-3`.
* `TIMEZONE_MINUTE` — La partie minute signée du décalage UTC du fuseau horaire de l’opérande. Par exemple, `+5:30` renvoie `30`, `-3:30` renvoie `-30`.

Le paramètre `part` est insensible à la casse.

Le paramètre `date` indique la valeur à traiter. Les types [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md) et [Interval](../../sql-reference/data-types/special-data-types/interval.md) sont pris en charge. Lorsque `date` est un `Interval`, la valeur `part` demandée doit correspondre à l’unité stockée par l’intervalle (par exemple, `EXTRACT(DAY FROM INTERVAL 5 DAY)` est autorisé ; `EXTRACT(HOUR FROM INTERVAL 5 DAY)` est rejeté, car les intervalles ClickHouse ne stockent qu’une seule unité). Le résultat pour un opérande `Interval` est `Int64`.

Exemples :

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 5
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 30
SELECT EXTRACT(DAY   FROM INTERVAL 40 DAY);                                                -- 40
SELECT EXTRACT(MONTH FROM INTERVAL 7 MONTH);                                               -- 7
```

Dans l’exemple suivant, nous créons une table et y insérons une valeur de type `DateTime`.

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Vous pouvez consulter d’autres exemples dans les [tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

<div id="interval">
  ### INTERVAL
</div>

Crée une valeur de type [Interval](../../sql-reference/data-types/special-data-types/interval.md) à utiliser dans des opérations arithmétiques avec des valeurs de type [Date](../../sql-reference/data-types/date.md) et [DateTime](../../sql-reference/data-types/datetime.md).

Types d’intervalles :

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

Vous pouvez également utiliser un littéral de chaîne lors de la définition de la valeur `INTERVAL`. Par exemple, `INTERVAL 1 HOUR` est identique à `INTERVAL '1 hour'` ou `INTERVAL '1' hour`.

:::tip
Les intervalles de types différents ne peuvent pas être combinés. Vous ne pouvez pas utiliser des expressions comme `INTERVAL 4 DAY 1 HOUR`. Exprimez les intervalles dans des unités inférieures ou égales à la plus petite unité, par exemple `INTERVAL 25 HOUR`. Vous pouvez utiliser des opérations successives, comme dans l’exemple ci-dessous.
:::

Exemples :

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
La syntaxe `INTERVAL` ou la fonction `addDays` sont toujours à privilégier. Une simple addition ou soustraction (avec une syntaxe du type `now() + ...`) ne tient pas compte des paramètres de temps. Par exemple, l’heure d’été.
:::

Exemples :

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**Voir aussi**

* type de données [Interval](../../sql-reference/data-types/special-data-types/interval.md)
* fonctions de conversion de type [toInterval](/fr/sql-reference/functions/type-conversion-functions#toIntervalYear)

<div id="date-time-addition">
  ### Addition de date et d’heure
</div>

Une valeur [Date](../../sql-reference/data-types/date.md) ou [Date32](../../sql-reference/data-types/date32.md) peut être ajoutée à une valeur [Time](../../sql-reference/data-types/time.md) ou [Time64](../../sql-reference/data-types/time64.md) à l’aide de l’opérateur `+`. Le résultat est un [DateTime](../../sql-reference/data-types/datetime.md) ou [DateTime64](../../sql-reference/data-types/datetime64.md) correspondant à la date pour l’heure de la journée indiquée. L’opération est commutative.

Le type du résultat dépend du type des opérandes :

| Opérande gauche | Opérande droit | Type du résultat |
| --------------- | -------------- | ---------------- |
| `Date`          | `Time`         | `DateTime`       |
| `Date`          | `Time64(s)`    | `DateTime64(s)`  |
| `Date32`        | `Time`         | `DateTime64(0)`  |
| `Date32`        | `Time64(s)`    | `DateTime64(s)`  |

:::note
Le résultat utilise le [fuseau horaire de session](../../operations/settings/settings.md#session_timezone) (ou le fuseau horaire par défaut du serveur si aucun fuseau horaire de session n’est défini). Le paramètre [`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) contrôle ce qui se passe lorsque le résultat sort de la plage représentable.
:::

Exemples :

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

<div id="at-time-zone">
  ### AT TIME ZONE et AT LOCAL
</div>

Les opérateurs postfixés `AT TIME ZONE` et `AT LOCAL` convertissent une valeur `DateTime` ou `DateTime64` dans un autre fuseau horaire. Ils ne sont qu’un sucre syntaxique pour la fonction existante [`toTimeZone`](/fr/sql-reference/functions/date-time-functions#totimezone) :

| Syntaxe                  | Équivalent                     |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

`zone` peut être n’importe quelle expression constante de type chaîne qui s’évalue en un nom de fuseau horaire valide (par exemple `'America/Denver'`, `'UTC'` ou `concat('America', '/', 'Denver')`). Comme `AT TIME ZONE` est réécrit en `toTimeZone`, les mêmes règles s’appliquent aux arguments de fuseau horaire : les expressions non constantes, comme une référence de colonne, nécessitent [`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments).

`AT LOCAL` utilise le [fuseau horaire de session](../../operations/settings/settings.md#session_timezone) actuel (ou le fuseau par défaut du serveur si aucun fuseau horaire de session n’est défini). Sur les tables `Distributed`, `session_timezone` doit être défini explicitement ; lorsqu’il est vide, `timeZone()` est propre au shard et ne peut pas être utilisé comme argument constant de `toTimeZone`, ce qui provoque une exception `ILLEGAL_COLUMN`.

:::note
Contrairement à PostgreSQL, où `timestamp without time zone AT TIME ZONE zone` réinterprète la valeur affichée comme appartenant au fuseau donné avant conversion, ClickHouse conserve toujours le même instant absolu et ne change que l’étiquette de fuseau horaire utilisée pour l’affichage. Les deux formes sont équivalentes à `toTimeZone` et ne modifient pas le timestamp sous-jacent.
:::

`AT TIME ZONE` a une préséance de 13 (au-dessus de `*`/`/`/`%` à 12, et de `+`/`-` à 11), comme dans PostgreSQL. Cela signifie que `a * ts AT TIME ZONE 'tz'` s’interprète comme `a * (ts AT TIME ZONE 'tz')`, et que `ts + interval AT TIME ZONE 'tz'` s’interprète comme `ts + (interval AT TIME ZONE 'tz')`. Pour appliquer la conversion de fuseau horaire après l’opération arithmétique, utilisez des parenthèses explicites :

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

Exemples :

```sql
SET session_timezone = 'UTC';

SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), 'America/Denver')─┐
│ 2001-02-16 13:38:40                                              │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), timeZone())─┐
│ 2001-02-16 20:38:40                                        │
└────────────────────────────────────────────────────────────┘
```

**Voir aussi**

* [`toTimeZone`](/fr/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/fr/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## Opérateur logique AND
</div>

Syntaxe `SELECT a AND b` — calcule la conjonction logique de `a` et `b` à l’aide de la fonction [and](/fr/sql-reference/functions/logical-functions#and).

<div id="logical-or-operator">
  ## Opérateur logique OR
</div>

Syntaxe `SELECT a OR b` — calcule la disjonction logique de `a` et `b` avec la fonction [or](/fr/sql-reference/functions/logical-functions#or).

<div id="logical-negation-operator">
  ## Opérateur de négation logique
</div>

Syntaxe `SELECT NOT a` — permet de calculer la négation logique de `a` à l’aide de la fonction [not](/fr/sql-reference/functions/logical-functions#not).

<div id="conditional-operator">
  ## Opérateur conditionnel
</div>

`a ? b : c` – La fonction `if(a, b, c)`.

Remarque :

L’opérateur conditionnel calcule les valeurs de b et de c, puis vérifie si la condition a est satisfaite, avant de renvoyer la valeur correspondante. Si `b` ou `C` est une fonction [arrayJoin()](/fr/sql-reference/functions/array-join), chaque ligne sera répliquée независимоamment de la condition « a ».

<div id="conditional-expression">
  ## Expression conditionnelle
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Si `x` est spécifié, la fonction `transform(x, [a, ...], [b, ...], c)` est utilisée. Sinon, `multiIf(a, b, ..., c)` est utilisée.

S’il n’y a pas de clause `ELSE c` dans l’expression, la valeur par défaut est `NULL`.

La fonction `transform` ne prend pas en charge `NULL`.

<div id="concatenation-operator">
  ## Opérateur de concaténation
</div>

`s1 || s2` – La fonction `concat(s1, s2)`.

<div id="lambda-creation-operator">
  ## Opérateur de création de lambda
</div>

`x -> expr` – La `fonction lambda(x, expr)`.

Les opérateurs suivants n&#39;ont pas de priorité, car il s&#39;agit de parenthèses :

<div id="array-creation-operator">
  ## Opérateur de création de tableau
</div>

`[x1, ...]` – La fonction `array(x1, ...)`.

<div id="tuple-creation-operator">
  ## Opérateur de création de Tuple
</div>

`(x1, x2, ...)` – la fonction `tuple(x2, x2, ...)`.

<div id="associativity">
  ## Associativité
</div>

Tous les opérateurs binaires ont une associativité à gauche. Par exemple, `1 + 2 + 3` est transformé en `plus(plus(1, 2), 3)`.
Il arrive parfois que cela ne fonctionne pas comme vous l’attendez. Par exemple, `SELECT 4 > 2 > 3` renverra 0.

Pour des raisons d’efficacité, les fonctions `and` et `or` acceptent un nombre quelconque d’arguments. Les suites correspondantes d’opérateurs `AND` et `OR` sont transformées en un seul appel à ces fonctions.

<div id="checking-for-null">
  ## Vérifier `NULL`
</div>

ClickHouse prend en charge les opérateurs `IS NULL` et `IS NOT NULL`.

<div id="is_null">
  ### IS NULL
</div>

* Pour les valeurs de type [Nullable](../../sql-reference/data-types/nullable.md), l’opérateur `IS NULL` renvoie :
  * `1` si la valeur est `NULL`.
  * `0` dans le cas contraire.
* Pour les autres valeurs, l’opérateur `IS NULL` renvoie toujours `0`.

Cette opération peut être optimisée en activant le paramètre [optimize&#95;functions&#95;to&#95;subcolumns](/fr/operations/settings/settings#optimize_functions_to_subcolumns). Avec `optimize_functions_to_subcolumns = 1`, la fonction lit uniquement la sous-colonne [null](../../sql-reference/data-types/nullable.md#finding-null) au lieu de lire et de traiter l’ensemble des données de la colonne. La requête `SELECT n IS NULL FROM table` est transformée en `SELECT n.null FROM TABLE`.

{/* */ }

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

<div id="is_not_null">
  ### IS NOT NULL
</div>

* Pour les valeurs de type [Nullable](../../sql-reference/data-types/nullable.md), l’opérateur `IS NOT NULL` renvoie :
  * `0` si la valeur est `NULL`.
  * `1` dans le cas contraire.
* Pour les autres valeurs, l’opérateur `IS NOT NULL` renvoie toujours `1`.

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

Peut être optimisé en activant le paramètre [optimize&#95;functions&#95;to&#95;subcolumns](/fr/operations/settings/settings#optimize_functions_to_subcolumns). Avec `optimize_functions_to_subcolumns = 1`, la fonction ne lit que la sous-colonne [null](../../sql-reference/data-types/nullable.md#finding-null) au lieu de lire et de traiter l’ensemble des données de la colonne. La requête `SELECT n IS NOT NULL FROM table` est transformée en `SELECT NOT n.null FROM TABLE`.

<div id="checking-boolean-values">
  ## Vérifier les valeurs booléennes
</div>

ClickHouse prend en charge les opérateurs `IS TRUE`, `IS FALSE`, `IS UNKNOWN`, `IS NOT TRUE`, `IS NOT FALSE` et `IS NOT UNKNOWN`.
Ils s’utilisent avec les expressions [Bool](../../sql-reference/data-types/boolean.md) et `Nullable(Bool)`.

* `expr IS TRUE` renvoie `1` uniquement si `expr` vaut `true`.
* `expr IS FALSE` renvoie `1` uniquement si `expr` vaut `false`.
* `expr IS UNKNOWN` renvoie `1` uniquement si `expr` vaut `NULL`.
* `expr IS NOT TRUE` renvoie `1` si `expr` vaut `false` ou `NULL`.
* `expr IS NOT FALSE` renvoie `1` si `expr` vaut `true` ou `NULL`.
* `expr IS NOT UNKNOWN` renvoie `1` si `expr` n’est pas `NULL`.

Pour les expressions booléennes, `IS UNKNOWN` est équivalent à `IS NULL`, et `IS NOT UNKNOWN` est équivalent à `IS NOT NULL`.

{/* */ }

```sql
CREATE TABLE t_bool (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (true), (false), (NULL);

SELECT
    x,
    x IS TRUE,
    x IS FALSE,
    x IS UNKNOWN,
    x IS NOT TRUE,
    x IS NOT FALSE,
    x IS NOT UNKNOWN
FROM t_bool;
```