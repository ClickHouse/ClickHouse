---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "Op\xE9rateur"
---

# Opérateur {#operators}

ClickHouse transforme les opérateurs en leurs fonctions correspondantes à l'étape d'analyse des requêtes en fonction de leur priorité, de leur priorité et de leur associativité.

## Des Opérateurs D'Accès {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` fonction.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` fonction.

## Opérateur De Négation Numérique {#numeric-negation-operator}

`-a` – The `negate (a)` fonction.

## Opérateurs de Multiplication et de Division {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` fonction.

`a / b` – The `divide(a, b)` fonction.

`a % b` – The `modulo(a, b)` fonction.

## Opérateurs d'Addition et de soustraction {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` fonction.

`a - b` – The `minus(a, b)` fonction.

## Opérateurs De Comparaison {#comparison-operators}

`a = b` – The `equals(a, b)` fonction.

`a == b` – The `equals(a, b)` fonction.

`a != b` – The `notEquals(a, b)` fonction.

`a <> b` – The `notEquals(a, b)` fonction.

`a <= b` – The `lessOrEquals(a, b)` fonction.

`a >= b` – The `greaterOrEquals(a, b)` fonction.

`a < b` – The `less(a, b)` fonction.

`a > b` – The `greater(a, b)` fonction.

`a LIKE s` – The `like(a, b)` fonction.

`a NOT LIKE s` – The `notLike(a, b)` fonction.

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## Opérateurs pour travailler avec des ensembles de données {#operators-for-working-with-data-sets}

*Voir [Dans les opérateurs](in.md).*

`a IN ...` – The `in(a, b)` fonction.

`a NOT IN ...` – The `notIn(a, b)` fonction.

`a GLOBAL IN ...` – The `globalIn(a, b)` fonction.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` fonction.

## Opérateurs pour travailler avec des Dates et des heures {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

Extraire des parties d'une date donnée. Par exemple, vous pouvez récupérer un mois à partir d'une date donnée, ou d'une seconde à partir d'un moment.

Le `part` paramètre spécifie la partie de la date à récupérer. Les valeurs suivantes sont disponibles:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

Le `part` le paramètre est insensible à la casse.

Le `date` paramètre spécifie la date ou l'heure à traiter. Soit [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md) le type est pris en charge.

Exemple:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

Dans l'exemple suivant, nous créons un tableau et de les insérer dans une valeur avec le `DateTime` type.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Vous pouvez voir plus d'exemples de [test](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

Crée un [Intervalle](../../sql-reference/data-types/special-data-types/interval.md)- valeur de type qui doit être utilisée dans les opérations arithmétiques avec [Date](../../sql-reference/data-types/date.md) et [DateTime](../../sql-reference/data-types/datetime.md)-type de valeurs.

Types d'intervalles:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

!!! warning "Avertissement"
    Les intervalles avec différents types ne peuvent pas être combinés. Vous ne pouvez pas utiliser des expressions comme `INTERVAL 4 DAY 1 HOUR`. Spécifiez des intervalles en unités inférieures ou égales à la plus petite unité de l'intervalle, par exemple, `INTERVAL 25 HOUR`. Vous pouvez utiliser les opérations consécutives, comme dans l'exemple ci-dessous.

Exemple:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**Voir Aussi**

-   [Intervalle](../../sql-reference/data-types/special-data-types/interval.md) type de données
-   [toInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval) type fonctions de conversion

## Opérateur De Négation Logique {#logical-negation-operator}

`NOT a` – The `not(a)` fonction.

## Logique ET de l'Opérateur {#logical-and-operator}

`a AND b` – The`and(a, b)` fonction.

## Logique ou opérateur {#logical-or-operator}

`a OR b` – The `or(a, b)` fonction.

## Opérateur Conditionnel {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` fonction.

Note:

L'opérateur conditionnel calcule les valeurs de b et c, puis vérifie si la condition a est remplie, puis renvoie la valeur correspondante. Si `b` ou `C` est un [arrayJoin()](../../sql-reference/functions/array-join.md#functions_arrayjoin) fonction, chaque ligne sera répliquée indépendamment de la “a” condition.

## Expression Conditionnelle {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Si `x` est spécifié, alors `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

Si il n'y a pas de `ELSE c` dans l'expression, la valeur par défaut est `NULL`.

Le `transform` la fonction ne fonctionne pas avec `NULL`.

## Opérateur De Concaténation {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## Opérateur De Création Lambda {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

Les opérateurs suivants n'ont pas de priorité puisqu'ils sont des parenthèses:

## Opérateur De Création De Tableau {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## Opérateur De Création De Tuple {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## Associativité {#associativity}

Tous les opérateurs binaires ont associativité gauche. Exemple, `1 + 2 + 3` est transformé à `plus(plus(1, 2), 3)`.
Parfois, cela ne fonctionne pas de la façon que vous attendez. Exemple, `SELECT 4 > 2 > 3` résultat sera 0.

Pour l'efficacité, le `and` et `or` les fonctions acceptent n'importe quel nombre d'arguments. Les chaînes de `AND` et `OR` les opérateurs se sont transformés en un seul appel de ces fonctions.

## La vérification de `NULL` {#checking-for-null}

Clickhouse soutient le `IS NULL` et `IS NOT NULL` opérateur.

### IS NULL {#operator-is-null}

-   Pour [Nullable](../../sql-reference/data-types/nullable.md) type de valeurs, l' `IS NULL` opérateur retourne:
    -   `1` si la valeur est `NULL`.
    -   `0` autrement.
-   Pour les autres valeurs, la `IS NULL` l'opérateur renvoie toujours `0`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   Pour [Nullable](../../sql-reference/data-types/nullable.md) type de valeurs, l' `IS NOT NULL` opérateur retourne:
    -   `0` si la valeur est `NULL`.
    -   `1` autrement.
-   Pour les autres valeurs, la `IS NOT NULL` l'opérateur renvoie toujours `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[Article Original](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
