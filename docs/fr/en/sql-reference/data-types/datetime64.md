---
description: 'Documentation du type de données DateTime64 dans ClickHouse, qui stocke
  des horodatages avec une précision inférieure à la seconde'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'référence'
---

Permet de stocker un instant, pouvant être exprimé sous la forme d&#39;une date calendaire et d&#39;une heure de la journée, avec une précision inférieure à la seconde définie

Taille du tick (précision) : 10<sup>-precision</sup> secondes. Plage valide : [ 0 : 9 ].
Les valeurs généralement utilisées sont 3 (millisecondes), 6 (microsecondes) et 9 (nanosecondes).

Valeur par défaut : 3 (millisecondes).

**Syntaxe :**

```sql
DateTime64(precision, [timezone])
```

En interne, les données sont stockées sous la forme d’un certain nombre de &#39;ticks&#39; depuis le début de l’époque Unix (1970-01-01 00:00:00 UTC), en Int64. La résolution des ticks est déterminée par le paramètre `precision`. De plus, le type `DateTime64` peut stocker un fuseau horaire identique pour toute la colonne, ce qui affecte la manière dont les valeurs du type `DateTime64` sont affichées au format texte et la manière dont les valeurs spécifiées sous forme de chaînes sont interprétées (&#39;2020-01-01 05:00:01.000&#39;). Le fuseau horaire n’est pas stocké dans les lignes de la table (ni dans le jeu de résultats), mais dans les métadonnées de la colonne. Voir les détails dans [DateTime](../../sql-reference/data-types/datetime.md).

Plage de valeurs prise en charge : [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

Le nombre de chiffres après le séparateur décimal dépend du paramètre `precision`.

Remarque : la précision de la valeur maximale est de 8. Si la précision maximale de 9 chiffres (nanosecondes) est utilisée, la valeur maximale prise en charge est `2262-04-11 23:47:16` en UTC.

<div id="examples">
  ## Exemples
</div>

1. Création d’une table avec une colonne de type `DateTime64` et insertion de données dans cette table :

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* Lors de l’insertion d’une valeur datetime sous forme d’entier, elle est traitée comme un Unix timestamp (UTC) mis à l’échelle de manière appropriée. `1546300800000` (avec une précision de 3) représente `'2019-01-01 00:00:00'` UTC. Cependant, comme le fuseau horaire `Asia/Istanbul` (UTC+3) est spécifié pour la colonne `timestamp`, lors de l’affichage sous forme de chaîne, la valeur sera affichée comme `'2019-01-01 03:00:00'`. Lors de l’insertion d’une valeur datetime sous forme décimale, elle est traitée de façon similaire à un entier, sauf que la partie avant le point décimal correspond au Unix timestamp jusqu’aux secondes incluses, et la partie après le point décimal est traitée comme la précision.
* Lors de l’insertion d’une valeur de chaîne en tant que datetime, elle est traitée comme appartenant au fuseau horaire de la colonne. `'2019-01-01 00:00:00'` sera interprété comme appartenant au fuseau horaire `Asia/Istanbul` et stocké sous la forme `1546290000000`.

2. Filtrage des valeurs `DateTime64`

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

Contrairement à `DateTime`, les valeurs `DateTime64` ne sont pas converties automatiquement depuis `String`.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

Contrairement à l’insertion, la fonction `toDateTime64` traitera toutes les valeurs comme la variante décimale ; la précision doit donc
être indiquée après le point décimal.

3. Obtenir un fuseau horaire pour une valeur de type `DateTime64` :

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Conversion de fuseaux horaires

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**Voir aussi**

* [Fonctions de conversion de type](../../sql-reference/functions/type-conversion-functions.md)
* [Fonctions pour manipuler les dates et les heures](../../sql-reference/functions/date-time-functions.md)
* [Le paramètre `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [Le paramètre `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [Le paramètre de configuration du serveur `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [Le paramètre `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Opérateurs pour manipuler les dates et les heures](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Type de données `Date`](../../sql-reference/data-types/date.md)
* [Type de données `DateTime`](../../sql-reference/data-types/datetime.md)