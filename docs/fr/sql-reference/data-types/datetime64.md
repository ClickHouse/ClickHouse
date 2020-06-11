---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: DateTime64
---

# Datetime64 {#data_type-datetime64}

Permet de stocker un instant dans le temps, qui peut être exprimé comme une date de calendrier et une heure d'un jour, avec une précision de sous-seconde définie

Tick taille (précision): 10<sup>-précision</sup> deuxième

Syntaxe:

``` sql
DateTime64(precision, [timezone])
```

En interne, stocke les données comme un certain nombre de ‘ticks’ depuis le début de l'époque (1970-01-01 00: 00: 00 UTC) comme Int64. La résolution des tiques est déterminée par le paramètre de précision. En outre, l' `DateTime64` type peut stocker le fuseau horaire qui est le même pour la colonne entière, qui affecte la façon dont les valeurs de la `DateTime64` les valeurs de type sont affichées au format texte et comment les valeurs spécifiées en tant que chaînes sont analysées (‘2020-01-01 05:00:01.000’). Le fuseau horaire n'est pas stocké dans les lignes de la table (ou dans resultset), mais est stocké dans les métadonnées de la colonne. Voir les détails dans [DateTime](datetime.md).

## Exemple {#examples}

**1.** Création d'une table avec `DateTime64`- tapez la colonne et insérez des données dedans:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```

``` sql
SELECT * FROM dt
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   Lors de l'insertion de datetime en tant qu'entier, il est traité comme un horodatage Unix (UTC) mis à l'échelle de manière appropriée. `1546300800000` (avec précision 3) représente `'2019-01-01 00:00:00'` L'UTC. Cependant, comme `timestamp` la colonne a `Europe/Moscow` (UTC+3) fuseau horaire spécifié, lors de la sortie sous forme de chaîne, la valeur sera affichée comme `'2019-01-01 03:00:00'`
-   Lors de l'insertion d'une valeur de chaîne en tant que datetime, elle est traitée comme étant dans le fuseau horaire de la colonne. `'2019-01-01 00:00:00'` sera considérée comme étant en `Europe/Moscow` fuseau horaire et stocké comme `1546290000000`.

**2.** Le filtrage sur `DateTime64` valeur

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

Contrairement `DateTime`, `DateTime64` les valeurs ne sont pas converties depuis `String` automatiquement

**3.** Obtenir un fuseau horaire pour un `DateTime64`-le type de la valeur:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** Conversion de fuseau horaire

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## Voir Aussi {#see-also}

-   [Fonctions de conversion de Type](../../sql-reference/functions/type-conversion-functions.md)
-   [Fonctions pour travailler avec des dates et des heures](../../sql-reference/functions/date-time-functions.md)
-   [Fonctions pour travailler avec des tableaux](../../sql-reference/functions/array-functions.md)
-   [Le `date_time_input_format` paramètre](../../operations/settings/settings.md#settings-date_time_input_format)
-   [Le `timezone` paramètre de configuration du serveur](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Opérateurs pour travailler avec des dates et des heures](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` type de données](date.md)
-   [`DateTime` type de données](datetime.md)
