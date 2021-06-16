---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: DateTime
---

# Datetime {#data_type-datetime}

Permet de stocker un instant dans le temps, qui peut être exprimé comme une date de calendrier et une heure d'une journée.

Syntaxe:

``` sql
DateTime([timezone])
```

Plage de valeurs prise en charge: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

Résolution: 1 seconde.

## Utilisation Remarques {#usage-remarks}

Le point dans le temps est enregistré en tant que [Le timestamp Unix](https://en.wikipedia.org/wiki/Unix_time), quel que soit le fuseau horaire ou l'heure d'été. En outre, l' `DateTime` type peut stocker le fuseau horaire qui est le même pour la colonne entière, qui affecte la façon dont les valeurs de la `DateTime` les valeurs de type sont affichées au format texte et comment les valeurs spécifiées en tant que chaînes sont analysées (‘2020-01-01 05:00:01’). Le fuseau horaire n'est pas stocké dans les lignes de la table (ou dans resultset), mais est stocké dans les métadonnées de la colonne.
Une liste des fuseaux horaires pris en charge peut être trouvée dans le [Base de données de fuseau horaire IANA](https://www.iana.org/time-zones).
Le `tzdata` paquet, contenant [Base de données de fuseau horaire IANA](https://www.iana.org/time-zones), doit être installé dans le système. L'utilisation de la `timedatectl list-timezones` commande pour lister les fuseaux horaires connus par un système local.

Vous pouvez définir explicitement un fuseau horaire `DateTime`- tapez des colonnes lors de la création d'une table. Si le fuseau horaire n'est pas défini, ClickHouse utilise la valeur [fuseau](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) paramètre dans les paramètres du serveur ou les paramètres du système d'exploitation au moment du démarrage du serveur ClickHouse.

Le [clickhouse-client](../../interfaces/cli.md) applique le fuseau horaire du serveur par défaut si un fuseau horaire n'est pas explicitement défini lors de l'initialisation du type de données. Pour utiliser le fuseau horaire du client, exécutez `clickhouse-client` avec l' `--use_client_time_zone` paramètre.

Clickhouse affiche les valeurs dans `YYYY-MM-DD hh:mm:ss` format de texte par défaut. Vous pouvez modifier la sortie avec le [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) fonction.

Lorsque vous insérez des données dans ClickHouse, vous pouvez utiliser différents formats de chaînes de date et d'heure, en fonction de la valeur du [date\_time\_input\_format](../../operations/settings/settings.md#settings-date_time_input_format) paramètre.

## Exemple {#examples}

**1.** Création d'une table avec un `DateTime`- tapez la colonne et insérez des données dedans:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   Lors de l'insertion de datetime en tant qu'entier, il est traité comme un horodatage Unix (UTC). `1546300800` représenter `'2019-01-01 00:00:00'` L'UTC. Cependant, comme `timestamp` la colonne a `Europe/Moscow` (UTC+3) fuseau horaire spécifié, lors de la sortie en tant que chaîne, la valeur sera affichée comme `'2019-01-01 03:00:00'`
-   Lors de l'insertion d'une valeur de chaîne en tant que datetime, elle est traitée comme étant dans le fuseau horaire de la colonne. `'2019-01-01 00:00:00'` sera considérée comme étant en `Europe/Moscow` fuseau horaire et enregistré sous `1546290000`.

**2.** Le filtrage sur `DateTime` valeur

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` les valeurs de colonne peuvent être filtrées à l'aide d'une `WHERE` prédicat. Elle sera convertie `DateTime` automatiquement:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Obtenir un fuseau horaire pour un `DateTime`colonne de type:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** Conversion de fuseau horaire

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## Voir Aussi {#see-also}

-   [Fonctions de conversion de Type](../../sql-reference/functions/type-conversion-functions.md)
-   [Fonctions pour travailler avec des dates et des heures](../../sql-reference/functions/date-time-functions.md)
-   [Fonctions pour travailler avec des tableaux](../../sql-reference/functions/array-functions.md)
-   [Le `date_time_input_format` paramètre](../../operations/settings/settings.md#settings-date_time_input_format)
-   [Le `timezone` paramètre de configuration du serveur](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Opérateurs pour travailler avec des dates et des heures](../../sql-reference/operators/index.md#operators-datetime)
-   [Le `Date` type de données](date.md)

[Article Original](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
