---
description: "Documentation du type de données DateTime dans ClickHouse, qui stocke des horodatages avec une précision à la seconde"
sidebar_label: 'DateTime'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: 'DateTime'
doc_type: 'reference'
---

Permet de stocker un instant, exprimable sous la forme d’une date calendaire et d’une heure de la journée.

Syntaxe :

```sql
DateTime([timezone])
```

Intervalle de valeurs pris en charge : [1970-01-01 00:00:00, 2106-02-07 06:28:15].

Résolution : 1 seconde.

<div id="speed">
  ## Vitesse
</div>

Le type de données `Date` est plus rapide que `DateTime` dans *la plupart* des cas.

Le type `Date` nécessite 2 octets de stockage, tandis que `DateTime` en nécessite 4. Cependant, à la compression, l’écart de taille entre `Date` et `DateTime` devient plus marqué. Cela s’explique par le fait que les minutes et les secondes de `DateTime` se compressent moins bien. Le filtrage et l’agrégation sur `Date` plutôt que sur `DateTime` sont également plus rapides.

<div id="usage-remarks">
  ## Remarques d’utilisation
</div>

Le point dans le temps est enregistré sous forme de [timestamp Unix](https://en.wikipedia.org/wiki/Unix_time), quel que soit le fuseau horaire ou l’heure d’été. Le fuseau horaire influe sur la façon dont les valeurs du type `DateTime` sont affichées au format texte et dont les valeurs spécifiées sous forme de chaînes sont analysées (`'2020-01-01 05:00:01'`).

Un timestamp Unix indépendant du fuseau horaire est stocké dans les tables, et le fuseau horaire sert à le convertir vers le format texte ou inversement lors de l’import/export des données, ou à effectuer des calculs calendaires sur les valeurs (par exemple : fonctions `toDate`, `toHour`, etc.). Le fuseau horaire n’est pas stocké dans les lignes de la table (ni dans le jeu de résultats), mais dans les métadonnées de la colonne.

Une liste des fuseaux horaires pris en charge est disponible dans la [IANA Time Zone Database](https://www.iana.org/time-zones) et peut également être obtenue avec `SELECT * FROM system.time_zones`. [La liste](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) est aussi disponible sur Wikipedia.

Vous pouvez définir explicitement un fuseau horaire pour les colonnes de type `DateTime` lors de la création d’une table. Exemple : `DateTime('UTC')`. Si le fuseau horaire n’est pas défini, ClickHouse utilise la valeur du paramètre [timezone](../../operations/server-configuration-parameters/settings.md#timezone) dans les paramètres du serveur ou les paramètres du système d’exploitation au moment du démarrage du serveur ClickHouse.

Le [clickhouse-client](../../interfaces/client.md) applique par défaut le fuseau horaire du serveur si aucun fuseau horaire n’est explicitement défini lors de l’initialisation du type de données. Pour utiliser le fuseau horaire du client, exécutez `clickhouse-client` avec le paramètre `--use_client_time_zone`.

ClickHouse affiche les valeurs en fonction de la valeur du paramètre [date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format). Le format texte par défaut est `YYYY-MM-DD hh:mm:ss`. En outre, vous pouvez modifier la sortie avec la fonction [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime).

Lors de l’insertion de données dans ClickHouse, vous pouvez utiliser différents formats de chaînes de date et d’heure, selon la valeur du paramètre [date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format).

<div id="examples">
  ## Exemples
</div>

**1.** Création d’une table avec une colonne de type `DateTime` et insertion de données dans cette table :

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* Lors de l’insertion d’une valeur datetime sous forme d’entier, elle est interprétée comme un timestamp Unix (UTC). `1546300800` représente `'2019-01-01 00:00:00'` UTC. Cependant, comme le fuseau horaire `Asia/Istanbul` (UTC+3) est spécifié pour la colonne `timestamp`, lors de l’affichage sous forme de chaîne, la valeur sera affichée comme `'2019-01-01 03:00:00'`
* Lors de l’insertion d’une valeur chaîne comme datetime, elle est interprétée comme étant dans le fuseau horaire de la colonne. `'2019-01-01 00:00:00'` sera interprété comme étant dans le fuseau horaire `Asia/Istanbul` et enregistré sous la forme `1546290000`.

**2.** Filtrage des valeurs `DateTime`

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

Les valeurs de la colonne `DateTime` peuvent être filtrées à l’aide d’une chaîne de caractères dans le prédicat `WHERE`. Elle sera automatiquement convertie en `DateTime` :

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Récupération du fuseau horaire d’une colonne de type `DateTime` :

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** Conversion de fuseaux horaires

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

Comme la conversion de fuseau horaire ne modifie que les métadonnées, l’opération n’entraîne aucun coût de calcul.

<div id="limitations-on-time-zones-support">
  ## Limitations de la prise en charge des fuseaux horaires
</div>

Il se peut que certains fuseaux horaires ne soient pas entièrement pris en charge. Voici quelques cas :

Si le décalage par rapport à l’UTC n’est pas un multiple de 15 minutes, le calcul des heures et des minutes peut être incorrect. Par exemple, le fuseau horaire de Monrovia, au Liberia, avait un décalage UTC de -0:44:30 avant le 7 janv. 1972. Si vous effectuez des calculs sur des heures historiques dans le fuseau horaire de Monrovia, les fonctions de traitement du temps peuvent renvoyer des résultats incorrects. Les résultats après le 7 janv. 1972 seront néanmoins corrects.

Si le changement d’heure (en raison de l’heure d’été ou pour d’autres raisons) a eu lieu à un moment qui n’est pas un multiple de 15 minutes, vous pouvez également obtenir des résultats incorrects ce jour-là.

Dates calendaires non monotones. Par exemple, à Happy Valley - Goose Bay, l’heure a été reculée d’une heure à 00:01:00 le 7 nov. 2010 (une minute après minuit). Ainsi, après la fin du 6 nov., les habitants ont connu une minute entière du 7 nov., puis l’heure a été ramenée à 23:01 le 6 nov. et, 59 minutes plus tard, le 7 nov. a recommencé. ClickHouse ne prend pas (encore) en charge ce genre de bizarrerie. Pendant ces jours, les résultats des fonctions de traitement du temps peuvent être légèrement incorrects.

Un problème similaire existe pour la station antarctique Casey en 2010. L’heure y a été reculée de trois heures le 5 mars, à 02:00. Si vous travaillez dans une station antarctique, n’ayez crainte d’utiliser ClickHouse. Assurez-vous simplement de définir le fuseau horaire sur UTC ou gardez à l’esprit qu’il peut y avoir des imprécisions.

Décalages horaires sur plusieurs jours. Certaines îles du Pacifique ont modifié leur décalage horaire par rapport à l’UTC, passant de UTC+14 à UTC-12. Ce n’est pas un problème en soi, mais certaines imprécisions peuvent apparaître si vous effectuez des calculs avec leur fuseau horaire pour des dates historiques correspondant aux jours de transition.

<div id="handling-daylight-saving-time-dst">
  ## Gestion des changements d’heure (DST)
</div>

Le type DateTime de ClickHouse avec fuseaux horaires peut présenter un comportement inattendu lors des transitions liées à l’heure d’été (DST), notamment lorsque :

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) est défini sur `simple`.
* Les horloges sont retardées (« Fall Back »), ce qui entraîne un chevauchement d’une heure.
* Les horloges sont avancées (« Spring Forward »), ce qui crée un trou d’une heure.

Par défaut, ClickHouse choisit toujours la première occurrence d’une heure qui se chevauche et peut interpréter des heures inexistantes lors des passages à l’heure d’été.

Par exemple, considérons la transition suivante de l’heure d’été (DST) à l’heure normale.

* Le 29 octobre 2023, à 02:00:00, les horloges sont retardées à 01:00:00 (BST → GMT).
* L’heure 01:00:00 – 01:59:59 apparaît deux fois (une fois en BST et une fois en GMT)
* ClickHouse choisit toujours la première occurrence (BST), ce qui peut produire des résultats inattendus lors de l’ajout d’intervalles de temps.

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

De même, lors du passage de l’heure normale à l’heure d’été, une heure peut sembler avoir été sautée.

Par exemple :

* Le 26 mars 2023, à `00:59:59`, les horloges passent directement à 02:00:00 (GMT → BST).
* L’heure `01:00:00` – `01:59:59` n’existe pas.

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

Dans ce cas, ClickHouse ramène l&#39;heure inexistante `2023-03-26 01:30:00` à `2023-03-26 00:30:00`.

<div id="see-also">
  ## Voir aussi
</div>

* [Fonctions de conversion de type](../../sql-reference/functions/type-conversion-functions.md)
* [Fonctions de manipulation des dates et des heures](../../sql-reference/functions/date-time-functions.md)
* [Fonctions de manipulation des tableaux](../../sql-reference/functions/array-functions.md)
* [Le paramètre `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [Le paramètre `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [Le paramètre de configuration du serveur `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [Le paramètre `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Opérateurs pour les dates et les heures](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [Le type de données `Date`](../../sql-reference/data-types/date.md)