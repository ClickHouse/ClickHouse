---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: Travailler avec les Dates et les heures
---

# Fonctions pour travailler avec des Dates et des heures {#functions-for-working-with-dates-and-times}

Support des fuseaux horaires

Toutes les fonctions pour travailler avec la date et l'heure qui ont une logique d'utilisation pour le fuseau horaire peut accepter un second fuseau horaire argument. Exemple: Asie / Ekaterinbourg. Dans ce cas, ils utilisent le fuseau horaire spécifié au lieu du fuseau horaire local (par défaut).

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

``` text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

Seuls les fuseaux horaires qui diffèrent de L'UTC par un nombre entier d'heures sont pris en charge.

## toTimeZone {#totimezone}

Convertir l'heure ou la date et de l'heure au fuseau horaire spécifié.

## toYear {#toyear}

Convertit une date ou une date avec l'heure en un numéro UInt16 contenant le numéro d'année (AD).

## toQuarter {#toquarter}

Convertit une date ou une date avec l'heure en un numéro UInt8 contenant le numéro de trimestre.

## toMonth {#tomonth}

Convertit une date ou une date avec l'heure en un numéro UInt8 contenant le numéro de mois (1-12).

## toDayOfYear {#todayofyear}

Convertit une date ou une date avec l'heure en un numéro UInt16 contenant le numéro du jour de l'année (1-366).

## toDayOfMonth {#todayofmonth}

Convertit une date ou une date avec le temps à un UInt8 contenant le numéro du jour du mois (1-31).

## toDayOfWeek {#todayofweek}

Convertit une date ou une date avec l'heure en un numéro UInt8 contenant le numéro du jour de la semaine (lundi est 1, et dimanche est 7).

## toHour {#tohour}

Convertit une date avec l'heure en un nombre UInt8 contenant le numéro de l'heure dans l'Heure de 24 heures (0-23).
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## toMinute {#tominute}

Convertit une date avec l'heure en un numéro UInt8 contenant le numéro de la minute de l'heure (0-59).

## toseconde {#tosecond}

Convertit une date avec l'heure en un nombre UInt8 contenant le numéro de la seconde dans la minute (0-59).
Les secondes intercalaires ne sont pas comptabilisés.

## toUnixTimestamp {#to-unix-timestamp}

Pour L'argument DateTime: convertit la valeur en sa représentation numérique interne (horodatage Unix).
For String argument: analyse datetime from string en fonction du fuseau horaire (second argument optionnel, le fuseau horaire du serveur est utilisé par défaut) et renvoie l'horodatage unix correspondant.
Pour L'argument Date: le comportement n'est pas spécifié.

**Syntaxe**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Valeur renvoyée**

-   Renvoie l'horodatage unix.

Type: `UInt32`.

**Exemple**

Requête:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Résultat:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## toStartOfYear {#tostartofyear}

Arrondit une date ou une date avec l'heure jusqu'au premier jour de l'année.
Renvoie la date.

## toStartOfISOYear {#tostartofisoyear}

Arrondit une date ou une date avec l'heure jusqu'au premier jour de L'année ISO.
Renvoie la date.

## toStartOfQuarter {#tostartofquarter}

Arrondit une date ou une date avec l'heure jusqu'au premier jour du trimestre.
Le premier jour du trimestre, soit le 1er janvier, 1er avril, 1er juillet ou 1er octobre.
Renvoie la date.

## toStartOfMonth {#tostartofmonth}

Arrondit une date ou une date avec l'heure jusqu'au premier jour du mois.
Renvoie la date.

!!! attention "Attention"
    Le comportement de l'analyse des dates incorrectes est spécifique à l'implémentation. ClickHouse peut renvoyer la date zéro, lancer une exception ou faire “natural” débordement.

## toMonday {#tomonday}

Arrondit une date ou une date avec l'heure au lundi le plus proche.
Renvoie la date.

## toStartOfWeek (t \[, mode\]) {#tostartofweektmode}

Arrondit une date ou une date avec l'heure au dimanche ou au lundi le plus proche par mode.
Renvoie la date.
L'argument mode fonctionne exactement comme l'argument mode de toWeek(). Pour la syntaxe à argument unique, une valeur de mode de 0 est utilisée.

## toStartOfDay {#tostartofday}

Arrondit une date avec le temps au début de la journée.

## toStartOfHour {#tostartofhour}

Arrondit une date avec le temps au début de l " heure.

## toStartOfMinute {#tostartofminute}

Arrondit une date avec le temps au début de la minute.

## toStartOfFiveMinute {#tostartoffiveminute}

Arrondit à une date avec l'heure de début de l'intervalle de cinq minutes.

## toStartOfTenMinutes {#tostartoftenminutes}

Arrondit une date avec le temps au début de l " intervalle de dix minutes.

## toStartOfFifteenMinutes {#tostartoffifteenminutes}

Arrondit la date avec le temps jusqu'au début de l'intervalle de quinze minutes.

## toStartOfInterval(time\_or\_data, intervalle x Unité \[, time\_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

Ceci est une généralisation d'autres fonctions nommées `toStartOf*`. Exemple,
`toStartOfInterval(t, INTERVAL 1 year)` renvoie la même chose que `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` renvoie la même chose que `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` renvoie la même chose que `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` renvoie la même chose que `toStartOfFifteenMinutes(t)` etc.

## toTime {#totime}

Convertit une date avec l'heure en une certaine date fixe, tout en préservant l'heure.

## toRelativeYearNum {#torelativeyearnum}

Convertit une date avec l'heure ou la date, le numéro de l'année, à partir d'un certain point fixe dans le passé.

## toRelativeQuarterNum {#torelativequarternum}

Convertit une date avec l'heure ou la date au numéro du trimestre, à partir d'un certain point fixe dans le passé.

## toRelativeMonthNum {#torelativemonthnum}

Convertit une date avec l'heure ou la date au numéro du mois, à partir d'un certain point fixe dans le passé.

## toRelativeWeekNum {#torelativeweeknum}

Convertit une date avec l'heure ou la date, le numéro de la semaine, à partir d'un certain point fixe dans le passé.

## toRelativeDayNum {#torelativedaynum}

Convertit une date avec l'heure ou la date au numéro du jour, à partir d'un certain point fixe dans le passé.

## toRelativeHourNum {#torelativehournum}

Convertit une date avec l'heure ou la date au nombre de l'heure, à partir d'un certain point fixe dans le passé.

## toRelativeMinuteNum {#torelativeminutenum}

Convertit une date avec l'heure ou la date au numéro de la minute, à partir d'un certain point fixe dans le passé.

## toRelativeSecondNum {#torelativesecondnum}

Convertit une date avec l'heure ou la date au numéro de la seconde, à partir d'un certain point fixe dans le passé.

## toISOYear {#toisoyear}

Convertit une date ou une date avec l'heure en un numéro UInt16 contenant le numéro D'année ISO.

## toISOWeek {#toisoweek}

Convertit une date ou une date avec l'heure en un numéro UInt8 contenant le numéro de semaine ISO.

## toWeek (date \[, mode\]) {#toweekdatemode}

Cette fonction renvoie le numéro de semaine pour date ou datetime. La forme à deux arguments de toWeek() vous permet de spécifier si la semaine commence le dimanche ou le lundi et si la valeur de retour doit être comprise entre 0 et 53 ou entre 1 et 53. Si l'argument mode est omis, le mode par défaut est 0.
`toISOWeek()`est une fonction de compatibilité équivalente à `toWeek(date,3)`.
Le tableau suivant décrit le fonctionnement de l'argument mode.

| Mode | Premier jour de la semaine | Gamme | Week 1 is the first week …       |
|------|----------------------------|-------|----------------------------------|
| 0    | Dimanche                   | 0-53  | avec un dimanche cette année     |
| 1    | Lundi                      | 0-53  | avec 4 jours ou plus cette année |
| 2    | Dimanche                   | 1-53  | avec un dimanche cette année     |
| 3    | Lundi                      | 1-53  | avec 4 jours ou plus cette année |
| 4    | Dimanche                   | 0-53  | avec 4 jours ou plus cette année |
| 5    | Lundi                      | 0-53  | avec un lundi cette année        |
| 6    | Dimanche                   | 1-53  | avec 4 jours ou plus cette année |
| 7    | Lundi                      | 1-53  | avec un lundi cette année        |
| 8    | Dimanche                   | 1-53  | contient Janvier 1               |
| 9    | Lundi                      | 1-53  | contient Janvier 1               |

Pour les valeurs de mode avec une signification de “with 4 or more days this year,” les semaines sont numérotées selon ISO 8601: 1988:

-   Si la semaine contenant Janvier 1 A 4 jours ou plus dans la nouvelle année, il est Semaine 1.

-   Sinon, c'est la dernière semaine de l'année précédente, et la semaine prochaine est la semaine 1.

Pour les valeurs de mode avec une signification de “contains January 1”, la semaine contient Janvier 1 est Semaine 1. Peu importe combien de jours dans la nouvelle année la semaine contenait, même si elle contenait seulement un jour.

``` sql
toWeek(date, [, mode][, Timezone])
```

**Paramètre**

-   `date` – Date or DateTime.
-   `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
-   `Timezone` – Optional parameter, it behaves like any other conversion function.

**Exemple**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek (date \[, mode\]) {#toyearweekdatemode}

Retourne l'année et la semaine pour une date. L'année dans le résultat peut être différente de l'année dans l'argument date pour la première et la dernière semaine de l'année.

L'argument mode fonctionne exactement comme l'argument mode de toWeek(). Pour la syntaxe à argument unique, une valeur de mode de 0 est utilisée.

`toISOYear()`est une fonction de compatibilité équivalente à `intDiv(toYearWeek(date,3),100)`.

**Exemple**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## maintenant {#now}

Accepte zéro argument et renvoie l'heure actuelle à l'un des moments de l'exécution de la requête.
Cette fonction renvoie une constante, même si la requête a pris beaucoup de temps à compléter.

## aujourd' {#today}

Accepte zéro argument et renvoie la date actuelle à l'un des moments de l'exécution de la requête.
Le même que ‘toDate(now())’.

## hier {#yesterday}

Accepte zéro argument et renvoie la date d'hier à l'un des moments de l'exécution de la requête.
Le même que ‘today() - 1’.

## l'horaire de diffusion {#timeslot}

Arrondit le temps à la demi-heure.
Cette fonction est spécifique à Yandex.Metrica, car une demi-heure est le temps minimum pour diviser une session en deux sessions si une balise de suivi affiche les pages vues consécutives d'un seul utilisateur qui diffèrent dans le temps de strictement plus que ce montant. Cela signifie que les tuples (l'ID de balise, l'ID utilisateur et l'intervalle de temps) peuvent être utilisés pour rechercher les pages vues incluses dans la session correspondante.

## toYYYYMM {#toyyyymm}

Convertit une date ou une date avec l'heure en un numéro UInt32 contenant le numéro d'année et de mois (AAAA \* 100 + MM).

## toYYYYMMDD {#toyyyymmdd}

Convertit une date ou une date avec l'heure en un numéro UInt32 contenant le numéro d'année et de mois (AAAA \* 10000 + MM \* 100 + JJ).

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

Convertit une date ou une date avec l'heure en un numéro UInt64 contenant le numéro d'année et de mois (AAAA \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss).

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

Fonction ajoute une date / DateTime intervalle à une Date / DateTime, puis retourner la Date / DateTime. Exemple:

``` sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

``` text
┌─add_years_with_date─┬─add_years_with_date_time─┐
│          2019-01-01 │      2019-01-01 00:00:00 │
└─────────────────────┴──────────────────────────┘
```

## subtractYears, subtractMonths, subtractWeeks, subtractDays, subtractHours, subtractMinutes, subtractSeconds, subtractQuarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

Fonction soustrayez un intervalle de Date / DateTime à une Date / DateTime, puis renvoyez la Date / DateTime. Exemple:

``` sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

``` text
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│               2018-01-01 │           2018-01-01 00:00:00 │
└──────────────────────────┴───────────────────────────────┘
```

## dateDiff {#datediff}

Renvoie la différence entre deux valeurs Date ou DateTime.

**Syntaxe**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Paramètre**

-   `unit` — Time unit, in which the returned value is expressed. [Chaîne](../syntax.md#syntax-string-literal).

        Supported values:

        | unit   |
        | ---- |
        |second  |
        |minute  |
        |hour    |
        |day     |
        |week    |
        |month   |
        |quarter |
        |year    |

-   `startdate` — The first time value to compare. [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — The second time value to compare. [Date](../../sql-reference/data-types/date.md) ou [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — Optional parameter. If specified, it is applied to both `startdate` et `enddate`. Si non spécifié, fuseaux horaires de l' `startdate` et `enddate` sont utilisés. Si elles ne sont pas identiques, le résultat n'est pas spécifié.

**Valeur renvoyée**

Différence entre `startdate` et `enddate` exprimé en `unit`.

Type: `int`.

**Exemple**

Requête:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Résultat:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## intervalle de temps (StartTime, Duration, \[, Size\]) {#timeslotsstarttime-duration-size}

Pour un intervalle de temps commençant à ‘StartTime’ et de poursuivre pour ‘Duration’ secondes, il renvoie un tableau de moments dans le temps, composé de points de cet intervalle arrondis vers le bas à la ‘Size’ en quelques secondes. ‘Size’ est un paramètre optionnel: une constante UInt32, définie sur 1800 par défaut.
Exemple, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
Ceci est nécessaire pour rechercher les pages vues dans la session correspondante.

## formatDateTime(Heure, Format \[, fuseau horaire\]) {#formatdatetime}

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

Modificateurs pris en charge pour le Format:
(“Example” colonne affiche le résultat de formatage pour le temps `2018-01-02 22:33:44`)

| Modificateur | Description                                                            | Exemple    |
|--------------|------------------------------------------------------------------------|------------|
| %C           | année divisée par 100 et tronquée en entier (00-99)                    | 20         |
| %d           | jour du mois, zero-rembourré (01-31)                                   | 02         |
| %D           | Date courte MM / JJ / AA, équivalente à %m / % d / % y                 | 01/02/18   |
| % e          | jour du mois, rembourré dans l'espace ( 1-31)                          | 2          |
| %F           | date courte AAAA-MM-JJ, équivalente à % Y - % m - % d                  | 2018-01-02 |
| %H           | heure en format 24h (00-23)                                            | 22         |
| %I           | heure en format 12h (01-12)                                            | 10         |
| %j           | les jours de l'année (001-366)                                         | 002        |
| %m           | mois en nombre décimal (01-12)                                         | 01         |
| %M           | minute (00-59)                                                         | 33         |
| %et          | caractère de nouvelle ligne (")                                        |            |
| %p           | Désignation AM ou PM                                                   | PM         |
| %R           | 24 heures HH:MM temps, équivalent à %H: % M                            | 22:33      |
| %S           | deuxième (00-59)                                                       | 44         |
| % t          | horizontal-caractère de tabulation (')                                 |            |
| %T           | Format d'heure ISO 8601 (HH:MM:SS), équivalent à %H: % M:%S            | 22:33:44   |
| % u          | ISO 8601 jour de la semaine comme numéro avec Lundi comme 1 (1-7)      | 2          |
| %V           | Numéro de semaine ISO 8601 (01-53)                                     | 01         |
| %W           | jour de la semaine comme un nombre décimal avec dimanche comme 0 (0-6) | 2          |
| % y          | Année, deux derniers chiffres (00-99)                                  | 18         |
| %Y           | An                                                                     | 2018       |
| %%           | signe                                                                  | %          |

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->
