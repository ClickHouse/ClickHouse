---
description: 'Documentation du type de données Time64 dans ClickHouse, qui stocke
  l’heure avec une précision à la sous-seconde'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

Le type de données `Time64` représente une heure avec des fractions de seconde.
Il ne comporte aucun composant de date calendaire (jour, mois, année).
Le paramètre `precision` définit le nombre de chiffres fractionnaires et, par conséquent, la taille du tick.

Taille du tick (précision) : 10<sup>-precision</sup> secondes. Plage valide : 0..9. Les valeurs les plus courantes sont 3 (millisecondes), 6 (microsecondes) et 9 (nanosecondes).

**Syntaxe :**

```sql
Time64(precision)
```

En interne, `Time64` stocke un nombre décimal signé sur 64 bits (Decimal64) représentant des fractions de seconde.
La résolution du tick est déterminée par le paramètre `precision`.
Les fuseaux horaires ne sont pas pris en charge : spécifier un fuseau horaire avec `Time64` provoquera une erreur.

Contrairement à `DateTime64`, `Time64` ne stocke pas de composant de date.
Voir aussi [`Time`](../../sql-reference/data-types/time.md).

Plage de représentation textuelle : [-999:59:59.000, 999:59:59.999] pour `precision = 3`. En général, le minimum est `-999:59:59` et le maximum est `999:59:59`, avec jusqu’à `precision` chiffres fractionnaires (pour `precision = 9`, le minimum est `-999:59:59.999999999`).

<div id="implementation-details">
  ## Détails d’implémentation
</div>

**Représentation**.
Valeur `Decimal64` signée représentant les fractions de seconde avec `precision` chiffres fractionnaires.

**Normalisation**.
Lors de l’analyse de chaînes en `Time64`, les composantes temporelles sont normalisées mais non validées.
Par exemple, `25:70:70` est interprété comme `26:11:10`.

**Valeurs négatives**.
Les signes moins en tête sont pris en charge et conservés.
Les valeurs négatives résultent généralement d’opérations arithmétiques sur des valeurs `Time64`.
Pour `Time64`, les entrées négatives sont conservées aussi bien pour les entrées textuelles (par exemple, `'-01:02:03.123'`) que pour les entrées numériques (par exemple, `-3723.123`).

**Saturation**.
La composante d’heure dans la journée est limitée à la plage [-999:59:59.xxx, 999:59:59.xxx] lors de la conversion en composantes ou de la sérialisation en texte.
La valeur numérique stockée peut dépasser cette plage ; toutefois, toute extraction de composantes (heures, minutes, secondes) et toute représentation textuelle utilisent la valeur saturée.

**Fuseaux horaires**.
`Time64` ne prend pas en charge les fuseaux horaires.
Spécifier un fuseau horaire lors de la création d’un type ou d’une valeur `Time64` génère une erreur.
De même, les tentatives d’appliquer ou de modifier un fuseau horaire sur des colonnes `Time64` ne sont pas prises en charge et entraînent une erreur.

<div id="examples">
  ## Exemples
</div>

1. Création d&#39;une table avec une colonne de type `Time64` et insertion de données dans cette table :

```sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

```sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. Filtrage des valeurs `Time64`

```sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

```sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

Remarque : `toTime64` interprète les littéraux numériques comme des secondes avec une partie fractionnaire selon la précision spécifiée ; indiquez donc explicitement le nombre voulu de chiffres après la virgule.

3. Inspection du type résultant :

```sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

```text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

<div id="addition-with-date">
  ## Addition avec Date
</div>

Une valeur [Time64](time64.md) peut être ajoutée à une valeur [Date](date.md) ou [Date32](date32.md) pour donner une [DateTime64](datetime64.md) avec la même précision décimale que `Time64` :

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

Voir [Addition de date et d’heure](../operators/index.md#date-time-addition) pour plus de détails sur toutes les combinaisons prises en charge et les types de résultats.

**Voir aussi**

* [Fonctions de conversion de types](../../sql-reference/functions/type-conversion-functions.md)
* [Fonctions pour travailler avec les dates et les heures](../../sql-reference/functions/date-time-functions.md)
* [Le paramètre `date_time_input_format`](../../operations/settings/settings-formats.md#date_time_input_format)
* [Le paramètre `date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)
* [Le paramètre de configuration du serveur `timezone`](../../operations/server-configuration-parameters/settings.md#timezone)
* [Le paramètre `session_timezone`](../../operations/settings/settings.md#session_timezone)
* [Opérateurs pour travailler avec les dates et les heures](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [Type de données `Date`](../../sql-reference/data-types/date.md)
* [Type de données `Time`](../../sql-reference/data-types/time.md)
* [Type de données `DateTime`](../../sql-reference/data-types/datetime.md)