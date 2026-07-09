---
description: 'Documentation des fonctions de conversion de type'
sidebar_label: 'Conversion de type'
slug: /sql-reference/functions/type-conversion-functions
title: 'Fonctions de conversion de type'
doc_type: 'reference'
---

<div id="common-issues-with-data-conversion">
  ## Problèmes courants liés à la conversion de données
</div>

ClickHouse utilise généralement le [même comportement que les programmes C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

Les fonctions `to<type>` et [cast](#CAST) se comportent différemment dans certains cas, par exemple avec [LowCardinality](../data-types/lowcardinality.md) : [cast](#CAST) supprime la caractéristique [LowCardinality](../data-types/lowcardinality.md), alors que les fonctions `to<type>` ne le font pas. Il en va de même avec [Nullable](../data-types/nullable.md) ; ce comportement n&#39;est pas compatible avec le standard SQL, mais il peut être modifié à l&#39;aide du paramètre [cast&#95;keep&#95;nullable](../../operations/settings/settings.md/#cast_keep_nullable).

:::note
Soyez conscient du risque de perte de données si des valeurs d&#39;un type de données sont converties vers un type plus petit (par exemple de `Int64` vers `Int32`) ou entre
des types de données incompatibles (par exemple de `String` vers `Int`). Vérifiez attentivement que le résultat correspond bien à vos attentes.
:::

Exemple :

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

<div id="to-string-functions">
  ## Remarques sur les fonctions `toString`
</div>

La famille de fonctions `toString` permet de convertir des nombres, des chaînes (mais pas des chaînes de longueur fixe), des dates et des dates avec heure.
Toutes ces fonctions acceptent un argument.

* Lors d&#39;une conversion vers une chaîne ou depuis une chaîne, la valeur est formatée ou analysée selon les mêmes règles que pour le format TabSeparated (et presque tous les autres formats texte). Si la chaîne ne peut pas être analysée, une exception est levée et la requête est annulée.
* Lors de la conversion de dates en nombres, ou inversement, la date correspond au nombre de jours écoulés depuis le début de l&#39;époque Unix.
* Lors de la conversion de dates avec heure en nombres, ou inversement, la date avec heure correspond au nombre de secondes écoulées depuis le début de l&#39;époque Unix.
* La fonction `toString` appliquée à un argument `DateTime` peut prendre un deuxième argument String contenant le nom du fuseau horaire, par exemple : `Europe/Amsterdam`. Dans ce cas, l&#39;heure est formatée selon le fuseau horaire spécifié.

<div id="to-date-and-date-time-functions">
  ## Remarques sur les fonctions `toDate`/`toDateTime`
</div>

Les formats de date et de date-heure des fonctions `toDate`/`toDateTime` sont définis comme suit :

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

À titre d&#39;exception, lors de la conversion de types numériques UInt32, Int32, UInt64 ou Int64 vers Date, si le nombre est supérieur ou égal à 65536, il est interprété comme un timestamp Unix (et non comme un nombre de jours), puis ramené à la date.
Cela permet de prendre en charge le cas courant où l&#39;on écrit `toDate(unix_timestamp)`, ce qui, sinon, provoquerait une erreur et obligerait à utiliser la forme plus lourde `toDate(toDateTime(unix_timestamp))`.

La conversion entre une date et une date avec heure s&#39;effectue naturellement : en ajoutant une heure nulle ou en supprimant l&#39;heure.

La conversion entre types numériques suit les mêmes règles que les affectations entre différents types numériques en C++.

**Exemple**

```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

Voir aussi la fonction [`toUnixTimestamp`](/fr/sql-reference/functions/date-time-functions#toUnixTimestamp).

{/* 
  Le contenu entre les balises ci-dessous est remplacé, lors du build du framework de documentation, par
  la documentation générée à partir de system.functions. Veuillez ne pas modifier ni supprimer ces balises.
  Voir : https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }